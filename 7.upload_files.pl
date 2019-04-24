#!/usr/local/bin/linear_perl

use strict;
use warnings;

use AppropriateLibrary 'linear';
use RTK::Linear::MooseX;
use namespace::autoclean;
use RTK::Util::ReasonableParams;

use JSON;
use File::Copy;
use Try::Tiny;

use Cwd qw(abs_path);
use File::Basename qw/ basename dirname /;
use RTK::Util::Misc qw/ timeout_after /;

use Aliases qw/
    RTK::Linear::DataSource
    RTK::Linear::Exporter::Table::DataExport
    RTK::Linear::Exporter::Table::DataExportClient
    RTK::Util::Set
/;

sub get_dir_path()
{
    my $path = abs_path($0);
    my $dir_cutoff = index($path, $0);
    return substr $path, 0, $dir_cutoff;
}

sub get_control_info()
{
    my $control_file_path = get_dir_path() . '1.control_file.txt';
    my $json;
    {
        local $/;
        open my $fh, "<", $control_file_path;
        $json = <$fh>;
        close $fh;
    }
    my $data = decode_json($json);
	return $data;
}

sub control_file_has_valid($field_name)
{
    my $control_info = get_control_info();
    return (exists $control_info->{$field_name} && scalar @{ $control_info->{$field_name} } > 0);
}

sub get_output_path()
{
    my $base_path = dirname(abs_path($0));
    my $output_path = "$base_path/output_directory/";
    return $output_path;
}

sub get_fh_for_script($filename)
{
    open my $fh, '>>', $filename or die "Unable to open file: $!\n";
    return $fh;
}

sub get_names($output_path)
{
    my @names;
    opendir (my $output_dh, $output_path);
    while (my $name = readdir($output_dh)) {
        if ($name ne "." && $name ne "..") {
            push @names, $name;
        }
    }
    close $output_dh;
    return @names;
}

sub get_names_and_files($output_path)
{
    my @names = get_names($output_path);
    my %names_and_filepaths;
    foreach (@names) {
        my @file_paths = glob "$output_path$_/*-reissue_*";

        $names_and_filepaths{$_} = [@file_paths]
    }
	return %names_and_filepaths;
}

sub get_data_export_clients_from_name($name)
{
    my $data_export = DataExport->find_the_one_by({name => $name});
    my $data_export_no = $data_export->data_export_no();
    my @data_export_clients = DataExportClient->find_by({data_export_no => $data_export_no});
}

sub map_file_data_source_nos(%names_and_files)
{
    my %file_ds_map;
    foreach my $name (keys(%names_and_files)) {
        my @files = @{ $names_and_files{$name} };
        my $data_export = DataExport->find_the_one_by({name => $name});
        my $exporter_class = $data_export->exporter_class();
        my @data_export_clients = get_data_export_clients_from_name($name);
        if (index($exporter_class, 'DemoMarket15Minute') == -1) {
            my @data_source_nos = map { $_->data_source_no() } @data_export_clients;
            foreach my $file (@files) {
                $file_ds_map{$file} = \@data_source_nos;
            }
        } else {
            foreach my $data_export_client (@data_export_clients) {
                my $parameters = $data_export_client->decoded_parameters_from_client_and_export();
                my $data_source_no = $data_export_client->data_source_no();
                my @market_nos = @{ $parameters->{market_nos} };
                my @market_file_signatures = map {"Rentrak-$_-Demo-QH"} @market_nos;
                my @filtered_files;
                foreach my $file (@files) {
                    if (grep {index($file, $_) != -1} @market_file_signatures) {
                        if ($file_ds_map{$file}) {
                            my $data_sources = $file_ds_map{$file};
                            push @{$data_sources}, $data_source_no;
                        } else {
                            my @data_sources = ($data_source_no);
                            $file_ds_map{$file} = \@data_sources;
                        }
                    }
                }
            }
        }
    }
    return %file_ds_map;
}
sub upload_file($data_source_no, $full_file_path)
{
    my $log_fh      = get_fh_for_script('transporter_log.txt');
    my $file_name   = basename($full_file_path);
    my $data_source = DataSource->find_the_one_by({data_source_no => $data_source_no}) || die "Unable to find the data source for data source no: $data_source_no";
    my $transporter = $data_source->transporter();
    my $out_dir     = $transporter->out_directory();

    # This is a hack for S3 transporters, which don't have OUT/Reissue folders but the others do.
    # Rule: if there is folder OUT in the out_directory, assume they get loaded to .../OUT/Reissue
    #       Otherwise, assume it is S3 upload to the original out_directory.

    my $has_out_dir_in_path = $out_dir =~ m/OUT/;
    my ($reissue_dir, undef) = split('OUT', $out_dir);
    $reissue_dir .= 'OUT/Reissue' if $has_out_dir_in_path;

    my (undef, $small_path) = split('output_directory', $full_file_path);

    print $log_fh "$small_path ==>> $reissue_dir\n";
    print         "$small_path ==>> $reissue_dir\n";

    timeout_after(
        2 * 60 * 60,
        sub {
            $transporter->transmit_to_dir(
                $full_file_path,
                $reissue_dir,
                $file_name,
            );
        }
    );
}

sub set_environment_variables()
{
    $ENV{"LINEAR_DB_SCHEMA"}="linear_national";
    if (uses_private_db()) {
        my $xmlpath = get_dir_path() . "ReissueDatabase.xml";
        $ENV{"DATABASE_XML"}=$xmlpath;
    }
}

sub uses_private_db()
{
    my $xmlpath = get_dir_path() . "ReissueDatabase.xml";
    return (-f $xmlpath);
}

sub main()
{
    set_environment_variables();
    my $output_path = get_output_path();
    my %names_and_files = get_names_and_files($output_path);
    my %file_ds_map = map_file_data_source_nos(%names_and_files);
    foreach my $filename (sort keys(%file_ds_map)) {
        my @data_source_nos = @{ $file_ds_map{$filename} };
        foreach my $data_source_no (@data_source_nos){
            upload_file($data_source_no, $filename);
        }
        my $move_destination = $filename;
        $move_destination =~ s/output_directory/uploaded_files/g;
        move($filename, $move_destination);
    }
    print "Done\n";
}

main();
	
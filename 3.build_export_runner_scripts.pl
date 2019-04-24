#!/usr/local/bin/linear_perl

use AppropriateLibrary 'linear';

use strict;
use warnings;

binmode STDOUT, "utf8";
use utf8;

use JSON;
use RTK::Util::DateTime;
use RTK::Util::ReasonableParams;

use Cwd qw(abs_path);
use RTK::Linear::Database qw/ get_dbh /;
use RTK::Util::DBI qw/ fetch_result_row /;

use Aliases qw/
    RTK::Util::DateTime
/;

sub get_dir_path()
{
    my $path = abs_path($0);
    my $dir_cutoff = index($path, $0);
    return substr $path, 0, $dir_cutoff;
}

sub uses_private_db()
{
    my $xmlpath = get_dir_path() . "ReissueDatabase.xml";
    return (-f $xmlpath)
}

sub setup_env_vars()
{
    $ENV{"LINEAR_DB_SCHEMA"}="linear_national";
    if (uses_private_db()) {
        my $xmlpath = get_dir_path() . "ReissueDatabase.xml";
        $ENV{"DATABASE_XML"}=$xmlpath;
    }
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
}

sub control_file_has_valid($field_name) {
    my $control_info = get_control_info();
    return (exists $control_info->{$field_name} && scalar @{ $control_info->{$field_name} } > 0);
}

sub get_exporter_parameters($name)
{
    return fetch_result_row(get_dbh(), "select parameters from data_exports where name='$name'")->{'parameters'};
}

sub get_exporter_class_for_export($name)
{
    return fetch_result_row(get_dbh(), "select exporter_class from data_exports where name='$name'")->{'exporter_class'};
}

sub generate_export_scripts()
{
    my $control_info = get_control_info();
    my $export_names = $control_info->{'export_names'};
    my $from_date = DateTime->new($control_info->{'start_date'});
    my $to_date = DateTime->new($control_info->{'end_date'});
    my $ticket = $control_info->{'ticket'};
    my $user = $control_info->{'user'};

    foreach my $export_name (@{$export_names}) {
        my $exporter_class = get_exporter_class_for_export($export_name);
        my $base_dir = get_dir_path();
        my $out_dir = $base_dir . 'output_directory';
        my $log_dir = $base_dir . 'logs';

        # Beanstalk exports store the Exporter in data_exports.exporter_class, not the ExportRunner.
        # Update the Exporter to the ExportRunner if needed.
        $exporter_class .= "Runner" if $exporter_class =~ "MarketRatingComparisonNoDemosExporter";
        $exporter_class = "RTK::Linear::Exporter::DemoMarket15MinuteExportRunner" if $exporter_class =~ "DemoMarket15MinuteExporter";

        my $market_nos = (control_file_has_valid('market_nos'))
            ? join(',', @{ $control_info->{'market_nos'} })
            : undef;

        my $tag_nos = (control_file_has_valid('tag_nos'))
            ? join(',', @{ $control_info->{'tag_nos'} })
            : undef;
        
        my $network_nos = (control_file_has_valid('network_nos'))
            ? join(',', @{ $control_info->{'network_nos'} })
			: undef;


        my $market_field_name = (
            $exporter_class =~ "DemoMarket15MinuteExportRunner" ||
            $exporter_class =~ "OptimizedDemoMarket15MinuteExportRunner" ||
            $exporter_class =~ "DVRMarket15MinuteExportRunner"
        ) ? "--market-nos" : "--tv-market-nos";

        my $private_db_clause = uses_private_db()
            ? 'export DATABASE_XML=' . $base_dir . 'ReissueDatabase.xml'
            : 'export USE_REPLICA_ROLE=reporting_replica';

        my $day_parameter = '--day';
        $day_parameter = '--first-day' if $exporter_class =~ 'WeeklyNetwork30SecondExportRunner';
        $day_parameter = '--date'      if $exporter_class =~ 'WeeklyNetwork5SecondExportRunner';


        my $curr_date = $from_date;
        my $date_indexer = 1;
        while ($curr_date->is_on_or_before($to_date)) {
            my $curr_date_trunc = $curr_date->as_str("YYYYMMDD");
            my $filename = "$export_name" . "_export_job_$curr_date_trunc.sh";
            my $fh = get_fh_for_script("./IN/$filename");

            print $fh <<HERE;
#!/bin/bash

out_dir=$out_dir
log_dir=$log_dir
curr_day=$curr_date_trunc

set -o pipefail

export LINEAR_DB_SCHEMA=linear_national
$private_db_clause

# if you are inside a dev tree, all of the default paths are overridden to use that dev tree
# for details see RTK::Linear::Properties::Paths
# the below variables force usage of the installed code

export LINEAR_BINDIR=/usr/local/linear/bin # modify if testing summarizer changes
export LINEAR_ETCDIR=/usr/local/linear/etc # modify if using custom config files
export LINEAR_LIBDIR=/usr/local/linear/lib # modify if testing reposql changes
export LINEAR_SRCDIR=/data_storage/apps/linear/current/cpp_src/linear # modify if testing reposql changes

export SERVER_SUBSYSTEM_PATH=/data_storage/apps/linear/current # modify if DB loading new or modified tables

export LINEAR_CLUSTER_ID=dynamic  # just in case the export needs to summarize data, such as DVRMarket15Minute.

echo "Beginning date: \$curr_day at `date`"

log=\$log_dir/$export_name
touch \$log
linear_run -Dtf $exporter_class \\
 $day_parameter=\$curr_day \\
    --output-directory=\$out_dir/$export_name \\
    --name=$export_name \\
HERE

            print $fh "    $market_field_name=$market_nos \\\n" if $market_nos;
            print $fh "    --network-nos==$network_nos \\\n" if $network_nos;
            print $fh "    --tag-nos=$tag_nos \\\n" if $tag_nos;
            print $fh "    --generate-files-only >> \$log 2>&1 \n";
            print $fh "exit \$?";
            close $fh;

            $curr_date = $curr_date->plus_days(1);
            $date_indexer = $date_indexer + 1;
        }
    }
}

sub create_output_directory_builder()
{
    my $fh = get_fh_for_script('4.make_directories.sh');
    my $control_info = get_control_info();
    my $user = $control_info->{'user'};
    my $ticket = $control_info->{'ticket'};
    my @export_names = @{ $control_info->{'export_names'} };

    print $fh "#!/bin/bash\n\n";
    print $fh "mkdir logs\n";
    print $fh "mkdir -p /mnt/nfs/gizmoxports/$user\n";
    print $fh "mkdir /mnt/nfs/gizmoxports/$user/$ticket\n";

    print $fh "mkdir /mnt/nfs/gizmoxports/$user/$ticket/output_directory\n";
    print $fh "ln -s /mnt/nfs/gizmoxports/$user/$ticket/output_directory output_directory\n";
    print $fh "mkdir -p output_directory/$_\n" for @export_names;

    print $fh "mkdir /mnt/nfs/gizmoxports/$user/$ticket/uploaded_files\n";
    print $fh "ln -s /mnt/nfs/gizmoxports/$user/$ticket/uploaded_files uploaded_files\n";
    print $fh "mkdir -p uploaded_files/$_\n" for @export_names;

    close $fh;
}

sub main()
{
    setup_env_vars();
    generate_export_scripts();
    create_output_directory_builder();
}

main();

			
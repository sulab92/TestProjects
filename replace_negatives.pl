#!/usr/local/bin/linear_perl

use strict;
use warnings;

use AppropriateLibrary 'linear';  #this allows us to access all the utilities we have built in our rentrak library of code.  It is very powerful.
use RTK::Util::ReasonableParams;  #this allows us to pass parameters in a java-like manner, so we don't have to pull values out of @_
use RTK::Util::Misc qw/ is_one_of unique /; # I wanted to use these helper functions, which is why I imported AppropriateLibrary.

#Files you want to remove duplicates from must be in a directory called "files" in the same directory as this script
#This script will take all the .txt files in files/ and spit out new files with the same name in the same directory with duplicates removed

sub run
{
    my @filenames = get_filenames();
    foreach my $filename (@filenames) {
        next if $filename =~ ".zip";
        remove_duplicates_from_file($filename);
    }
    my $dupe_count = 0;
    print "\nThe following files contained duplicates";
    foreach my $filename (@filenames) {
        next if $filename =~ ".zip";
        $dupe_count += print_duplicates($filename);
        (my $zip_filename = $filename) =~ s/\.txt/\.zip/;
        `zip -j $zip_filename $filename`;
    }
    print "\nNo files had duplicates! Yay!" if $dupe_count == 0;
    print "\n";
}

sub remove_duplicates_from_file
{
    my ($output_file) = @_;
    my $input_file = "./files/$output_file";
    my %found_records;
    print "Removing duplicates from: $input_file for $output_file\n";
    open my $output_fh, '>', $output_file or die "Unable to open output file: $output_file\n";
    open my $input_fh, '<', $input_file or die "Unable to open input file: $input_file\n";
    while (<$input_fh>) {
        chomp;
        my $row = $_;
        if ($row =~ '^2\|') {
            my @cols = split('\|', $_);
            my $key = join('|', map { $cols[$_] } 0..6);
            next if $found_records{$key}++;
        }
        if ($row =~ '^6\|') {
            my $new_count = `wc -l < $output_file`;
            $new_count += 1;
            my ($old_count) = $row =~ m/\|(\d+)/;
            if ($new_count != $old_count) {
                my $new_row = $row =~ s/$old_count/$new_count/gr;
                $row = $new_row;
		    }
        }
        print $output_fh "$row\n";
    }
    close $output_fh;
    close $input_fh;
}

sub get_filenames {
    my $output_path = "./files";
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

sub print_duplicates {
    my ($file_1) = @_;
    my $file_2 = "./files/$file_1";

    my $result = `diff -q $file_1 $file_2`;
    print "\n$file_1 had duplicate records" if $result;
    return 1 if $result;
    return 0;
}

run();
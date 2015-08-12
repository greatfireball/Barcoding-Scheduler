#!/usr/bin/env perl

use strict;
use warnings;

use Net::SCP;

use Getopt::Long;

use File::Basename;

GetOptions(
    'hostname=s' => \(my $hostname),
    'controlfile=s' => \(my $controlfile)
) || die "Error parsing of input parameters\n";

unless (defined $hostname && defined $controlfile)
{
    die "Unable to get the required parameter hostname and conrolfile\n";
}

$hostname = "server";
my $scp = Net::SCP->new( "$hostname" );

# get the current cluster.info
$scp->get($controlfile) || die $scp->{errstr};

my ($file, undef, undef) = fileparse($controlfile);

open(FH, "<", $file) || die "Unable to load file '$file'";
my %dataset = ();
while (<FH>)
{
    chomp($_);

    my ($raw, $fasta, $tsv, $in, $time, $email) = split(/\|/, $_);

    $dataset{$_} = { rawout => $raw,
		     fastaout => $fasta,
		     tsvout => $tsv,
		     input => $in,
		     time => $time,
		     email => $email };
}
close(FH) || die "Unable to close file '$file'";

my $file_done = $file.".done";
# check if file exists, otherwise we can create one
unless (-e $file_done)
{
    open(FH, ">", $file_done) || die "Unable to create file '$file_done'\n";
    close(FH) || die "Unable to close file '$file_done' after creation\n";
}

open(FH, "<", $file_done) || die "Unable to open file '$file_done'";
while (<FH>)
{
    chomp($_);

    my ($raw, $fasta, $tsv, $in, $time, $email) = split(/\|/, $_);

    if (exists $dataset{$_})
    {
	warn "Dataset '$_' is already done... Deleting!\n";
	delete $dataset{$_};
    }
}
close(FH) || die "Unable to close file '$file_done'";

printf STDERR "Found %d jobs\n", (keys %dataset)+0;

foreach (keys %dataset)
{
    warn "Working on dataset '$_'\n";
    ### run run_utax script
    ### upload the files to the server
    ### send email
    ### create finished dataset
    ### push cluster.done to server
}

### done

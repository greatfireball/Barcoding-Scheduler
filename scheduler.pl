#!/usr/bin/env perl

use strict;
use warnings;

use Net::SCP;

use Getopt::Long;

use File::Basename;
use File::Temp;

GetOptions(
    'hostname=s'    => \(my $hostname),
    'controlfile=s' => \(my $controlfile),
    'tax=s'         => \(my $tax),
    'db=s'          => \(my $db),
    'utax=s'        => \(my $utax),
) || die "Error parsing of input parameters\n";

unless (defined $hostname && defined $controlfile && defined $db && defined $tax && defined $utax)
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

    if (exists $dataset{$_})
    {
	warn "Dataset '$_' is already done... Deleting!\n";
	delete $dataset{$_};
    }
}
close(FH) || die "Unable to close file '$file_done'";

printf STDERR "Found %d jobs\n", (keys %dataset)+0;

# create three output filenames

my (undef, $fasta) = File::Temp::tempfile(OPEN => 0);
my (undef, $tsv) = File::Temp::tempfile(OPEN => 0);
my (undef, $raw) = File::Temp::tempfile(OPEN => 0);
my (undef, $in) = File::Temp::tempfile(OPEN => 0);

foreach (keys %dataset)
{
    warn "Working on dataset '$_'\n";
    ### run run_utax script
    # Download the input file
    $scp->get($dataset{$_}{input}, $in) || die $scp->{errstr};

    # run our script
    my @cmd = (
	'run_utax.pl',
	'--database', $db,
	'--taxonomy', $tax,
	'--in',       $in,
	'--outfile',  $raw,
	'--tsv',      $tsv,
	'--fasta',    $fasta,
	'--utax',     $utax,
	'--force'
	);

    system(@cmd);

    my $failed = 0;

    if ($? != 0)
    {
	warn "Call of run_utax.pl failed!\n";
	$failed = 1;
    }

    ### upload the files to the server
    # Upload the output files if the run did not fail
    unless ($failed)
    {
	$scp->put($fasta, $dataset{$_}{fastaout}, $in) || die $scp->{errstr};
	$scp->put($tsv, $dataset{$_}{tsvout}, $in) || die $scp->{errstr};
	$scp->put($raw, $dataset{$_}{rawout}, $in) || die $scp->{errstr};
    }

    ### send email
    ### create finished dataset
    ### push cluster.done to server
}

### done

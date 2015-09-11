#!/usr/bin/env perl

use strict;
use warnings;
use 5.010_000;

use FindBin qw($RealBin);
use lib "$RealBin/lib";
use File::Spec();
use File::Temp 0.2304 ();
use File::Find();
use File::Basename qw(basename);
use Archive::Extract();
use Digest::SHA();
$Archive::Extract::PREFER_BIN = 1;

my $mode = shift(@ARGV) || "";
die usage() unless $mode =~ /^--(check|update)$/;

my $License_Dir = shift(@ARGV) || die usage();
my $Source      = shift(@ARGV) || die usage();
my $Ignore      = shift(@ARGV) || '';
my $ignore
    = $Ignore
    ? qr/${Ignore}[^\/]*$/
    : qr/elasticsearch[^\/]*$/;

$License_Dir = File::Spec->rel2abs($License_Dir) . '/';
$Source      = File::Spec->rel2abs($Source);

say "LICENSE DIR: $License_Dir";
say "SOURCE: $Source";
say "IGNORE: $Ignore";

die "License dir is not a directory: $License_Dir\n" . usage()
    unless -d $License_Dir;

my %shas
    = -f $Source ? jars_from_zip( $Source, $ignore )
    : -d $Source ? jars_from_dir( $Source, $ignore )
    :   die "Source is neither a directory nor a zip file: $Source" . usage();

$mode eq '--check'
    ? exit check_shas_and_licenses(%shas)
    : exit write_shas(%shas);

#===================================
sub check_shas_and_licenses {
#===================================
    my %new = @_;

    my %old      = get_sha_files();
    my %licenses = get_files_with('LICENSE');
    my %notices  = get_files_with('NOTICE');

    my $error     = 0;
    my $sha_error = 0;

    for my $jar ( sort keys %new ) {
        my $old_sha = delete $old{$jar};
        unless ($old_sha) {
            say STDERR "$jar: SHA is missing";
            $error++;
            $sha_error++;
            next;
        }

        unless ( $old_sha eq $new{$jar} ) {
            say STDERR
                "$jar: SHA has changed, expected $old_sha but found $new{$jar}";
            $error++;
            $sha_error++;
            next;
        }

        my $license_found;
        my $notice_found;
        my $prefix = $jar;
        $prefix =~ s/\.sha1//;

        while ( $prefix =~ s/-[^\-]+$// ) {
            if ( exists $licenses{$prefix} ) {
                $license_found = 1;

                # mark all licenses with the same prefix as used
                for ( keys %licenses ) {
                    $licenses{$_}++ if index( $prefix, $_ ) == 0;
                }

                if ( exists $notices{$prefix} ) {
                    $notices{$prefix}++;
                    $notice_found = 1;
                }
                last;
            }
        }
        unless ($license_found) {
            say STDERR "$jar: LICENSE is missing";
            $error++;
            $sha_error++;
        }
        unless ($notice_found) {
            say STDERR "$jar: NOTICE is missing";
            $error++;
        }
    }

    if ( keys %old ) {
        say STDERR "Extra SHA files present for: " . join ", ", sort keys %old;
        $error++;
    }

    my @unused_licenses = grep { !$licenses{$_} } keys %licenses;
    if (@unused_licenses) {
        $error++;
        say STDERR "Extra LICENCE file present: " . join ", ",
            sort @unused_licenses;
    }

    my @unused_notices = grep { !$notices{$_} } keys %notices;
    if (@unused_notices) {
        $error++;
        say STDERR "Extra NOTICE file present: " . join ", ",
            sort @unused_notices;
    }

    if ($sha_error) {
        say STDERR <<"SHAS"

You can update the SHA files by running:

$0 --update $License_Dir $Source $Ignore

SHAS
    }
    say("All SHAs and licenses OK") unless $error;
    return $error;
}

#===================================
sub write_shas {
#===================================
    my %new = @_;
    my %old = get_sha_files();

    for my $jar ( sort keys %new ) {
        if ( $old{$jar} ) {
            next if $old{$jar} eq $new{$jar};
            say "Updating $jar";
        }
        else {
            say "Adding $jar";
        }
        open my $fh, '>', $License_Dir . $jar or die $!;
        say $fh $new{$jar} or die $!;
        close $fh or die $!;
    }
    continue {
        delete $old{$jar};
    }

    for my $jar ( sort keys %old ) {
        say "Deleting $jar";
        unlink $License_Dir . $jar or die $!;
    }
    say "SHAs updated";
    return 0;
}

#===================================
sub get_files_with {
#===================================
    my $pattern = shift;
    my %files;
    for my $path ( grep {-f} glob("$License_Dir/*$pattern*") ) {
        my ($file) = ( $path =~ m{([^/]+)-${pattern}.*$} );
        $files{$file} = 0;
    }
    return %files;
}

#===================================
sub get_sha_files {
#===================================
    my %shas;

    die "Missing directory: $License_Dir\n"
        unless -d $License_Dir;

    for my $file ( grep {-f} glob("$License_Dir/*.sha1") ) {
        my ($jar) = ( $file =~ m{([^/]+)$} );
        open my $fh, '<', $file or die $!;
        my $sha = <$fh>;
        $sha ||= '';
        chomp $sha;
        $shas{$jar} = $sha;
    }
    return %shas;
}

#===================================
sub jars_from_zip {
#===================================
    my ( $source, $ignore ) = @_;
    my $temp_dir = File::Temp->newdir;
    my $dir_name = $temp_dir->dirname;
    my $archive  = Archive::Extract->new( archive => $source, type => 'zip' );
    $archive->extract( to => $dir_name ) || die $archive->error;
    my @jars = map { File::Spec->rel2abs( $_, $dir_name ) }
        grep { /\.jar$/ && !/$ignore/ } @{ $archive->files };
    return calculate_shas(@jars);
}

#===================================
sub jars_from_dir {
#===================================
    my ( $source, $ignore ) = @_;
    my @jars;
    File::Find::find(
        {   wanted => sub {
                push @jars, File::Spec->rel2abs( $_, $source )
                    if /\.jar$/ && !/$ignore/;
            },
            no_chdir => 1
        },
        $source
    );
    return calculate_shas(@jars);
}

#===================================
sub calculate_shas {
#===================================
    my %shas;
    while ( my $file = shift() ) {
        my $digest = eval { Digest::SHA->new(1)->addfile($file) }
            or die "Error calculating SHA1 for <$file>: $!\n";
        $shas{ basename($file) . ".sha1" } = $digest->hexdigest;
    }
    return %shas;
}

#===================================
sub usage {
#===================================
    return <<"USAGE";

USAGE:

    # check the sha1 and LICENSE files for each jar in the zip or directory
    $0 --check  path/to/licenses/ path/to/package.zip [prefix_to_ignore]
    $0 --check  path/to/licenses/ path/to/dir/ [prefix_to_ignore]

    # updates the sha1s for each jar in the zip or directory
    $0 --update path/to/licenses/ path/to/package.zip [prefix_to_ignore]
    $0 --update path/to/licenses/ path/to/dir/ [prefix_to_ignore]

The optional prefix_to_ignore parameter defaults to "elasticsearch".

USAGE

}


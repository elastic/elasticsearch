#!/usr/bin/env perl

use strict;
use warnings;

use FindBin qw($RealBin);
use lib "$RealBin/lib";
use File::Spec();
use File::Temp();
use File::Find();
use File::Basename qw(basename);
use Archive::Extract();
$Archive::Extract::PREFER_BIN = 1;

our $SHA_CLASS = 'Digest::SHA';
if ( eval { require Digest::SHA } ) {
    $SHA_CLASS = 'Digest::SHA';
}
else {

    print STDERR "Digest::SHA not available. "
        . "Falling back to Digest::SHA::PurePerl\n";
    require Digest::SHA::PurePerl;
    $SHA_CLASS = 'Digest::SHA::PurePerl';
}

my $mode = shift(@ARGV) || "";
die usage() unless $mode =~ /^--(check|update)$/;

my $License_Dir = shift(@ARGV) || die usage();
my $Source      = shift(@ARGV) || die usage();
$License_Dir = File::Spec->rel2abs($License_Dir) . '/';
$Source      = File::Spec->rel2abs($Source);

print "LICENSE DIR: $License_Dir\n";
print "SOURCE: $Source\n";

die "License dir is not a directory: $License_Dir\n" . usage()
    unless -d $License_Dir;

my %shas
    = -f $Source ? jars_from_zip($Source)
    : -d $Source ? jars_from_dir($Source)
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
            print STDERR "$jar: SHA is missing\n";
            $error++;
            $sha_error++;
            next;
        }

        unless ( $old_sha eq $new{$jar} ) {
            print STDERR
                "$jar: SHA has changed, expected $old_sha but found $new{$jar}\n";
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
            print STDERR "$jar: LICENSE is missing\n";
            $error++;
            $sha_error++;
        }
        unless ($notice_found) {
            print STDERR "$jar: NOTICE is missing\n";
            $error++;
        }
    }

    if ( keys %old ) {
        print STDERR "Extra SHA files present for: " . join ", ",
            sort keys %old;
        print "\n";
        $error++;
    }

    my @unused_licenses = grep { !$licenses{$_} } keys %licenses;
    if (@unused_licenses) {
        $error++;
        print STDERR "Extra LICENCE file present: " . join ", ",
            sort @unused_licenses;
        print "\n";
    }

    my @unused_notices = grep { !$notices{$_} } keys %notices;
    if (@unused_notices) {
        $error++;
        print STDERR "Extra NOTICE file present: " . join ", ",
            sort @unused_notices;
        print "\n";
    }

    if ($sha_error) {
        print STDERR <<"SHAS"

You can update the SHA files by running:

$0 --update $License_Dir $Source

SHAS
    }
    print("All SHAs and licenses OK\n") unless $error;
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
            print "Updating $jar\n";
        }
        else {
            print "Adding $jar\n";
        }
        open my $fh, '>', $License_Dir . $jar or die $!;
        print $fh $new{$jar} . "\n" or die $!;
        close $fh or die $!;
    }
    continue {
        delete $old{$jar};
    }

    for my $jar ( sort keys %old ) {
        print "Deleting $jar\n";
        unlink $License_Dir . $jar or die $!;
    }
    print "SHAs updated\n";
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
    my ($source) = @_;
    my $temp_dir = File::Temp->newdir;
    my $dir_name = $temp_dir->dirname;
    my $archive = Archive::Extract->new( archive => $source, type => 'zip' );
    $archive->extract( to => $dir_name ) || die $archive->error;
    my @jars = map { File::Spec->rel2abs( $_, $dir_name ) }
        grep { /\.jar$/ && !/elasticsearch[^\/]*$/ } @{ $archive->files };
    return calculate_shas(@jars);
}

#===================================
sub jars_from_dir {
#===================================
    my $source = shift;
    my @jars;
    File::Find::find(
        {   wanted => sub {
                push @jars, File::Spec->rel2abs( $_, $source )
                    if /\.jar$/ && !/elasticsearch[^\/]*$/;
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
        my $digest = eval { $SHA_CLASS->new(1)->addfile($file) }
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
    $0 --check  path/to/licenses/ path/to/package.zip
    $0 --check  path/to/licenses/ path/to/dir/

    # updates the sha1s for each jar in the zip or directory
    $0 --update path/to/licenses/ path/to/package.zip
    $0 --update path/to/licenses/ path/to/dir/

USAGE

}


#!/usr/bin/env perl

use strict;
use warnings;
use v5.10;

use FindBin qw($RealBin);
use lib "$RealBin/lib";
use Archive::Ar();
use Cwd();
use File::Spec();
use Digest::SHA qw(sha1);
use File::Temp();
use File::Basename qw(basename);
use Archive::Extract();
$Archive::Extract::PREFER_BIN = 1;

our %Extract_Package = (
    zip => \&extract_zip,
    gz  => \&extract_tar_gz,
    rpm => \&extract_rpm,
    deb => \&extract_deb
);

my $mode = shift(@ARGV) || "";
die usage() unless $mode =~ /^--(check|update)$/;

my $License_Dir = shift(@ARGV) || die usage();
my $Package     = shift(@ARGV) || die usage();
$License_Dir = File::Spec->rel2abs($License_Dir) . '/';
$Package     = File::Spec->rel2abs($Package);

die "License dir is not a directory: $License_Dir\n" . usage()
    unless -d $License_Dir;

die "Package is not a file: $Package\n" . usage()
    unless -f $Package;

my %shas = get_shas_from_package($Package);
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
        say STDERR "Extra LICENCE file present: " . join ", ",
            sort @unused_licenses;
    }

    my @unused_notices = grep { !$notices{$_} } keys %notices;
    if (@unused_notices) {
        say STDERR "Extra NOTICE file present: " . join ", ",
            sort @unused_notices;
    }

    if ($sha_error) {
        say STDERR <<"SHAS"

You can update the SHA files by running:

$0 --update $License_Dir $Package

SHAS
    }
    say "All SHAs and licenses OK" unless $error;
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
sub get_shas_from_package {
#===================================
    my $package = shift;
    my ($type) = ( $package =~ /\.(\w+)$/ );
    die "Unrecognised package type: $package"
        unless $type && $Extract_Package{$type};

    my $temp_dir = File::Temp->newdir;
    my $files
        = eval { $Extract_Package{$type}->( $package, $temp_dir->dirname ) }
        or die "Couldn't extract $package: $@";

    my @jars = map {"$temp_dir/$_"}
        grep { /\.jar$/ && !/elasticsearch[^\/]*$/ } @$files;
    return calculate_shas(@jars);
}

#===================================
sub extract_zip {
#===================================
    my ( $package, $dir ) = @_;
    my $archive = Archive::Extract->new( archive => $package, type => 'zip' );
    $archive->extract( to => $dir ) || die $archive->error;
    return $archive->files;
}

#===================================
sub extract_tar_gz {
#===================================
    my ( $package, $dir ) = @_;
    my $archive = Archive::Extract->new( archive => $package, type => 'tgz' );
    $archive->extract( to => $dir ) || die $archive->error;
    return $archive->files;
}

#===================================
sub extract_rpm {
#===================================
    my ( $package, $dir ) = @_;
    my $cwd = Cwd::cwd();
    my @files;
    eval {
        chdir $dir;
        say "Trying with rpm2cpio";
        my $out = eval {`rpm2cpio '$package'  | cpio -idmv --quiet`};
        unless ($out) {
            say "Trying with rpm2cpio.pl";
            $out = eval {`rpm2cpio.pl '$package'  | cpio -idmv --quiet`};
        }
        @files = split "\n", $out if $out;
    };
    chdir $cwd;
    die $@ if $@;
    die "Couldn't extract $package\n" unless @files;
    return \@files;
}

#===================================
sub extract_deb {
#===================================
    my ( $package, $dir ) = @_;
    my $archive = Archive::Ar->new;
    $archive->read($package) || die $archive->error;
    my $cwd = Cwd::cwd();
    eval {
        chdir $dir;
        $archive->extract('data.tar.gz') || die $archive->error;
    };
    chdir $cwd;
    die $@ if $@;
    $archive = Archive::Extract->new(
        archive => $dir . '/data.tar.gz',
        type    => 'tgz'
    );
    $archive->extract( to => $dir ) || die $archive->error;
    return $archive->files;
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

    # check the sha1 and LICENSE files for each jar in the zip|gz|deb|rpm
    $0 --check  path/to/licenses/ path/to/package.zip

    # updates the sha1s for each jar in the zip|gz|deb|rpm
    $0 --update path/to/licenses/ path/to/package.zip

USAGE

}


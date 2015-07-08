#!/usr/bin/env perl

use strict;
use warnings;
use v5.10;
use Digest::SHA qw(sha1);
use File::Temp();
use File::Basename qw(basename);
use File::Find();
my $mode = shift(@ARGV) || die usage();
my $dir  = shift(@ARGV) || die usage();
$dir =~ s{/$}{};

our $RELEASES_DIR = "$dir/target/releases/";
our $LICENSE_DIR  = "$dir/licenses/";

$mode eq '--check'        ? check_shas_and_licenses($dir)
    : $mode eq '--update' ? write_shas($dir)
    :                       die usage();

#===================================
sub check_shas_and_licenses {
#===================================
    my %new = get_shas_from_zip();
    check_tar_has_same_shas(%new);

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
            say STDERR "$jar: SHA has changed, expected $old_sha but found $new{$jar}";
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

    $0 --update core

SHAS
    }

    exit $error;
}

#===================================
sub write_shas {
#===================================
    my %new = get_shas_from_zip();
    my %old = get_sha_files();

    for my $jar ( sort keys %new ) {
        if ( $old{$jar} ) {
            next if $old{$jar} eq $new{$jar};
            say "Updating $jar";
        }
        else {
            say "Adding $jar";
        }
        open my $fh, '>', $LICENSE_DIR . $jar or die $!;
        say $fh $new{$jar} or die $!;
        close $fh or die $!;
    }
    continue {
        delete $old{$jar};
    }

    for my $jar ( sort keys %old ) {
        say "Deleting $jar";
        unlink $LICENSE_DIR . $jar or die $!;
    }
}

#===================================
sub get_files_with {
#===================================
    my $pattern = shift;
    my %files;
    for my $path ( grep {-f} glob("$LICENSE_DIR/*$pattern*") ) {
        my ($file) = ( $path =~ m{([^/]+)-${pattern}.*$} );
        $files{$file} = 0;
    }
    return %files;
}

#===================================
sub get_sha_files {
#===================================
    my %shas;

    die "Missing directory: $LICENSE_DIR\n"
        unless -d $LICENSE_DIR;

    for my $file ( grep {-f} glob("$LICENSE_DIR/*.sha1") ) {
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
sub get_shas_from_zip {
#===================================
    my ($zip) = glob("$RELEASES_DIR/elasticsearch*.zip")
        or die "No .zip file found in $RELEASES_DIR\n";

    my $temp_dir = File::Temp->newdir;
    my $dir_name = $temp_dir->dirname;
    system( 'unzip', "-j", "-q", $zip, "*.jar", "-d" => $dir_name )
        && die "Error unzipping <$zip> to <" . $dir_name . ">: $!\n";

    my @jars = grep { !/\/elasticsearch[^\/]+.jar$/ } glob "$dir_name/*.jar";
    return calculate_shas(@jars);
}

#===================================
sub check_tar_has_same_shas {
#===================================
    my %zip_shas = @_;
    my ($tar) = glob("$RELEASES_DIR/elasticsearch*.tar.gz")
        or return;

    my $temp_dir = File::Temp->newdir;
    my $dir_name = $temp_dir->dirname;
    system( 'tar', "-xz", "-C" => $dir_name, "-f" => $tar )
        && die "Error unpacking <$tar> to <" . $dir_name . ">: $!\n";

    my @jars;
    File::Find::find(
        {   wanted =>
                sub { push @jars, $_ if /\.jar$/ && !/elasticsearch[^\/]*$/ },
            no_chdir => 1
        },
        $dir_name
    );

    my %tar_shas = calculate_shas(@jars);
    my @errors;
    for ( sort keys %zip_shas ) {
        my $sha = delete $tar_shas{$_};
        if ( !$sha ) {
            push @errors, "$_: JAR present in zip but not in tar.gz";
        }
        elsif ( $sha ne $zip_shas{$_} ) {
            push @errors, "$_: JAR in zip and tar.gz are different";
        }
    }
    for ( sort keys %tar_shas ) {
        push @errors, "$_: JAR present in tar.gz but not in zip";
    }
    if (@errors) {
        die join "\n", @errors;
    }
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

    $0 --check  dir   # check the sha1 and LICENSE files for each jar
    $0 --update dir   # update the sha1 files for each jar

The <dir> can be set to e.g. 'core' or 'plugins/analysis-icu/'

USAGE

}

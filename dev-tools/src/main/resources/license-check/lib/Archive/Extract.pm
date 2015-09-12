package Archive::Extract;
use if $] > 5.017, 'deprecate';

use strict;

use Cwd                         qw[cwd chdir];
use Carp                        qw[carp];
use IPC::Cmd                    qw[run can_run];
use FileHandle;
use File::Path                  qw[mkpath];
use File::Spec;
use File::Basename              qw[dirname basename];
use Params::Check               qw[check];
use Module::Load::Conditional   qw[can_load check_install];
use Locale::Maketext::Simple    Style => 'gettext';

### solaris has silly /bin/tar output ###
use constant ON_SOLARIS     => $^O eq 'solaris' ? 1 : 0;
use constant ON_NETBSD      => $^O eq 'netbsd' ? 1 : 0;
use constant ON_OPENBSD     => $^O =~ m!^(openbsd|bitrig)$! ? 1 : 0;
use constant ON_FREEBSD     => $^O =~ m!^(free|midnight|dragonfly)(bsd)?$! ? 1 : 0;
use constant ON_LINUX       => $^O eq 'linux' ? 1 : 0;
use constant FILE_EXISTS    => sub { -e $_[0] ? 1 : 0 };

### VMS may require quoting upper case command options
use constant ON_VMS         => $^O eq 'VMS' ? 1 : 0;

### Windows needs special treatment of Tar options
use constant ON_WIN32       => $^O eq 'MSWin32' ? 1 : 0;

### we can't use this extraction method, because of missing
### modules/binaries:
use constant METHOD_NA      => [];

### If these are changed, update @TYPES and the new() POD
use constant TGZ            => 'tgz';
use constant TAR            => 'tar';
use constant GZ             => 'gz';
use constant ZIP            => 'zip';
use constant BZ2            => 'bz2';
use constant TBZ            => 'tbz';
use constant Z              => 'Z';
use constant LZMA           => 'lzma';
use constant XZ             => 'xz';
use constant TXZ            => 'txz';

use vars qw[$VERSION $PREFER_BIN $PROGRAMS $WARN $DEBUG
            $_ALLOW_BIN $_ALLOW_PURE_PERL $_ALLOW_TAR_ITER
         ];

$VERSION            = '0.76';
$PREFER_BIN         = 0;
$WARN               = 1;
$DEBUG              = 0;
$_ALLOW_PURE_PERL   = 1;    # allow pure perl extractors
$_ALLOW_BIN         = 1;    # allow binary extractors
$_ALLOW_TAR_ITER    = 1;    # try to use Archive::Tar->iter if available

# same as all constants
my @Types           = ( TGZ, TAR, GZ, ZIP, BZ2, TBZ, Z, LZMA, XZ, TXZ );

local $Params::Check::VERBOSE = $Params::Check::VERBOSE = 1;

=pod

=head1 NAME

Archive::Extract - A generic archive extracting mechanism

=head1 SYNOPSIS

    use Archive::Extract;

    ### build an Archive::Extract object ###
    my $ae = Archive::Extract->new( archive => 'foo.tgz' );

    ### extract to cwd() ###
    my $ok = $ae->extract;

    ### extract to /tmp ###
    my $ok = $ae->extract( to => '/tmp' );

    ### what if something went wrong?
    my $ok = $ae->extract or die $ae->error;

    ### files from the archive ###
    my $files   = $ae->files;

    ### dir that was extracted to ###
    my $outdir  = $ae->extract_path;


    ### quick check methods ###
    $ae->is_tar     # is it a .tar file?
    $ae->is_tgz     # is it a .tar.gz or .tgz file?
    $ae->is_gz;     # is it a .gz file?
    $ae->is_zip;    # is it a .zip file?
    $ae->is_bz2;    # is it a .bz2 file?
    $ae->is_tbz;    # is it a .tar.bz2 or .tbz file?
    $ae->is_lzma;   # is it a .lzma file?
    $ae->is_xz;     # is it a .xz file?
    $ae->is_txz;    # is it a .tar.xz or .txz file?

    ### absolute path to the archive you provided ###
    $ae->archive;

    ### commandline tools, if found ###
    $ae->bin_tar     # path to /bin/tar, if found
    $ae->bin_gzip    # path to /bin/gzip, if found
    $ae->bin_unzip   # path to /bin/unzip, if found
    $ae->bin_bunzip2 # path to /bin/bunzip2 if found
    $ae->bin_unlzma  # path to /bin/unlzma if found
    $ae->bin_unxz    # path to /bin/unxz if found

=head1 DESCRIPTION

Archive::Extract is a generic archive extraction mechanism.

It allows you to extract any archive file of the type .tar, .tar.gz,
.gz, .Z, tar.bz2, .tbz, .bz2, .zip, .xz,, .txz, .tar.xz or .lzma
without having to worry how it
does so, or use different interfaces for each type by using either
perl modules, or commandline tools on your system.

See the C<HOW IT WORKS> section further down for details.

=cut


### see what /bin/programs are available ###
$PROGRAMS = {};
CMD: for my $pgm (qw[tar unzip gzip bunzip2 uncompress unlzma unxz]) {
    if ( $pgm eq 'unzip' and ON_FREEBSD and my $unzip = can_run('info-unzip') ) {
      $PROGRAMS->{$pgm} = $unzip;
      next CMD;
    }
    if ( $pgm eq 'unzip' and ( ON_NETBSD or ON_FREEBSD ) ) {
      local $IPC::Cmd::INSTANCES = 1;
      ($PROGRAMS->{$pgm}) = grep { ON_NETBSD ? m!/usr/pkg/! : m!/usr/local! } can_run($pgm);
      next CMD;
    }
    if ( $pgm eq 'unzip' and ON_LINUX ) {
      # Check if 'unzip' is busybox masquerading
      local $IPC::Cmd::INSTANCES = 1;
      my $opt = ON_VMS ? '"-Z"' : '-Z';
      ($PROGRAMS->{$pgm}) = grep { scalar run(command=> [ $_, $opt, '-1' ]) } can_run($pgm);
      next CMD;
    }
    if ( $pgm eq 'tar' and ( ON_OPENBSD || ON_SOLARIS || ON_NETBSD ) ) {
      # try gtar first
      next CMD if $PROGRAMS->{$pgm} = can_run('gtar');
    }
    $PROGRAMS->{$pgm} = can_run($pgm);
}

### mapping from types to extractor methods ###
my $Mapping = {  # binary program           # pure perl module
    is_tgz  => { bin => '_untar_bin',       pp => '_untar_at'   },
    is_tar  => { bin => '_untar_bin',       pp => '_untar_at'   },
    is_gz   => { bin => '_gunzip_bin',      pp => '_gunzip_cz'  },
    is_zip  => { bin => '_unzip_bin',       pp => '_unzip_az'   },
    is_tbz  => { bin => '_untar_bin',       pp => '_untar_at'   },
    is_bz2  => { bin => '_bunzip2_bin',     pp => '_bunzip2_bz2'},
    is_Z    => { bin => '_uncompress_bin',  pp => '_gunzip_cz'  },
    is_lzma => { bin => '_unlzma_bin',      pp => '_unlzma_cz'  },
    is_xz   => { bin => '_unxz_bin',        pp => '_unxz_cz'    },
    is_txz  => { bin => '_untar_bin',       pp => '_untar_at'   },
};

{   ### use subs so we re-generate array refs etc for the no-override flags
    ### if we don't, then we reuse the same arrayref, meaning objects store
    ### previous errors
    my $tmpl = {
        archive         => sub { { required => 1, allow => FILE_EXISTS }    },
        type            => sub { { default => '', allow => [ @Types ] }     },
        _error_msg      => sub { { no_override => 1, default => [] }        },
        _error_msg_long => sub { { no_override => 1, default => [] }        },
    };

    ### build accessors ###
    for my $method( keys %$tmpl,
                    qw[_extractor _gunzip_to files extract_path],
    ) {
        no strict 'refs';
        *$method = sub {
                        my $self = shift;
                        $self->{$method} = $_[0] if @_;
                        return $self->{$method};
                    }
    }

=head1 METHODS

=head2 $ae = Archive::Extract->new(archive => '/path/to/archive',[type => TYPE])

Creates a new C<Archive::Extract> object based on the archive file you
passed it. Automatically determines the type of archive based on the
extension, but you can override that by explicitly providing the
C<type> argument.

Valid values for C<type> are:

=over 4

=item tar

Standard tar files, as produced by, for example, C</bin/tar>.
Corresponds to a C<.tar> suffix.

=item tgz

Gzip compressed tar files, as produced by, for example C</bin/tar -z>.
Corresponds to a C<.tgz> or C<.tar.gz> suffix.

=item gz

Gzip compressed file, as produced by, for example C</bin/gzip>.
Corresponds to a C<.gz> suffix.

=item Z

Lempel-Ziv compressed file, as produced by, for example C</bin/compress>.
Corresponds to a C<.Z> suffix.

=item zip

Zip compressed file, as produced by, for example C</bin/zip>.
Corresponds to a C<.zip>, C<.jar> or C<.par> suffix.

=item bz2

Bzip2 compressed file, as produced by, for example, C</bin/bzip2>.
Corresponds to a C<.bz2> suffix.

=item tbz

Bzip2 compressed tar file, as produced by, for example C</bin/tar -j>.
Corresponds to a C<.tbz> or C<.tar.bz2> suffix.

=item lzma

Lzma compressed file, as produced by C</bin/lzma>.
Corresponds to a C<.lzma> suffix.

=item xz

Xz compressed file, as produced by C</bin/xz>.
Corresponds to a C<.xz> suffix.

=item txz

Xz compressed tar file, as produced by, for example C</bin/tar -J>.
Corresponds to a C<.txz> or C<.tar.xz> suffix.

=back

Returns a C<Archive::Extract> object on success, or false on failure.

=cut

    ### constructor ###
    sub new {
        my $class   = shift;
        my %hash    = @_;

        ### see above why we use subs here and generate the template;
        ### it's basically to not re-use arrayrefs
        my %utmpl   = map { $_ => $tmpl->{$_}->() } keys %$tmpl;

        my $parsed = check( \%utmpl, \%hash ) or return;

        ### make sure we have an absolute path ###
        my $ar = $parsed->{archive} = File::Spec->rel2abs( $parsed->{archive} );

        ### figure out the type, if it wasn't already specified ###
        unless ( $parsed->{type} ) {
            $parsed->{type} =
                $ar =~ /.+?\.(?:tar\.gz|tgz)$/i         ? TGZ   :
                $ar =~ /.+?\.gz$/i                      ? GZ    :
                $ar =~ /.+?\.tar$/i                     ? TAR   :
                $ar =~ /.+?\.(zip|jar|ear|war|par)$/i   ? ZIP   :
                $ar =~ /.+?\.(?:tbz2?|tar\.bz2?)$/i     ? TBZ   :
                $ar =~ /.+?\.bz2$/i                     ? BZ2   :
                $ar =~ /.+?\.Z$/                        ? Z     :
                $ar =~ /.+?\.lzma$/                     ? LZMA  :
                $ar =~ /.+?\.(?:txz|tar\.xz)$/i         ? TXZ   :
                $ar =~ /.+?\.xz$/                       ? XZ    :
                '';

        }

        bless $parsed, $class;

        ### don't know what type of file it is
        ### XXX this *has* to be an object call, not a package call
        return $parsed->_error(loc("Cannot determine file type for '%1'",
                                $parsed->{archive} )) unless $parsed->{type};
        return $parsed;
    }
}

=head2 $ae->extract( [to => '/output/path'] )

Extracts the archive represented by the C<Archive::Extract> object to
the path of your choice as specified by the C<to> argument. Defaults to
C<cwd()>.

Since C<.gz> files never hold a directory, but only a single file; if
the C<to> argument is an existing directory, the file is extracted
there, with its C<.gz> suffix stripped.
If the C<to> argument is not an existing directory, the C<to> argument
is understood to be a filename, if the archive type is C<gz>.
In the case that you did not specify a C<to> argument, the output
file will be the name of the archive file, stripped from its C<.gz>
suffix, in the current working directory.

C<extract> will try a pure perl solution first, and then fall back to
commandline tools if they are available. See the C<GLOBAL VARIABLES>
section below on how to alter this behaviour.

It will return true on success, and false on failure.

On success, it will also set the follow attributes in the object:

=over 4

=item $ae->extract_path

This is the directory that the files where extracted to.

=item $ae->files

This is an array ref with the paths of all the files in the archive,
relative to the C<to> argument you specified.
To get the full path to an extracted file, you would use:

    File::Spec->catfile( $to, $ae->files->[0] );

Note that all files from a tar archive will be in unix format, as per
the tar specification.

=back

=cut

sub extract {
    my $self = shift;
    my %hash = @_;

    ### reset error messages
    $self->_error_msg( [] );
    $self->_error_msg_long( [] );

    my $to;
    my $tmpl = {
        to  => { default => '.', store => \$to }
    };

    check( $tmpl, \%hash ) or return;

    ### so 'to' could be a file or a dir, depending on whether it's a .gz
    ### file, or basically anything else.
    ### so, check that, then act accordingly.
    ### set an accessor specifically so _gunzip can know what file to extract
    ### to.
    my $dir;
    {   ### a foo.gz file
        if( $self->is_gz or $self->is_bz2 or $self->is_Z or $self->is_lzma or $self->is_xz ) {

            my $cp = $self->archive; $cp =~ s/\.(?:gz|bz2?|Z|lzma|xz)$//i;

            ### to is a dir?
            if ( -d $to ) {
                $dir = $to;
                $self->_gunzip_to( basename($cp) );

            ### then it's a filename
            } else {
                $dir = dirname($to);
                $self->_gunzip_to( basename($to) );
            }

        ### not a foo.gz file
        } else {
            $dir = $to;
        }
    }

    ### make the dir if it doesn't exist ###
    unless( -d $dir ) {
        eval { mkpath( $dir ) };

        return $self->_error(loc("Could not create path '%1': %2", $dir, $@))
            if $@;
    }

    ### get the current dir, to restore later ###
    my $cwd = cwd();

    my $ok = 1;
    EXTRACT: {

        ### chdir to the target dir ###
        unless( chdir $dir ) {
            $self->_error(loc("Could not chdir to '%1': %2", $dir, $!));
            $ok = 0; last EXTRACT;
        }

        ### set files to an empty array ref, so there's always an array
        ### ref IN the accessor, to avoid errors like:
        ### Can't use an undefined value as an ARRAY reference at
        ### ../lib/Archive/Extract.pm line 742. (rt #19815)
        $self->files( [] );

        ### find out the dispatch methods needed for this type of
        ### archive. Do a $self->is_XXX to figure out the type, then
        ### get the hashref with bin + pure perl dispatchers.
        my ($map) = map { $Mapping->{$_} } grep { $self->$_ } keys %$Mapping;

        ### add pure perl extractor if allowed & add bin extractor if allowed
        my @methods;
        push @methods, $map->{'pp'}  if $_ALLOW_PURE_PERL;
        push @methods, $map->{'bin'} if $_ALLOW_BIN;

        ### reverse it if we prefer bin extractors
        @methods = reverse @methods if $PREFER_BIN;

        my($na, $fail);
        for my $method (@methods) {
            $self->debug( "# Extracting with ->$method\n" );

            my $rv = $self->$method;

            ### a positive extraction
            if( $rv and $rv ne METHOD_NA ) {
                $self->debug( "# Extraction succeeded\n" );
                $self->_extractor($method);
                last;

            ### method is not available
            } elsif ( $rv and $rv eq METHOD_NA ) {
                $self->debug( "# Extraction method not available\n" );
                $na++;
            } else {
                $self->debug( "# Extraction method failed\n" );
                $fail++;
            }
        }

        ### warn something went wrong if we didn't get an extractor
        unless( $self->_extractor ) {
            my $diag = $fail ? loc("Extract failed due to errors") :
                       $na   ? loc("Extract failed; no extractors available") :
                       '';

            $self->_error($diag);
            $ok = 0;
        }
    }

    ### and chdir back ###
    unless( chdir $cwd ) {
        $self->_error(loc("Could not chdir back to start dir '%1': %2'",
                            $cwd, $!));
    }

    return $ok;
}

=pod

=head1 ACCESSORS

=head2 $ae->error([BOOL])

Returns the last encountered error as string.
Pass it a true value to get the C<Carp::longmess()> output instead.

=head2 $ae->extract_path

This is the directory the archive got extracted to.
See C<extract()> for details.

=head2 $ae->files

This is an array ref holding all the paths from the archive.
See C<extract()> for details.

=head2 $ae->archive

This is the full path to the archive file represented by this
C<Archive::Extract> object.

=head2 $ae->type

This is the type of archive represented by this C<Archive::Extract>
object. See accessors below for an easier way to use this.
See the C<new()> method for details.

=head2 $ae->types

Returns a list of all known C<types> for C<Archive::Extract>'s
C<new> method.

=cut

sub types { return @Types }

=head2 $ae->is_tgz

Returns true if the file is of type C<.tar.gz>.
See the C<new()> method for details.

=head2 $ae->is_tar

Returns true if the file is of type C<.tar>.
See the C<new()> method for details.

=head2 $ae->is_gz

Returns true if the file is of type C<.gz>.
See the C<new()> method for details.

=head2 $ae->is_Z

Returns true if the file is of type C<.Z>.
See the C<new()> method for details.

=head2 $ae->is_zip

Returns true if the file is of type C<.zip>.
See the C<new()> method for details.

=head2 $ae->is_lzma

Returns true if the file is of type C<.lzma>.
See the C<new()> method for details.

=head2 $ae->is_xz

Returns true if the file is of type C<.xz>.
See the C<new()> method for details.

=cut

### quick check methods ###
sub is_tgz  { return $_[0]->type eq TGZ }
sub is_tar  { return $_[0]->type eq TAR }
sub is_gz   { return $_[0]->type eq GZ  }
sub is_zip  { return $_[0]->type eq ZIP }
sub is_tbz  { return $_[0]->type eq TBZ }
sub is_bz2  { return $_[0]->type eq BZ2 }
sub is_Z    { return $_[0]->type eq Z   }
sub is_lzma { return $_[0]->type eq LZMA }
sub is_xz   { return $_[0]->type eq XZ   }
sub is_txz  { return $_[0]->type eq TXZ }

=pod

=head2 $ae->bin_tar

Returns the full path to your tar binary, if found.

=head2 $ae->bin_gzip

Returns the full path to your gzip binary, if found

=head2 $ae->bin_unzip

Returns the full path to your unzip binary, if found

=head2 $ae->bin_unlzma

Returns the full path to your unlzma binary, if found

=head2 $ae->bin_unxz

Returns the full path to your unxz binary, if found

=cut

### paths to commandline tools ###
sub bin_gzip        { return $PROGRAMS->{'gzip'}    if $PROGRAMS->{'gzip'}  }
sub bin_unzip       { return $PROGRAMS->{'unzip'}   if $PROGRAMS->{'unzip'} }
sub bin_tar         { return $PROGRAMS->{'tar'}     if $PROGRAMS->{'tar'}   }
sub bin_bunzip2     { return $PROGRAMS->{'bunzip2'} if $PROGRAMS->{'bunzip2'} }
sub bin_uncompress  { return $PROGRAMS->{'uncompress'}
                                                 if $PROGRAMS->{'uncompress'} }
sub bin_unlzma      { return $PROGRAMS->{'unlzma'}  if $PROGRAMS->{'unlzma'} }
sub bin_unxz        { return $PROGRAMS->{'unxz'}    if $PROGRAMS->{'unxz'} }

=head2 $bool = $ae->have_old_bunzip2

Older versions of C</bin/bunzip2>, from before the C<bunzip2 1.0> release,
require all archive names to end in C<.bz2> or it will not extract
them. This method checks if you have a recent version of C<bunzip2>
that allows any extension, or an older one that doesn't.

=cut

sub have_old_bunzip2 {
    my $self = shift;

    ### no bunzip2? no old bunzip2 either :)
    return unless $self->bin_bunzip2;

    ### if we can't run this, we can't be sure if it's too old or not
    ### XXX stupid stupid stupid bunzip2 doesn't understand --version
    ### is not a request to extract data:
    ### $ bunzip2 --version
    ### bzip2, a block-sorting file compressor.  Version 1.0.2, 30-Dec-2001.
    ### [...]
    ### bunzip2: I won't read compressed data from a terminal.
    ### bunzip2: For help, type: `bunzip2 --help'.
    ### $ echo $?
    ### 1
    ### HATEFUL!

    ### double hateful: bunzip2 --version also hangs if input is a pipe
    ### See #32370: Archive::Extract will hang if stdin is a pipe [+PATCH]
    ### So, we have to provide *another* argument which is a fake filename,
    ### just so it wont try to read from stdin to print its version..
    ### *sigh*
    ### Even if the file exists, it won't clobber or change it.
    my $buffer;
    scalar run(
         command => [$self->bin_bunzip2, '--version', 'NoSuchFile'],
         verbose => 0,
         buffer  => \$buffer
    );

    ### no output
    return unless $buffer;

    my ($version) = $buffer =~ /version \s+ (\d+)/ix;

    return 1 if $version < 1;
    return;
}

#################################
#
# Untar code
#
#################################

### annoying issue with (gnu) tar on win32, as illustrated by this
### bug: https://rt.cpan.org/Ticket/Display.html?id=40138
### which shows that (gnu) tar will interpret a file name with a :
### in it as a remote file name, so C:\tmp\foo.txt is interpreted
### as a remote shell, and the extract fails.
{   my @ExtraTarFlags;
    if( ON_WIN32 and my $cmd = __PACKAGE__->bin_tar ) {

        ### if this is gnu tar we are running, we need to use --force-local
        push @ExtraTarFlags, '--force-local' if `$cmd --version` =~ /gnu tar/i;
    }


    ### use /bin/tar to extract ###
    sub _untar_bin {
        my $self = shift;

        ### check for /bin/tar ###
        ### check for /bin/gzip if we need it ###
        ### if any of the binaries are not available, return NA
        {   my $diag =  !$self->bin_tar ?
                            loc("No '%1' program found", '/bin/tar') :
                        $self->is_tgz && !$self->bin_gzip ?
                            loc("No '%1' program found", '/bin/gzip') :
                        $self->is_tbz && !$self->bin_bunzip2 ?
                            loc("No '%1' program found", '/bin/bunzip2') :
                        $self->is_txz && !$self->bin_unxz ?
                            loc("No '%1' program found", '/bin/unxz') :
                        '';

            if( $diag ) {
                $self->_error( $diag );
                return METHOD_NA;
            }
        }

        ### XXX figure out how to make IPC::Run do this in one call --
        ### currently i don't know how to get output of a command after a pipe
        ### trapped in a scalar. Mailed barries about this 5th of june 2004.

        ### see what command we should run, based on whether
        ### it's a .tgz or .tar

        ### GNU tar can't handled VMS filespecs, but VMSTAR can handle Unix filespecs.
        my $archive = $self->archive;
        $archive = VMS::Filespec::unixify($archive) if ON_VMS;

        ### XXX solaris tar and bsdtar are having different outputs
        ### depending whether you run with -x or -t
        ### compensate for this insanity by running -t first, then -x
        {    my $cmd =
                $self->is_tgz ? [$self->bin_gzip, '-c', '-d', '-f', $archive, '|',
                                 $self->bin_tar, '-tf', '-'] :
                $self->is_tbz ? [$self->bin_bunzip2, '-cd', $archive, '|',
                                 $self->bin_tar, '-tf', '-'] :
                $self->is_txz ? [$self->bin_unxz, '-cd', $archive, '|',
                                 $self->bin_tar, '-tf', '-'] :
                [$self->bin_tar, @ExtraTarFlags, '-tf', $archive];

            ### run the command
            ### newer versions of 'tar' (1.21 and up) now print record size
            ### to STDERR as well if v OR t is given (used to be both). This
            ### is a 'feature' according to the changelog, so we must now only
            ### inspect STDOUT, otherwise, failures like these occur:
            ### http://www.cpantesters.org/cpan/report/3230366
            my $buffer  = '';
            my @out     = run(  command => $cmd,
                                buffer  => \$buffer,
                                verbose => $DEBUG );

            ### command was unsuccessful
            unless( $out[0] ) {
                return $self->_error(loc(
                                "Error listing contents of archive '%1': %2",
                                $archive, $buffer ));
            }

            ### no buffers available?
            if( !IPC::Cmd->can_capture_buffer and !$buffer ) {
                $self->_error( $self->_no_buffer_files( $archive ) );

            } else {
                ### if we're on solaris we /might/ be using /bin/tar, which has
                ### a weird output format... we might also be using
                ### /usr/local/bin/tar, which is gnu tar, which is perfectly
                ### fine... so we have to do some guessing here =/
                my @files = map { chomp;
                              !ON_SOLARIS ? $_
                                          : (m|^ x \s+  # 'xtract' -- sigh
                                                (.+?),  # the actual file name
                                                \s+ [\d,.]+ \s bytes,
                                                \s+ [\d,.]+ \s tape \s blocks
                                            |x ? $1 : $_);

                        ### only STDOUT, see above. Sometimes, extra whitespace
                        ### is present, so make sure we only pick lines with
                        ### a length
                        } grep { length } map { split $/, $_ } join '', @{$out[3]};

                ### store the files that are in the archive ###
                $self->files(\@files);
            }
        }

        ### now actually extract it ###
        {   my $cmd =
                $self->is_tgz ? [$self->bin_gzip, '-c', '-d', '-f', $archive, '|',
                                 $self->bin_tar, '-xf', '-'] :
                $self->is_tbz ? [$self->bin_bunzip2, '-cd', $archive, '|',
                                 $self->bin_tar, '-xf', '-'] :
                $self->is_txz ? [$self->bin_unxz, '-cd', $archive, '|',
                                 $self->bin_tar, '-xf', '-'] :
                [$self->bin_tar, @ExtraTarFlags, '-xf', $archive];

            my $buffer = '';
            unless( scalar run( command => $cmd,
                                buffer  => \$buffer,
                                verbose => $DEBUG )
            ) {
                return $self->_error(loc("Error extracting archive '%1': %2",
                                $archive, $buffer ));
            }

            ### we might not have them, due to lack of buffers
            if( $self->files ) {
                ### now that we've extracted, figure out where we extracted to
                my $dir = $self->__get_extract_dir( $self->files );

                ### store the extraction dir ###
                $self->extract_path( $dir );
            }
        }

        ### we got here, no error happened
        return 1;
    }
}


### use archive::tar to extract ###
sub _untar_at {
    my $self = shift;

    ### Loading Archive::Tar is going to set it to 1, so make it local
    ### within this block, starting with its initial value. Whatever
    ### Achive::Tar does will be undone when we return.
    ###
    ### Also, later, set $Archive::Tar::WARN to $Archive::Extract::WARN
    ### so users don't have to even think about this variable. If they
    ### do, they still get their set value outside of this call.
    local $Archive::Tar::WARN = $Archive::Tar::WARN;

    ### we definitely need Archive::Tar, so load that first
    {   my $use_list = { 'Archive::Tar' => '0.0' };

        unless( can_load( modules => $use_list ) ) {

            $self->_error(loc("You do not have '%1' installed - " .
                              "Please install it as soon as possible.",
                              'Archive::Tar'));

            return METHOD_NA;
        }
    }

    ### we might pass it a filehandle if it's a .tbz file..
    my $fh_to_read = $self->archive;

    ### we will need Compress::Zlib too, if it's a tgz... and IO::Zlib
    ### if A::T's version is 0.99 or higher
    if( $self->is_tgz ) {
        my $use_list = { 'Compress::Zlib' => '0.0' };
           $use_list->{ 'IO::Zlib' } = '0.0'
                if $Archive::Tar::VERSION >= '0.99';

        unless( can_load( modules => $use_list ) ) {
            my $which = join '/', sort keys %$use_list;

            $self->_error(loc(
                "You do not have '%1' installed - Please ".
                "install it as soon as possible.", $which)
            );

            return METHOD_NA;
        }

    } elsif ( $self->is_tbz ) {
        my $use_list = { 'IO::Uncompress::Bunzip2' => '0.0' };
        unless( can_load( modules => $use_list ) ) {
            $self->_error(loc(
                "You do not have '%1' installed - Please " .
                "install it as soon as possible.",
                'IO::Uncompress::Bunzip2')
            );

            return METHOD_NA;
        }

        my $bz = IO::Uncompress::Bunzip2->new( $self->archive ) or
            return $self->_error(loc("Unable to open '%1': %2",
                            $self->archive,
                            $IO::Uncompress::Bunzip2::Bunzip2Error));

        $fh_to_read = $bz;
    } elsif ( $self->is_txz ) {
        my $use_list = { 'IO::Uncompress::UnXz' => '0.0' };
        unless( can_load( modules => $use_list ) ) {
            $self->_error(loc(
                "You do not have '%1' installed - Please " .
                "install it as soon as possible.",
                'IO::Uncompress::UnXz')
            );

            return METHOD_NA;
        }

        my $xz = IO::Uncompress::UnXz->new( $self->archive ) or
            return $self->_error(loc("Unable to open '%1': %2",
                            $self->archive,
                            $IO::Uncompress::UnXz::UnXzError));

        $fh_to_read = $xz;
    }

    my @files;
    {
        ### $Archive::Tar::WARN is 1 by default in Archive::Tar, but we've
        ### localized $Archive::Tar::WARN already.
        $Archive::Tar::WARN = $Archive::Extract::WARN;

        ### only tell it it's compressed if it's a .tgz, as we give it a file
        ### handle if it's a .tbz
        my @read = ( $fh_to_read, ( $self->is_tgz ? 1 : 0 ) );

        ### for version of Archive::Tar > 1.04
        local $Archive::Tar::CHOWN = 0;

        ### use the iterator if we can. it's a feature of A::T 1.40 and up
        if ( $_ALLOW_TAR_ITER && Archive::Tar->can( 'iter' ) ) {

            my $next;
            unless ( $next = Archive::Tar->iter( @read ) ) {
                return $self->_error(loc(
                            "Unable to read '%1': %2", $self->archive,
                            $Archive::Tar::error));
            }

            while ( my $file = $next->() ) {
                push @files, $file->full_path;

                $file->extract or return $self->_error(loc(
                        "Unable to read '%1': %2",
                        $self->archive,
                        $Archive::Tar::error));
            }

        ### older version, read the archive into memory
        } else {

            my $tar = Archive::Tar->new();

            unless( $tar->read( @read ) ) {
                return $self->_error(loc("Unable to read '%1': %2",
                            $self->archive, $Archive::Tar::error));
            }

            ### workaround to prevent Archive::Tar from setting uid, which
            ### is a potential security hole. -autrijus
            ### have to do it here, since A::T needs to be /loaded/ first ###
            {   no strict 'refs'; local $^W;

                ### older versions of archive::tar <= 0.23
                *Archive::Tar::chown = sub {};
            }

            {   local $^W;  # quell 'splice() offset past end of array' warnings
                            # on older versions of A::T

                ### older archive::tar always returns $self, return value
                ### slightly fux0r3d because of it.
                $tar->extract or return $self->_error(loc(
                        "Unable to extract '%1': %2",
                        $self->archive, $Archive::Tar::error ));
            }

            @files = $tar->list_files;
        }
    }

    my $dir = $self->__get_extract_dir( \@files );

    ### store the files that are in the archive ###
    $self->files(\@files);

    ### store the extraction dir ###
    $self->extract_path( $dir );

    ### check if the dir actually appeared ###
    return 1 if -d $self->extract_path;

    ### no dir, we failed ###
    return $self->_error(loc("Unable to extract '%1': %2",
                                $self->archive, $Archive::Tar::error ));
}

#################################
#
# Gunzip code
#
#################################

sub _gunzip_bin {
    my $self = shift;

    ### check for /bin/gzip -- we need it ###
    unless( $self->bin_gzip ) {
        $self->_error(loc("No '%1' program found", '/bin/gzip'));
        return METHOD_NA;
    }

    my $fh = FileHandle->new('>'. $self->_gunzip_to) or
        return $self->_error(loc("Could not open '%1' for writing: %2",
                            $self->_gunzip_to, $! ));

    my $cmd = [ $self->bin_gzip, '-c', '-d', '-f', $self->archive ];

    my $buffer;
    unless( scalar run( command => $cmd,
                        verbose => $DEBUG,
                        buffer  => \$buffer )
    ) {
        return $self->_error(loc("Unable to gunzip '%1': %2",
                                    $self->archive, $buffer));
    }

    ### no buffers available?
    if( !IPC::Cmd->can_capture_buffer and !$buffer ) {
        $self->_error( $self->_no_buffer_content( $self->archive ) );
    }

    $self->_print($fh, $buffer) if defined $buffer;

    close $fh;

    ### set what files where extract, and where they went ###
    $self->files( [$self->_gunzip_to] );
    $self->extract_path( File::Spec->rel2abs(cwd()) );

    return 1;
}

sub _gunzip_cz {
    my $self = shift;

    my $use_list = { 'Compress::Zlib' => '0.0' };
    unless( can_load( modules => $use_list ) ) {
        $self->_error(loc("You do not have '%1' installed - Please " .
                    "install it as soon as possible.", 'Compress::Zlib'));
        return METHOD_NA;
    }

    my $gz = Compress::Zlib::gzopen( $self->archive, "rb" ) or
                return $self->_error(loc("Unable to open '%1': %2",
                            $self->archive, $Compress::Zlib::gzerrno));

    my $fh = FileHandle->new('>'. $self->_gunzip_to) or
        return $self->_error(loc("Could not open '%1' for writing: %2",
                            $self->_gunzip_to, $! ));

    my $buffer;
    $self->_print($fh, $buffer) while $gz->gzread($buffer) > 0;
    $fh->close;

    ### set what files where extract, and where they went ###
    $self->files( [$self->_gunzip_to] );
    $self->extract_path( File::Spec->rel2abs(cwd()) );

    return 1;
}

#################################
#
# Uncompress code
#
#################################

sub _uncompress_bin {
    my $self = shift;

    ### check for /bin/gzip -- we need it ###
    unless( $self->bin_uncompress ) {
        $self->_error(loc("No '%1' program found", '/bin/uncompress'));
        return METHOD_NA;
    }

    my $fh = FileHandle->new('>'. $self->_gunzip_to) or
        return $self->_error(loc("Could not open '%1' for writing: %2",
                            $self->_gunzip_to, $! ));

    my $cmd = [ $self->bin_uncompress, '-c', $self->archive ];

    my $buffer;
    unless( scalar run( command => $cmd,
                        verbose => $DEBUG,
                        buffer  => \$buffer )
    ) {
        return $self->_error(loc("Unable to uncompress '%1': %2",
                                    $self->archive, $buffer));
    }

    ### no buffers available?
    if( !IPC::Cmd->can_capture_buffer and !$buffer ) {
        $self->_error( $self->_no_buffer_content( $self->archive ) );
    }

    $self->_print($fh, $buffer) if defined $buffer;

    close $fh;

    ### set what files where extract, and where they went ###
    $self->files( [$self->_gunzip_to] );
    $self->extract_path( File::Spec->rel2abs(cwd()) );

    return 1;
}


#################################
#
# Unzip code
#
#################################


sub _unzip_bin {
    my $self = shift;

    ### check for /bin/gzip if we need it ###
    unless( $self->bin_unzip ) {
        $self->_error(loc("No '%1' program found", '/bin/unzip'));
        return METHOD_NA;
    }

    ### first, get the files.. it must be 2 different commands with 'unzip' :(
    {   ### on VMS, capital letter options have to be quoted. This is
        ### reported by John Malmberg on P5P Tue 21 Aug 2007 05:05:11
        ### Subject: [patch@31735]Archive Extract fix on VMS.
        my $opt = ON_VMS ? '"-Z"' : '-Z';
        my $cmd = [ $self->bin_unzip, $opt, '-1', $self->archive ];

        my $buffer = '';
        unless( scalar run( command => $cmd,
                            verbose => $DEBUG,
                            buffer  => \$buffer )
        ) {
            return $self->_error(loc("Unable to unzip '%1': %2",
                                        $self->archive, $buffer));
        }

        ### no buffers available?
        if( !IPC::Cmd->can_capture_buffer and !$buffer ) {
            $self->_error( $self->_no_buffer_files( $self->archive ) );

        } else {
            ### Annoyingly, pesky MSWin32 can either have 'native' tools
            ### which have \r\n line endings or Cygwin-based tools which
            ### have \n line endings. Jan Dubois suggested using this fix
            my $split = ON_WIN32 ? qr/\r?\n/ : "\n";
            $self->files( [split $split, $buffer] );
        }
    }

    ### now, extract the archive ###
    {   my $cmd = [ $self->bin_unzip, '-qq', '-o', $self->archive ];

        my $buffer;
        unless( scalar run( command => $cmd,
                            verbose => $DEBUG,
                            buffer  => \$buffer )
        ) {
            return $self->_error(loc("Unable to unzip '%1': %2",
                                        $self->archive, $buffer));
        }

        if( scalar @{$self->files} ) {
            my $files   = $self->files;
            my $dir     = $self->__get_extract_dir( $files );

            $self->extract_path( $dir );
        }
    }

    return 1;
}

sub _unzip_az {
    my $self = shift;

    my $use_list = { 'Archive::Zip' => '0.0' };
    unless( can_load( modules => $use_list ) ) {
        $self->_error(loc("You do not have '%1' installed - Please " .
                      "install it as soon as possible.", 'Archive::Zip'));
        return METHOD_NA;
    }

    my $zip = Archive::Zip->new();

    unless( $zip->read( $self->archive ) == &Archive::Zip::AZ_OK ) {
        return $self->_error(loc("Unable to read '%1'", $self->archive));
    }

    my @files;


    ### Address: #43278: Explicitly tell Archive::Zip where to put the files:
    ### "In my BackPAN indexing, Archive::Zip was extracting things
    ### in my script's directory instead of the current working directory.
    ### I traced this back through Archive::Zip::_asLocalName which
    ### eventually calls File::Spec::Win32::rel2abs which on Windows might
    ### call Cwd::getdcwd. getdcwd returns the wrong directory in my
    ### case, even though I think I'm on the same drive.
    ###
    ### To fix this, I pass the optional second argument to
    ### extractMember using the cwd from Archive::Extract." --bdfoy

    ## store cwd() before looping; calls to cwd() can be expensive, and
    ### it won't change during the loop
    my $extract_dir = cwd();

    ### have to extract every member individually ###
    for my $member ($zip->members) {
        push @files, $member->{fileName};

        ### file to extract to, to avoid the above problem
        my $to = File::Spec->catfile( $extract_dir, $member->{fileName} );

        unless( $zip->extractMember($member, $to) == &Archive::Zip::AZ_OK ) {
            return $self->_error(loc("Extraction of '%1' from '%2' failed",
                        $member->{fileName}, $self->archive ));
        }
    }

    my $dir = $self->__get_extract_dir( \@files );

    ### set what files where extract, and where they went ###
    $self->files( \@files );
    $self->extract_path( File::Spec->rel2abs($dir) );

    return 1;
}

sub __get_extract_dir {
    my $self    = shift;
    my $files   = shift || [];

    return unless scalar @$files;

    my($dir1, $dir2);
    for my $aref ( [ \$dir1, 0 ], [ \$dir2, -1 ] ) {
        my($dir,$pos) = @$aref;

        ### add a catdir(), so that any trailing slashes get
        ### take care of (removed)
        ### also, a catdir() normalises './dir/foo' to 'dir/foo';
        ### which was the problem in bug #23999
        my $res = -d $files->[$pos]
                    ? File::Spec->catdir( $files->[$pos], '' )
                    : File::Spec->catdir( dirname( $files->[$pos] ) );

        $$dir = $res;
    }

    ### if the first and last dir don't match, make sure the
    ### dirname is not set wrongly
    my $dir;

    ### dirs are the same, so we know for sure what the extract dir is
    if( $dir1 eq $dir2 ) {
        $dir = $dir1;

    ### dirs are different.. do they share the base dir?
    ### if so, use that, if not, fall back to '.'
    } else {
        my $base1 = [ File::Spec->splitdir( $dir1 ) ]->[0];
        my $base2 = [ File::Spec->splitdir( $dir2 ) ]->[0];

        $dir = File::Spec->rel2abs( $base1 eq $base2 ? $base1 : '.' );
    }

    return File::Spec->rel2abs( $dir );
}

#################################
#
# Bunzip2 code
#
#################################

sub _bunzip2_bin {
    my $self = shift;

    ### check for /bin/gzip -- we need it ###
    unless( $self->bin_bunzip2 ) {
        $self->_error(loc("No '%1' program found", '/bin/bunzip2'));
        return METHOD_NA;
    }

    my $fh = FileHandle->new('>'. $self->_gunzip_to) or
        return $self->_error(loc("Could not open '%1' for writing: %2",
                            $self->_gunzip_to, $! ));

    ### guard against broken bunzip2. See ->have_old_bunzip2()
    ### for details
    if( $self->have_old_bunzip2 and $self->archive !~ /\.bz2$/i ) {
        return $self->_error(loc("Your bunzip2 version is too old and ".
                                 "can only extract files ending in '%1'",
                                 '.bz2'));
    }

    my $cmd = [ $self->bin_bunzip2, '-cd', $self->archive ];

    my $buffer;
    unless( scalar run( command => $cmd,
                        verbose => $DEBUG,
                        buffer  => \$buffer )
    ) {
        return $self->_error(loc("Unable to bunzip2 '%1': %2",
                                    $self->archive, $buffer));
    }

    ### no buffers available?
    if( !IPC::Cmd->can_capture_buffer and !$buffer ) {
        $self->_error( $self->_no_buffer_content( $self->archive ) );
    }

    $self->_print($fh, $buffer) if defined $buffer;

    close $fh;

    ### set what files where extract, and where they went ###
    $self->files( [$self->_gunzip_to] );
    $self->extract_path( File::Spec->rel2abs(cwd()) );

    return 1;
}

### using cz2, the compact versions... this we use mainly in archive::tar
### extractor..
# sub _bunzip2_cz1 {
#     my $self = shift;
#
#     my $use_list = { 'IO::Uncompress::Bunzip2' => '0.0' };
#     unless( can_load( modules => $use_list ) ) {
#         return $self->_error(loc("You do not have '%1' installed - Please " .
#                         "install it as soon as possible.",
#                         'IO::Uncompress::Bunzip2'));
#     }
#
#     my $bz = IO::Uncompress::Bunzip2->new( $self->archive ) or
#                 return $self->_error(loc("Unable to open '%1': %2",
#                             $self->archive,
#                             $IO::Uncompress::Bunzip2::Bunzip2Error));
#
#     my $fh = FileHandle->new('>'. $self->_gunzip_to) or
#         return $self->_error(loc("Could not open '%1' for writing: %2",
#                             $self->_gunzip_to, $! ));
#
#     my $buffer;
#     $fh->print($buffer) while $bz->read($buffer) > 0;
#     $fh->close;
#
#     ### set what files where extract, and where they went ###
#     $self->files( [$self->_gunzip_to] );
#     $self->extract_path( File::Spec->rel2abs(cwd()) );
#
#     return 1;
# }

sub _bunzip2_bz2 {
    my $self = shift;

    my $use_list = { 'IO::Uncompress::Bunzip2' => '0.0' };
    unless( can_load( modules => $use_list ) ) {
        $self->_error(loc("You do not have '%1' installed - Please " .
                          "install it as soon as possible.",
                          'IO::Uncompress::Bunzip2'));
        return METHOD_NA;
    }

    IO::Uncompress::Bunzip2::bunzip2($self->archive => $self->_gunzip_to)
        or return $self->_error(loc("Unable to uncompress '%1': %2",
                            $self->archive,
                            $IO::Uncompress::Bunzip2::Bunzip2Error));

    ### set what files where extract, and where they went ###
    $self->files( [$self->_gunzip_to] );
    $self->extract_path( File::Spec->rel2abs(cwd()) );

    return 1;
}

#################################
#
# UnXz code
#
#################################

sub _unxz_bin {
    my $self = shift;

    ### check for /bin/unxz -- we need it ###
    unless( $self->bin_unxz ) {
        $self->_error(loc("No '%1' program found", '/bin/unxz'));
        return METHOD_NA;
    }

    my $fh = FileHandle->new('>'. $self->_gunzip_to) or
        return $self->_error(loc("Could not open '%1' for writing: %2",
                            $self->_gunzip_to, $! ));

    my $cmd = [ $self->bin_unxz, '-c', '-d', '-f', $self->archive ];

    my $buffer;
    unless( scalar run( command => $cmd,
                        verbose => $DEBUG,
                        buffer  => \$buffer )
    ) {
        return $self->_error(loc("Unable to unxz '%1': %2",
                                    $self->archive, $buffer));
    }

    ### no buffers available?
    if( !IPC::Cmd->can_capture_buffer and !$buffer ) {
        $self->_error( $self->_no_buffer_content( $self->archive ) );
    }

    $self->_print($fh, $buffer) if defined $buffer;

    close $fh;

    ### set what files where extract, and where they went ###
    $self->files( [$self->_gunzip_to] );
    $self->extract_path( File::Spec->rel2abs(cwd()) );

    return 1;
}

sub _unxz_cz {
    my $self = shift;

    my $use_list = { 'IO::Uncompress::UnXz' => '0.0' };
    unless( can_load( modules => $use_list ) ) {
        $self->_error(loc("You do not have '%1' installed - Please " .
                          "install it as soon as possible.",
                          'IO::Uncompress::UnXz'));
        return METHOD_NA;
    }

    IO::Uncompress::UnXz::unxz($self->archive => $self->_gunzip_to)
        or return $self->_error(loc("Unable to uncompress '%1': %2",
                            $self->archive,
                            $IO::Uncompress::UnXz::UnXzError));

    ### set what files where extract, and where they went ###
    $self->files( [$self->_gunzip_to] );
    $self->extract_path( File::Spec->rel2abs(cwd()) );

    return 1;
}


#################################
#
# unlzma code
#
#################################

sub _unlzma_bin {
    my $self = shift;

    ### check for /bin/unlzma -- we need it ###
    unless( $self->bin_unlzma ) {
        $self->_error(loc("No '%1' program found", '/bin/unlzma'));
        return METHOD_NA;
    }

    my $fh = FileHandle->new('>'. $self->_gunzip_to) or
        return $self->_error(loc("Could not open '%1' for writing: %2",
                            $self->_gunzip_to, $! ));

    my $cmd = [ $self->bin_unlzma, '-c', $self->archive ];

    my $buffer;
    unless( scalar run( command => $cmd,
                        verbose => $DEBUG,
                        buffer  => \$buffer )
    ) {
        return $self->_error(loc("Unable to unlzma '%1': %2",
                                    $self->archive, $buffer));
    }

    ### no buffers available?
    if( !IPC::Cmd->can_capture_buffer and !$buffer ) {
        $self->_error( $self->_no_buffer_content( $self->archive ) );
    }

    $self->_print($fh, $buffer) if defined $buffer;

    close $fh;

    ### set what files where extract, and where they went ###
    $self->files( [$self->_gunzip_to] );
    $self->extract_path( File::Spec->rel2abs(cwd()) );

    return 1;
}

sub _unlzma_cz {
    my $self = shift;

    my $use_list1 = { 'IO::Uncompress::UnLzma' => '0.0' };
    my $use_list2 = { 'Compress::unLZMA' => '0.0' };

    if (can_load( modules => $use_list1 ) ) {
        IO::Uncompress::UnLzma::unlzma($self->archive => $self->_gunzip_to)
            or return $self->_error(loc("Unable to uncompress '%1': %2",
                                $self->archive,
                                $IO::Uncompress::UnLzma::UnLzmaError));
    }
    elsif (can_load( modules => $use_list2 ) ) {

        my $fh = FileHandle->new('>'. $self->_gunzip_to) or
            return $self->_error(loc("Could not open '%1' for writing: %2",
                                $self->_gunzip_to, $! ));

        my $buffer;
        $buffer = Compress::unLZMA::uncompressfile( $self->archive );
        unless ( defined $buffer ) {
            return $self->_error(loc("Could not unlzma '%1': %2",
                                        $self->archive, $@));
        }

        $self->_print($fh, $buffer) if defined $buffer;

        close $fh;
    }
    else {
        $self->_error(loc("You do not have '%1' or '%2' installed - Please " .
                    "install it as soon as possible.", 'Compress::unLZMA', 'IO::Uncompress::UnLzma'));
        return METHOD_NA;
    }

    ### set what files where extract, and where they went ###
    $self->files( [$self->_gunzip_to] );
    $self->extract_path( File::Spec->rel2abs(cwd()) );

    return 1;
}

#################################
#
# Error code
#
#################################

# For printing binaries that avoids interfering globals
sub _print {
    my $self = shift;
    my $fh = shift;

    local( $\, $", $, ) = ( undef, ' ', '' );
    return print $fh @_;
}

sub _error {
    my $self    = shift;
    my $error   = shift;
    my $lerror  = Carp::longmess($error);

    push @{$self->_error_msg},      $error;
    push @{$self->_error_msg_long}, $lerror;

    ### set $Archive::Extract::WARN to 0 to disable printing
    ### of errors
    if( $WARN ) {
        carp $DEBUG ? $lerror : $error;
    }

    return;
}

sub error {
    my $self = shift;

    ### make sure we have a fallback aref
    my $aref = do {
        shift()
            ? $self->_error_msg_long
            : $self->_error_msg
    } || [];

    return join $/, @$aref;
}

=head2 debug( MESSAGE )

This method outputs MESSAGE to the default filehandle if C<$DEBUG> is
true. It's a small method, but it's here if you'd like to subclass it
so you can so something else with any debugging output.

=cut

### this is really a stub for subclassing
sub debug {
    return unless $DEBUG;

    print $_[1];
}

sub _no_buffer_files {
    my $self = shift;
    my $file = shift or return;
    return loc("No buffer captured, unable to tell ".
               "extracted files or extraction dir for '%1'", $file);
}

sub _no_buffer_content {
    my $self = shift;
    my $file = shift or return;
    return loc("No buffer captured, unable to get content for '%1'", $file);
}
1;

=pod

=head1 HOW IT WORKS

C<Archive::Extract> tries first to determine what type of archive you
are passing it, by inspecting its suffix. It does not do this by using
Mime magic, or something related. See C<CAVEATS> below.

Once it has determined the file type, it knows which extraction methods
it can use on the archive. It will try a perl solution first, then fall
back to a commandline tool if that fails. If that also fails, it will
return false, indicating it was unable to extract the archive.
See the section on C<GLOBAL VARIABLES> to see how to alter this order.

=head1 CAVEATS

=head2 File Extensions

C<Archive::Extract> trusts on the extension of the archive to determine
what type it is, and what extractor methods therefore can be used. If
your archives do not have any of the extensions as described in the
C<new()> method, you will have to specify the type explicitly, or
C<Archive::Extract> will not be able to extract the archive for you.

=head2 Supporting Very Large Files

C<Archive::Extract> can use either pure perl modules or command line
programs under the hood. Some of the pure perl modules (like
C<Archive::Tar> and Compress::unLZMA) take the entire contents of the archive into memory,
which may not be feasible on your system. Consider setting the global
variable C<$Archive::Extract::PREFER_BIN> to C<1>, which will prefer
the use of command line programs and won't consume so much memory.

See the C<GLOBAL VARIABLES> section below for details.

=head2 Bunzip2 support of arbitrary extensions.

Older versions of C</bin/bunzip2> do not support arbitrary file
extensions and insist on a C<.bz2> suffix. Although we do our best
to guard against this, if you experience a bunzip2 error, it may
be related to this. For details, please see the C<have_old_bunzip2>
method.

=head1 GLOBAL VARIABLES

=head2 $Archive::Extract::DEBUG

Set this variable to C<true> to have all calls to command line tools
be printed out, including all their output.
This also enables C<Carp::longmess> errors, instead of the regular
C<carp> errors.

Good for tracking down why things don't work with your particular
setup.

Defaults to C<false>.

=head2 $Archive::Extract::WARN

This variable controls whether errors encountered internally by
C<Archive::Extract> should be C<carp>'d or not.

Set to false to silence warnings. Inspect the output of the C<error()>
method manually to see what went wrong.

Defaults to C<true>.

=head2 $Archive::Extract::PREFER_BIN

This variables controls whether C<Archive::Extract> should prefer the
use of perl modules, or commandline tools to extract archives.

Set to C<true> to have C<Archive::Extract> prefer commandline tools.

Defaults to C<false>.

=head1 TODO / CAVEATS

=over 4

=item Mime magic support

Maybe this module should use something like C<File::Type> to determine
the type, rather than blindly trust the suffix.

=item Thread safety

Currently, C<Archive::Extract> does a C<chdir> to the extraction dir before
extraction, and a C<chdir> back again after. This is not necessarily
thread safe. See C<rt.cpan.org> bug C<#45671> for details.

=back

=head1 BUG REPORTS

Please report bugs or other issues to E<lt>bug-archive-extract@rt.cpan.orgE<gt>.

=head1 AUTHOR

This module by Jos Boumans E<lt>kane@cpan.orgE<gt>.

=head1 COPYRIGHT

This library is free software; you may redistribute and/or modify it
under the same terms as Perl itself.

=cut

# Local variables:
# c-indentation-style: bsd
# c-basic-offset: 4
# indent-tabs-mode: nil
# End:
# vim: expandtab shiftwidth=4:


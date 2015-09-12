package Archive::Zip;

use 5.006;
use strict;
use Carp                ();
use Cwd                 ();
use IO::File            ();
use IO::Seekable        ();
use Compress::Raw::Zlib ();
use File::Spec          ();
use File::Temp          ();
use FileHandle          ();

use vars qw( $VERSION @ISA );

BEGIN {
    $VERSION = '1.48';

    require Exporter;
    @ISA = qw( Exporter );
}

use vars qw( $ChunkSize $ErrorHandler );

BEGIN {
    # This is the size we'll try to read, write, and (de)compress.
    # You could set it to something different if you had lots of memory
    # and needed more speed.
    $ChunkSize ||= 32768;

    $ErrorHandler = \&Carp::carp;
}

# BEGIN block is necessary here so that other modules can use the constants.
use vars qw( @EXPORT_OK %EXPORT_TAGS );

BEGIN {
    @EXPORT_OK   = ('computeCRC32');
    %EXPORT_TAGS = (
        CONSTANTS => [
            qw(
              FA_MSDOS
              FA_UNIX
              GPBF_ENCRYPTED_MASK
              GPBF_DEFLATING_COMPRESSION_MASK
              GPBF_HAS_DATA_DESCRIPTOR_MASK
              COMPRESSION_STORED
              COMPRESSION_DEFLATED
              COMPRESSION_LEVEL_NONE
              COMPRESSION_LEVEL_DEFAULT
              COMPRESSION_LEVEL_FASTEST
              COMPRESSION_LEVEL_BEST_COMPRESSION
              IFA_TEXT_FILE_MASK
              IFA_TEXT_FILE
              IFA_BINARY_FILE
              )
        ],

        MISC_CONSTANTS => [
            qw(
              FA_AMIGA
              FA_VAX_VMS
              FA_VM_CMS
              FA_ATARI_ST
              FA_OS2_HPFS
              FA_MACINTOSH
              FA_Z_SYSTEM
              FA_CPM
              FA_TOPS20
              FA_WINDOWS_NTFS
              FA_QDOS
              FA_ACORN
              FA_VFAT
              FA_MVS
              FA_BEOS
              FA_TANDEM
              FA_THEOS
              GPBF_IMPLODING_8K_SLIDING_DICTIONARY_MASK
              GPBF_IMPLODING_3_SHANNON_FANO_TREES_MASK
              GPBF_IS_COMPRESSED_PATCHED_DATA_MASK
              COMPRESSION_SHRUNK
              DEFLATING_COMPRESSION_NORMAL
              DEFLATING_COMPRESSION_MAXIMUM
              DEFLATING_COMPRESSION_FAST
              DEFLATING_COMPRESSION_SUPER_FAST
              COMPRESSION_REDUCED_1
              COMPRESSION_REDUCED_2
              COMPRESSION_REDUCED_3
              COMPRESSION_REDUCED_4
              COMPRESSION_IMPLODED
              COMPRESSION_TOKENIZED
              COMPRESSION_DEFLATED_ENHANCED
              COMPRESSION_PKWARE_DATA_COMPRESSION_LIBRARY_IMPLODED
              )
        ],

        ERROR_CODES => [
            qw(
              AZ_OK
              AZ_STREAM_END
              AZ_ERROR
              AZ_FORMAT_ERROR
              AZ_IO_ERROR
              )
        ],

        # For Internal Use Only
        PKZIP_CONSTANTS => [
            qw(
              SIGNATURE_FORMAT
              SIGNATURE_LENGTH
              LOCAL_FILE_HEADER_SIGNATURE
              LOCAL_FILE_HEADER_FORMAT
              LOCAL_FILE_HEADER_LENGTH
              CENTRAL_DIRECTORY_FILE_HEADER_SIGNATURE
              DATA_DESCRIPTOR_FORMAT
              DATA_DESCRIPTOR_LENGTH
              DATA_DESCRIPTOR_SIGNATURE
              DATA_DESCRIPTOR_FORMAT_NO_SIG
              DATA_DESCRIPTOR_LENGTH_NO_SIG
              CENTRAL_DIRECTORY_FILE_HEADER_FORMAT
              CENTRAL_DIRECTORY_FILE_HEADER_LENGTH
              END_OF_CENTRAL_DIRECTORY_SIGNATURE
              END_OF_CENTRAL_DIRECTORY_SIGNATURE_STRING
              END_OF_CENTRAL_DIRECTORY_FORMAT
              END_OF_CENTRAL_DIRECTORY_LENGTH
              )
        ],

        # For Internal Use Only
        UTILITY_METHODS => [
            qw(
              _error
              _printError
              _ioError
              _formatError
              _subclassResponsibility
              _binmode
              _isSeekable
              _newFileHandle
              _readSignature
              _asZipDirName
              )
        ],
    );

    # Add all the constant names and error code names to @EXPORT_OK
    Exporter::export_ok_tags(
        qw(
          CONSTANTS
          ERROR_CODES
          PKZIP_CONSTANTS
          UTILITY_METHODS
          MISC_CONSTANTS
          ));

}

# Error codes
use constant AZ_OK           => 0;
use constant AZ_STREAM_END   => 1;
use constant AZ_ERROR        => 2;
use constant AZ_FORMAT_ERROR => 3;
use constant AZ_IO_ERROR     => 4;

# File types
# Values of Archive::Zip::Member->fileAttributeFormat()

use constant FA_MSDOS        => 0;
use constant FA_AMIGA        => 1;
use constant FA_VAX_VMS      => 2;
use constant FA_UNIX         => 3;
use constant FA_VM_CMS       => 4;
use constant FA_ATARI_ST     => 5;
use constant FA_OS2_HPFS     => 6;
use constant FA_MACINTOSH    => 7;
use constant FA_Z_SYSTEM     => 8;
use constant FA_CPM          => 9;
use constant FA_TOPS20       => 10;
use constant FA_WINDOWS_NTFS => 11;
use constant FA_QDOS         => 12;
use constant FA_ACORN        => 13;
use constant FA_VFAT         => 14;
use constant FA_MVS          => 15;
use constant FA_BEOS         => 16;
use constant FA_TANDEM       => 17;
use constant FA_THEOS        => 18;

# general-purpose bit flag masks
# Found in Archive::Zip::Member->bitFlag()

use constant GPBF_ENCRYPTED_MASK             => 1 << 0;
use constant GPBF_DEFLATING_COMPRESSION_MASK => 3 << 1;
use constant GPBF_HAS_DATA_DESCRIPTOR_MASK   => 1 << 3;

# deflating compression types, if compressionMethod == COMPRESSION_DEFLATED
# ( Archive::Zip::Member->bitFlag() & GPBF_DEFLATING_COMPRESSION_MASK )

use constant DEFLATING_COMPRESSION_NORMAL     => 0 << 1;
use constant DEFLATING_COMPRESSION_MAXIMUM    => 1 << 1;
use constant DEFLATING_COMPRESSION_FAST       => 2 << 1;
use constant DEFLATING_COMPRESSION_SUPER_FAST => 3 << 1;

# compression method

# these two are the only ones supported in this module
use constant COMPRESSION_STORED        => 0;   # file is stored (no compression)
use constant COMPRESSION_DEFLATED      => 8;   # file is Deflated
use constant COMPRESSION_LEVEL_NONE    => 0;
use constant COMPRESSION_LEVEL_DEFAULT => -1;
use constant COMPRESSION_LEVEL_FASTEST => 1;
use constant COMPRESSION_LEVEL_BEST_COMPRESSION => 9;

# internal file attribute bits
# Found in Archive::Zip::Member::internalFileAttributes()

use constant IFA_TEXT_FILE_MASK => 1;
use constant IFA_TEXT_FILE      => 1;
use constant IFA_BINARY_FILE    => 0;

# PKZIP file format miscellaneous constants (for internal use only)
use constant SIGNATURE_FORMAT => "V";
use constant SIGNATURE_LENGTH => 4;

# these lengths are without the signature.
use constant LOCAL_FILE_HEADER_SIGNATURE => 0x04034b50;
use constant LOCAL_FILE_HEADER_FORMAT    => "v3 V4 v2";
use constant LOCAL_FILE_HEADER_LENGTH    => 26;

# PKZIP docs don't mention the signature, but Info-Zip writes it.
use constant DATA_DESCRIPTOR_SIGNATURE => 0x08074b50;
use constant DATA_DESCRIPTOR_FORMAT    => "V3";
use constant DATA_DESCRIPTOR_LENGTH    => 12;

# but the signature is apparently optional.
use constant DATA_DESCRIPTOR_FORMAT_NO_SIG => "V2";
use constant DATA_DESCRIPTOR_LENGTH_NO_SIG => 8;

use constant CENTRAL_DIRECTORY_FILE_HEADER_SIGNATURE => 0x02014b50;
use constant CENTRAL_DIRECTORY_FILE_HEADER_FORMAT    => "C2 v3 V4 v5 V2";
use constant CENTRAL_DIRECTORY_FILE_HEADER_LENGTH    => 42;

use constant END_OF_CENTRAL_DIRECTORY_SIGNATURE => 0x06054b50;
use constant END_OF_CENTRAL_DIRECTORY_SIGNATURE_STRING =>
  pack("V", END_OF_CENTRAL_DIRECTORY_SIGNATURE);
use constant END_OF_CENTRAL_DIRECTORY_FORMAT => "v4 V2 v";
use constant END_OF_CENTRAL_DIRECTORY_LENGTH => 18;

use constant GPBF_IMPLODING_8K_SLIDING_DICTIONARY_MASK => 1 << 1;
use constant GPBF_IMPLODING_3_SHANNON_FANO_TREES_MASK  => 1 << 2;
use constant GPBF_IS_COMPRESSED_PATCHED_DATA_MASK      => 1 << 5;

# the rest of these are not supported in this module
use constant COMPRESSION_SHRUNK    => 1;    # file is Shrunk
use constant COMPRESSION_REDUCED_1 => 2;    # file is Reduced CF=1
use constant COMPRESSION_REDUCED_2 => 3;    # file is Reduced CF=2
use constant COMPRESSION_REDUCED_3 => 4;    # file is Reduced CF=3
use constant COMPRESSION_REDUCED_4 => 5;    # file is Reduced CF=4
use constant COMPRESSION_IMPLODED  => 6;    # file is Imploded
use constant COMPRESSION_TOKENIZED => 7;    # reserved for Tokenizing compr.
use constant COMPRESSION_DEFLATED_ENHANCED => 9;   # reserved for enh. Deflating
use constant COMPRESSION_PKWARE_DATA_COMPRESSION_LIBRARY_IMPLODED => 10;

# Load the various required classes
require Archive::Zip::Archive;
require Archive::Zip::Member;
require Archive::Zip::FileMember;
require Archive::Zip::DirectoryMember;
require Archive::Zip::ZipFileMember;
require Archive::Zip::NewFileMember;
require Archive::Zip::StringMember;

# Convenience functions

sub _ISA ($$) {

    # Can't rely on Scalar::Util, so use the next best way
    local $@;
    !!eval { ref $_[0] and $_[0]->isa($_[1]) };
}

sub _CAN ($$) {
    local $@;
    !!eval { ref $_[0] and $_[0]->can($_[1]) };
}

#####################################################################
# Methods

sub new {
    my $class = shift;
    return Archive::Zip::Archive->new(@_);
}

sub computeCRC32 {
    my ($data, $crc);

    if (ref($_[0]) eq 'HASH') {
        $data = $_[0]->{string};
        $crc  = $_[0]->{checksum};
    } else {
        $data = shift;
        $data = shift if ref($data);
        $crc  = shift;
    }

    return Compress::Raw::Zlib::crc32($data, $crc);
}

# Report or change chunk size used for reading and writing.
# Also sets Zlib's default buffer size (eventually).
sub setChunkSize {
    shift if ref($_[0]) eq 'Archive::Zip::Archive';
    my $chunkSize = (ref($_[0]) eq 'HASH') ? shift->{chunkSize} : shift;
    my $oldChunkSize = $Archive::Zip::ChunkSize;
    $Archive::Zip::ChunkSize = $chunkSize if ($chunkSize);
    return $oldChunkSize;
}

sub chunkSize {
    return $Archive::Zip::ChunkSize;
}

sub setErrorHandler {
    my $errorHandler = (ref($_[0]) eq 'HASH') ? shift->{subroutine} : shift;
    $errorHandler = \&Carp::carp unless defined($errorHandler);
    my $oldErrorHandler = $Archive::Zip::ErrorHandler;
    $Archive::Zip::ErrorHandler = $errorHandler;
    return $oldErrorHandler;
}

######################################################################
# Private utility functions (not methods).

sub _printError {
    my $string = join(' ', @_, "\n");
    my $oldCarpLevel = $Carp::CarpLevel;
    $Carp::CarpLevel += 2;
    &{$ErrorHandler}($string);
    $Carp::CarpLevel = $oldCarpLevel;
}

# This is called on format errors.
sub _formatError {
    shift if ref($_[0]);
    _printError('format error:', @_);
    return AZ_FORMAT_ERROR;
}

# This is called on IO errors.
sub _ioError {
    shift if ref($_[0]);
    _printError('IO error:', @_, ':', $!);
    return AZ_IO_ERROR;
}

# This is called on generic errors.
sub _error {
    shift if ref($_[0]);
    _printError('error:', @_);
    return AZ_ERROR;
}

# Called when a subclass should have implemented
# something but didn't
sub _subclassResponsibility {
    Carp::croak("subclass Responsibility\n");
}

# Try to set the given file handle or object into binary mode.
sub _binmode {
    my $fh = shift;
    return _CAN($fh, 'binmode') ? $fh->binmode() : binmode($fh);
}

# Attempt to guess whether file handle is seekable.
# Because of problems with Windows, this only returns true when
# the file handle is a real file.
sub _isSeekable {
    my $fh = shift;
    return 0 unless ref $fh;
    _ISA($fh, "IO::Scalar")    # IO::Scalar objects are brokenly-seekable
      and return 0;
    _ISA($fh, "IO::String")
      and return 1;
    if (_ISA($fh, "IO::Seekable")) {

        # Unfortunately, some things like FileHandle objects
        # return true for Seekable, but AREN'T!!!!!
        _ISA($fh, "FileHandle")
          and return 0;
        return 1;
    }

    # open my $fh, "+<", \$data;
    ref $fh eq "GLOB" && eval { seek $fh, 0, 1 } and return 1;
    _CAN($fh, "stat")
      and return -f $fh;
    return (_CAN($fh, "seek") and _CAN($fh, "tell")) ? 1 : 0;
}

# Print to the filehandle, while making sure the pesky Perl special global
# variables don't interfere.
sub _print {
    my ($self, $fh, @data) = @_;

    local $\;

    return $fh->print(@data);
}

# Return an opened IO::Handle
# my ( $status, fh ) = _newFileHandle( 'fileName', 'w' );
# Can take a filename, file handle, or ref to GLOB
# Or, if given something that is a ref but not an IO::Handle,
# passes back the same thing.
sub _newFileHandle {
    my $fd     = shift;
    my $status = 1;
    my $handle;

    if (ref($fd)) {
        if (_ISA($fd, 'IO::Scalar') or _ISA($fd, 'IO::String')) {
            $handle = $fd;
        } elsif (_ISA($fd, 'IO::Handle') or ref($fd) eq 'GLOB') {
            $handle = IO::File->new;
            $status = $handle->fdopen($fd, @_);
        } else {
            $handle = $fd;
        }
    } else {
        $handle = IO::File->new;
        $status = $handle->open($fd, @_);
    }

    return ($status, $handle);
}

# Returns next signature from given file handle, leaves
# file handle positioned afterwards.
# In list context, returns ($status, $signature)
# ( $status, $signature) = _readSignature( $fh, $fileName );

sub _readSignature {
    my $fh                = shift;
    my $fileName          = shift;
    my $expectedSignature = shift;    # optional

    my $signatureData;
    my $bytesRead = $fh->read($signatureData, SIGNATURE_LENGTH);
    if ($bytesRead != SIGNATURE_LENGTH) {
        return _ioError("reading header signature");
    }
    my $signature = unpack(SIGNATURE_FORMAT, $signatureData);
    my $status = AZ_OK;

    # compare with expected signature, if any, or any known signature.
    if (
        (defined($expectedSignature) && $signature != $expectedSignature)
        || (   !defined($expectedSignature)
            && $signature != CENTRAL_DIRECTORY_FILE_HEADER_SIGNATURE
            && $signature != LOCAL_FILE_HEADER_SIGNATURE
            && $signature != END_OF_CENTRAL_DIRECTORY_SIGNATURE
            && $signature != DATA_DESCRIPTOR_SIGNATURE)
      ) {
        my $errmsg = sprintf("bad signature: 0x%08x", $signature);
        if (_isSeekable($fh)) {
            $errmsg .= sprintf(" at offset %d", $fh->tell() - SIGNATURE_LENGTH);
        }

        $status = _formatError("$errmsg in file $fileName");
    }

    return ($status, $signature);
}

# Utility method to make and open a temp file.
# Will create $temp_dir if it does not exist.
# Returns file handle and name:
#
# my ($fh, $name) = Archive::Zip::tempFile();
# my ($fh, $name) = Archive::Zip::tempFile('mytempdir');
#

sub tempFile {
    my $dir = (ref($_[0]) eq 'HASH') ? shift->{tempDir} : shift;
    my ($fh, $filename) = File::Temp::tempfile(
        SUFFIX => '.zip',
        UNLINK => 1,
        $dir ? (DIR => $dir) : ());
    return (undef, undef) unless $fh;
    my ($status, $newfh) = _newFileHandle($fh, 'w+');
    return ($newfh, $filename);
}

# Return the normalized directory name as used in a zip file (path
# separators become slashes, etc.).
# Will translate internal slashes in path components (i.e. on Macs) to
# underscores.  Discards volume names.
# When $forceDir is set, returns paths with trailing slashes (or arrays
# with trailing blank members).
#
# If third argument is a reference, returns volume information there.
#
# input         output
# .             ('.')   '.'
# ./a           ('a')   a
# ./a/b         ('a','b')   a/b
# ./a/b/        ('a','b')   a/b
# a/b/          ('a','b')   a/b
# /a/b/         ('','a','b')    a/b
# c:\a\b\c.doc  ('','a','b','c.doc')    a/b/c.doc      # on Windows
# "i/o maps:whatever"   ('i_o maps', 'whatever')  "i_o maps/whatever"   # on Macs
sub _asZipDirName {
    my $name      = shift;
    my $forceDir  = shift;
    my $volReturn = shift;
    my ($volume, $directories, $file) =
      File::Spec->splitpath(File::Spec->canonpath($name), $forceDir);
    $$volReturn = $volume if (ref($volReturn));
    my @dirs = map { $_ =~ s{/}{_}g; $_ } File::Spec->splitdir($directories);
    if (@dirs > 0) { pop(@dirs) unless $dirs[-1] }    # remove empty component
    push(@dirs, defined($file) ? $file : '');

    #return wantarray ? @dirs : join ( '/', @dirs );

    my $normalised_path = join '/', @dirs;

    # Leading directory separators should not be stored in zip archives.
    # Example:
    #   C:\a\b\c\      a/b/c
    #   C:\a\b\c.txt   a/b/c.txt
    #   /a/b/c/        a/b/c
    #   /a/b/c.txt     a/b/c.txt
    $normalised_path =~ s{^/}{};    # remove leading separator

    return $normalised_path;
}

# Return an absolute local name for a zip name.
# Assume a directory if zip name has trailing slash.
# Takes an optional volume name in FS format (like 'a:').
#
sub _asLocalName {
    my $name   = shift;    # zip format
    my $volume = shift;
    $volume = '' unless defined($volume);    # local FS format

    my @paths = split(/\//, $name);
    my $filename = pop(@paths);
    $filename = '' unless defined($filename);
    my $localDirs = @paths ? File::Spec->catdir(@paths) : '';
    my $localName = File::Spec->catpath($volume, $localDirs, $filename);
    unless ($volume) {
        $localName = File::Spec->rel2abs($localName, Cwd::getcwd());
    }
    return $localName;
}

1;

__END__

=pod

=encoding utf8

=head1 NAME

Archive::Zip - Provide an interface to ZIP archive files.

=head1 SYNOPSIS

   # Create a Zip file
   use Archive::Zip qw( :ERROR_CODES :CONSTANTS );
   my $zip = Archive::Zip->new();

   # Add a directory
   my $dir_member = $zip->addDirectory( 'dirname/' );

   # Add a file from a string with compression
   my $string_member = $zip->addString( 'This is a test', 'stringMember.txt' );
   $string_member->desiredCompressionMethod( COMPRESSION_DEFLATED );

   # Add a file from disk
   my $file_member = $zip->addFile( 'xyz.pl', 'AnotherName.pl' );

   # Save the Zip file
   unless ( $zip->writeToFileNamed('someZip.zip') == AZ_OK ) {
       die 'write error';
   }

   # Read a Zip file
   my $somezip = Archive::Zip->new();
   unless ( $somezip->read( 'someZip.zip' ) == AZ_OK ) {
       die 'read error';
   }

   # Change the compression type for a file in the Zip
   my $member = $somezip->memberNamed( 'stringMember.txt' );
   $member->desiredCompressionMethod( COMPRESSION_STORED );
   unless ( $zip->writeToFileNamed( 'someOtherZip.zip' ) == AZ_OK ) {
       die 'write error';
   }

=head1 DESCRIPTION

The Archive::Zip module allows a Perl program to create, manipulate, read,
and write Zip archive files.

Zip archives can be created, or you can read from existing zip files.

Once created, they can be written to files, streams, or strings. Members
can be added, removed, extracted, replaced, rearranged, and enumerated.
They can also be renamed or have their dates, comments, or other attributes
queried or modified. Their data can be compressed or uncompressed as needed.

Members can be created from members in existing Zip files, or from existing
directories, files, or strings.

This module uses the L<Compress::Raw::Zlib> library to read and write the
compressed streams inside the files.

One can use L<Archive::Zip::MemberRead> to read the zip file archive members
as if they were files.

=head2 File Naming

Regardless of what your local file system uses for file naming, names in a
Zip file are in Unix format (I<forward> slashes (/) separating directory
names, etc.).

C<Archive::Zip> tries to be consistent with file naming conventions, and will
translate back and forth between native and Zip file names.

However, it can't guess which format names are in. So two rules control what
kind of file name you must pass various routines:

=over 4

=item Names of files are in local format.

C<File::Spec> and C<File::Basename> are used for various file
operations. When you're referring to a file on your system, use its
file naming conventions.

=item Names of archive members are in Unix format.

This applies to every method that refers to an archive member, or
provides a name for new archive members. The C<extract()> methods
that can take one or two names will convert from local to zip names
if you call them with a single name.

=back

=head2 Archive::Zip Object Model

=head3 Overview

Archive::Zip::Archive objects are what you ordinarily deal with.
These maintain the structure of a zip file, without necessarily
holding data. When a zip is read from a disk file, the (possibly
compressed) data still lives in the file, not in memory. Archive
members hold information about the individual members, but not
(usually) the actual member data. When the zip is written to a
(different) file, the member data is compressed or copied as needed.
It is possible to make archive members whose data is held in a string
in memory, but this is not done when a zip file is read. Directory
members don't have any data.

=head2 Inheritance

  Exporter
   Archive::Zip                            Common base class, has defs.
       Archive::Zip::Archive               A Zip archive.
       Archive::Zip::Member                Abstract superclass for all members.
           Archive::Zip::StringMember      Member made from a string
           Archive::Zip::FileMember        Member made from an external file
               Archive::Zip::ZipFileMember Member that lives in a zip file
               Archive::Zip::NewFileMember Member whose data is in a file
           Archive::Zip::DirectoryMember   Member that is a directory

=head1 EXPORTS

=over 4

=item :CONSTANTS

Exports the following constants:

FA_MSDOS FA_UNIX GPBF_ENCRYPTED_MASK
GPBF_DEFLATING_COMPRESSION_MASK GPBF_HAS_DATA_DESCRIPTOR_MASK
COMPRESSION_STORED COMPRESSION_DEFLATED IFA_TEXT_FILE_MASK
IFA_TEXT_FILE IFA_BINARY_FILE COMPRESSION_LEVEL_NONE
COMPRESSION_LEVEL_DEFAULT COMPRESSION_LEVEL_FASTEST
COMPRESSION_LEVEL_BEST_COMPRESSION

=item :MISC_CONSTANTS

Exports the following constants (only necessary for extending the
module):

FA_AMIGA FA_VAX_VMS FA_VM_CMS FA_ATARI_ST FA_OS2_HPFS
FA_MACINTOSH FA_Z_SYSTEM FA_CPM FA_WINDOWS_NTFS
GPBF_IMPLODING_8K_SLIDING_DICTIONARY_MASK
GPBF_IMPLODING_3_SHANNON_FANO_TREES_MASK
GPBF_IS_COMPRESSED_PATCHED_DATA_MASK COMPRESSION_SHRUNK
DEFLATING_COMPRESSION_NORMAL DEFLATING_COMPRESSION_MAXIMUM
DEFLATING_COMPRESSION_FAST DEFLATING_COMPRESSION_SUPER_FAST
COMPRESSION_REDUCED_1 COMPRESSION_REDUCED_2 COMPRESSION_REDUCED_3
COMPRESSION_REDUCED_4 COMPRESSION_IMPLODED COMPRESSION_TOKENIZED
COMPRESSION_DEFLATED_ENHANCED
COMPRESSION_PKWARE_DATA_COMPRESSION_LIBRARY_IMPLODED

=item :ERROR_CODES

Explained below. Returned from most methods.

AZ_OK AZ_STREAM_END AZ_ERROR AZ_FORMAT_ERROR AZ_IO_ERROR

=back

=head1 ERROR CODES

Many of the methods in Archive::Zip return error codes. These are implemented
as inline subroutines, using the C<use constant> pragma. They can be imported
into your namespace using the C<:ERROR_CODES> tag:

  use Archive::Zip qw( :ERROR_CODES );

  ...

  unless ( $zip->read( 'myfile.zip' ) == AZ_OK ) {
      die "whoops!";
  }

=over 4

=item AZ_OK (0)

Everything is fine.

=item AZ_STREAM_END (1)

The read stream (or central directory) ended normally.

=item AZ_ERROR (2)

There was some generic kind of error.

=item AZ_FORMAT_ERROR (3)

There is a format error in a ZIP file being read.

=item AZ_IO_ERROR (4)

There was an IO error.

=back

=head2 Compression

Archive::Zip allows each member of a ZIP file to be compressed (using the
Deflate algorithm) or uncompressed.

Other compression algorithms that some versions of ZIP have been able to
produce are not supported. Each member has two compression methods: the
one it's stored as (this is always COMPRESSION_STORED for string and external
file members), and the one you desire for the member in the zip file.

These can be different, of course, so you can make a zip member that is not
compressed out of one that is, and vice versa.

You can inquire about the current compression and set the desired
compression method:

  my $member = $zip->memberNamed( 'xyz.txt' );
  $member->compressionMethod();    # return current compression

  # set to read uncompressed
  $member->desiredCompressionMethod( COMPRESSION_STORED );

  # set to read compressed
  $member->desiredCompressionMethod( COMPRESSION_DEFLATED );

There are two different compression methods:

=over 4

=item COMPRESSION_STORED

File is stored (no compression)

=item COMPRESSION_DEFLATED

File is Deflated

=back

=head2 Compression Levels

If a member's desiredCompressionMethod is COMPRESSION_DEFLATED, you
can choose different compression levels. This choice may affect the
speed of compression and decompression, as well as the size of the
compressed member data.

  $member->desiredCompressionLevel( 9 );

The levels given can be:

=over 4

=item * 0 or COMPRESSION_LEVEL_NONE

This is the same as saying

  $member->desiredCompressionMethod( COMPRESSION_STORED );

=item * 1 .. 9

1 gives the best speed and worst compression, and 9 gives the
best compression and worst speed.

=item * COMPRESSION_LEVEL_FASTEST

This is a synonym for level 1.

=item * COMPRESSION_LEVEL_BEST_COMPRESSION

This is a synonym for level 9.

=item * COMPRESSION_LEVEL_DEFAULT

This gives a good compromise between speed and compression,
and is currently equivalent to 6 (this is in the zlib code).
This is the level that will be used if not specified.

=back

=head1 Archive::Zip Methods

The Archive::Zip class (and its invisible subclass Archive::Zip::Archive)
implement generic zip file functionality. Creating a new Archive::Zip object
actually makes an Archive::Zip::Archive object, but you don't have to worry
about this unless you're subclassing.

=head2 Constructor

=over 4

=item new( [$fileName] )

=item new( { filename => $fileName } )

Make a new, empty zip archive.

    my $zip = Archive::Zip->new();

If an additional argument is passed, new() will call read()
to read the contents of an archive:

    my $zip = Archive::Zip->new( 'xyz.zip' );

If a filename argument is passed and the read fails for any
reason, new will return undef. For this reason, it may be
better to call read separately.

=back

=head2 Zip Archive Utility Methods

These Archive::Zip methods may be called as functions or as object
methods. Do not call them as class methods:

    $zip = Archive::Zip->new();
    $crc = Archive::Zip::computeCRC32( 'ghijkl' );    # OK
    $crc = $zip->computeCRC32( 'ghijkl' );            # also OK
    $crc = Archive::Zip->computeCRC32( 'ghijkl' );    # NOT OK

=over 4

=item Archive::Zip::computeCRC32( $string [, $crc] )

=item Archive::Zip::computeCRC32( { string => $string [, checksum => $crc ] } )

This is a utility function that uses the Compress::Raw::Zlib CRC
routine to compute a CRC-32. You can get the CRC of a string:

    $crc = Archive::Zip::computeCRC32( $string );

Or you can compute the running CRC:

    $crc = 0;
    $crc = Archive::Zip::computeCRC32( 'abcdef', $crc );
    $crc = Archive::Zip::computeCRC32( 'ghijkl', $crc );

=item Archive::Zip::setChunkSize( $number )

=item Archive::Zip::setChunkSize( { chunkSize => $number } )

Report or change chunk size used for reading and writing.
This can make big differences in dealing with large files.
Currently, this defaults to 32K. This also changes the chunk
size used for Compress::Raw::Zlib. You must call setChunkSize()
before reading or writing. This is not exportable, so you
must call it like:

    Archive::Zip::setChunkSize( 4096 );

or as a method on a zip (though this is a global setting).
Returns old chunk size.

=item Archive::Zip::chunkSize()

Returns the current chunk size:

    my $chunkSize = Archive::Zip::chunkSize();

=item Archive::Zip::setErrorHandler( \&subroutine )

=item Archive::Zip::setErrorHandler( { subroutine => \&subroutine } )

Change the subroutine called with error strings. This
defaults to \&Carp::carp, but you may want to change it to
get the error strings. This is not exportable, so you must
call it like:

    Archive::Zip::setErrorHandler( \&myErrorHandler );

If myErrorHandler is undef, resets handler to default.
Returns old error handler. Note that if you call Carp::carp
or a similar routine or if you're chaining to the default
error handler from your error handler, you may want to
increment the number of caller levels that are skipped (do
not just set it to a number):

    $Carp::CarpLevel++;

=item Archive::Zip::tempFile( [ $tmpdir ] )

=item Archive::Zip::tempFile( { tempDir => $tmpdir } )

Create a uniquely named temp file. It will be returned open
for read/write. If C<$tmpdir> is given, it is used as the
name of a directory to create the file in. If not given,
creates the file using C<File::Spec::tmpdir()>. Generally, you can
override this choice using the

    $ENV{TMPDIR}

environment variable. But see the L<File::Spec|File::Spec>
documentation for your system. Note that on many systems, if you're
running in taint mode, then you must make sure that C<$ENV{TMPDIR}> is
untainted for it to be used.
Will I<NOT> create C<$tmpdir> if it does not exist (this is a change
from prior versions!). Returns file handle and name:

    my ($fh, $name) = Archive::Zip::tempFile();
    my ($fh, $name) = Archive::Zip::tempFile('myTempDir');
    my $fh = Archive::Zip::tempFile();  # if you don't need the name

=back

=head2 Zip Archive Accessors

=over 4

=item members()

Return a copy of the members array

    my @members = $zip->members();

=item numberOfMembers()

Return the number of members I have

=item memberNames()

Return a list of the (internal) file names of the zip members

=item memberNamed( $string )

=item memberNamed( { zipName => $string } )

Return ref to member whose filename equals given filename or
undef. C<$string> must be in Zip (Unix) filename format.

=item membersMatching( $regex )

=item membersMatching( { regex => $regex } )

Return array of members whose filenames match given regular
expression in list context. Returns number of matching
members in scalar context.

    my @textFileMembers = $zip->membersMatching( '.*\.txt' );
    # or
    my $numberOfTextFiles = $zip->membersMatching( '.*\.txt' );

=item diskNumber()

Return the disk that I start on. Not used for writing zips,
but might be interesting if you read a zip in. This should be
0, as Archive::Zip does not handle multi-volume archives.

=item diskNumberWithStartOfCentralDirectory()

Return the disk number that holds the beginning of the
central directory. Not used for writing zips, but might be
interesting if you read a zip in. This should be 0, as
Archive::Zip does not handle multi-volume archives.

=item numberOfCentralDirectoriesOnThisDisk()

Return the number of CD structures in the zipfile last read in.
Not used for writing zips, but might be interesting if you read a zip
in.

=item numberOfCentralDirectories()

Return the number of CD structures in the zipfile last read in.
Not used for writing zips, but might be interesting if you read a zip
in.

=item centralDirectorySize()

Returns central directory size, as read from an external zip
file. Not used for writing zips, but might be interesting if
you read a zip in.

=item centralDirectoryOffsetWRTStartingDiskNumber()

Returns the offset into the zip file where the CD begins. Not
used for writing zips, but might be interesting if you read a
zip in.

=item zipfileComment( [ $string ] )

=item zipfileComment( [ { comment => $string } ] )

Get or set the zipfile comment. Returns the old comment.

    print $zip->zipfileComment();
    $zip->zipfileComment( 'New Comment' );

=item eocdOffset()

Returns the (unexpected) number of bytes between where the
EOCD was found and where it expected to be. This is normally
0, but would be positive if something (a virus, perhaps) had
added bytes somewhere before the EOCD. Not used for writing
zips, but might be interesting if you read a zip in. Here is
an example of how you can diagnose this:

  my $zip = Archive::Zip->new('somefile.zip');
  if ($zip->eocdOffset())
  {
    warn "A virus has added ", $zip->eocdOffset, " bytes of garbage\n";
  }

The C<eocdOffset()> is used to adjust the starting position of member
headers, if necessary.

=item fileName()

Returns the name of the file last read from. If nothing has
been read yet, returns an empty string; if read from a file
handle, returns the handle in string form.

=back

=head2 Zip Archive Member Operations

Various operations on a zip file modify members. When a member is
passed as an argument, you can either use a reference to the member
itself, or the name of a member. Of course, using the name requires
that names be unique within a zip (this is not enforced).

=over 4

=item removeMember( $memberOrName )

=item removeMember( { memberOrZipName => $memberOrName } )

Remove and return the given member, or match its name and
remove it. Returns undef if member or name does not exist in this
Zip. No-op if member does not belong to this zip.

=item replaceMember( $memberOrName, $newMember )

=item replaceMember( { memberOrZipName => $memberOrName,
    newMember => $newMember } )

Remove and return the given member, or match its name and
remove it. Replace with new member. Returns undef if member or
name does not exist in this Zip, or if C<$newMember> is undefined.

It is an (undiagnosed) error to provide a C<$newMember> that is a
member of the zip being modified.

    my $member1 = $zip->removeMember( 'xyz' );
    my $member2 = $zip->replaceMember( 'abc', $member1 );
    # now, $member2 (named 'abc') is not in $zip,
    # and $member1 (named 'xyz') is, having taken $member2's place.

=item extractMember( $memberOrName [, $extractedName ] )

=item extractMember( { memberOrZipName => $memberOrName
    [, name => $extractedName ] } )

Extract the given member, or match its name and extract it.
Returns undef if member does not exist in this Zip. If
optional second arg is given, use it as the name of the
extracted member. Otherwise, the internal filename of the
member is used as the name of the extracted file or
directory.
If you pass C<$extractedName>, it should be in the local file
system's format.
All necessary directories will be created. Returns C<AZ_OK>
on success.

=item extractMemberWithoutPaths( $memberOrName [, $extractedName ] )

=item extractMemberWithoutPaths( { memberOrZipName => $memberOrName
    [, name => $extractedName ] } )

Extract the given member, or match its name and extract it.
Does not use path information (extracts into the current
directory). Returns undef if member does not exist in this
Zip.
If optional second arg is given, use it as the name of the
extracted member (its paths will be deleted too). Otherwise,
the internal filename of the member (minus paths) is used as
the name of the extracted file or directory. Returns C<AZ_OK>
on success.

=item addMember( $member )

=item addMember( { member => $member } )

Append a member (possibly from another zip file) to the zip
file. Returns the new member. Generally, you will use
addFile(), addDirectory(), addFileOrDirectory(), addString(),
or read() to add members.

    # Move member named 'abc' to end of zip:
    my $member = $zip->removeMember( 'abc' );
    $zip->addMember( $member );

=item updateMember( $memberOrName, $fileName )

=item updateMember( { memberOrZipName => $memberOrName, name => $fileName } )

Update a single member from the file or directory named C<$fileName>.
Returns the (possibly added or updated) member, if any; C<undef> on
errors.
The comparison is based on C<lastModTime()> and (in the case of a
non-directory) the size of the file.

=item addFile( $fileName [, $newName, $compressionLevel ] )

=item addFile( { filename => $fileName
    [, zipName => $newName, compressionLevel => $compressionLevel } ] )

Append a member whose data comes from an external file,
returning the member or undef. The member will have its file
name set to the name of the external file, and its
desiredCompressionMethod set to COMPRESSION_DEFLATED. The
file attributes and last modification time will be set from
the file.
If the name given does not represent a readable plain file or
symbolic link, undef will be returned. C<$fileName> must be
in the format required for the local file system.
The optional C<$newName> argument sets the internal file name
to something different than the given $fileName. C<$newName>,
if given, must be in Zip name format (i.e. Unix).
The text mode bit will be set if the contents appears to be
text (as returned by the C<-T> perl operator).


I<NOTE> that you should not (generally) use absolute path names
in zip member names, as this will cause problems with some zip
tools as well as introduce a security hole and make the zip
harder to use.

=item addDirectory( $directoryName [, $fileName ] )

=item addDirectory( { directoryName => $directoryName
    [, zipName => $fileName ] } )


Append a member created from the given directory name. The
directory name does not have to name an existing directory.
If the named directory exists, the file modification time and
permissions are set from the existing directory, otherwise
they are set to now and permissive default permissions.
C<$directoryName> must be in local file system format.
The optional second argument sets the name of the archive
member (which defaults to C<$directoryName>). If given, it
must be in Zip (Unix) format.
Returns the new member.

=item addFileOrDirectory( $name [, $newName, $compressionLevel ] )

=item addFileOrDirectory( { name => $name [, zipName => $newName,
    compressionLevel => $compressionLevel ] } )


Append a member from the file or directory named $name. If
$newName is given, use it for the name of the new member.
Will add or remove trailing slashes from $newName as needed.
C<$name> must be in local file system format.
The optional second argument sets the name of the archive
member (which defaults to C<$name>). If given, it must be in
Zip (Unix) format.

=item addString( $stringOrStringRef, $name, [$compressionLevel] )

=item addString( { string => $stringOrStringRef [, zipName => $name,
    compressionLevel => $compressionLevel ] } )

Append a member created from the given string or string
reference. The name is given by the second argument.
Returns the new member. The last modification time will be
set to now, and the file attributes will be set to permissive
defaults.

    my $member = $zip->addString( 'This is a test', 'test.txt' );

=item contents( $memberOrMemberName [, $newContents ] )

=item contents( { memberOrZipName => $memberOrMemberName
    [, contents => $newContents ] } )


Returns the uncompressed data for a particular member, or
undef.

    print "xyz.txt contains " . $zip->contents( 'xyz.txt' );

Also can change the contents of a member:

    $zip->contents( 'xyz.txt', 'This is the new contents' );

If called expecting an array as the return value, it will include
the status as the second value in the array.

    ($content, $status) = $zip->contents( 'xyz.txt');

=back

=head2 Zip Archive I/O operations


A Zip archive can be written to a file or file handle, or read from
one.

=over 4

=item writeToFileNamed( $fileName )

=item writeToFileNamed( { fileName => $fileName } )

Write a zip archive to named file. Returns C<AZ_OK> on
success.

    my $status = $zip->writeToFileNamed( 'xx.zip' );
    die "error somewhere" if $status != AZ_OK;

Note that if you use the same name as an existing zip file
that you read in, you will clobber ZipFileMembers. So
instead, write to a different file name, then delete the
original.
If you use the C<overwrite()> or C<overwriteAs()> methods, you can
re-write the original zip in this way.
C<$fileName> should be a valid file name on your system.

=item writeToFileHandle( $fileHandle [, $seekable] )

Write a zip archive to a file handle. Return AZ_OK on
success. The optional second arg tells whether or not to try
to seek backwards to re-write headers. If not provided, it is
set if the Perl C<-f> test returns true. This could fail on
some operating systems, though.

    my $fh = IO::File->new( 'someFile.zip', 'w' );
    unless ( $zip->writeToFileHandle( $fh ) == AZ_OK ) {
        # error handling
    }

If you pass a file handle that is not seekable (like if
you're writing to a pipe or a socket), pass a false second
argument:

    my $fh = IO::File->new( '| cat > somefile.zip', 'w' );
    $zip->writeToFileHandle( $fh, 0 );   # fh is not seekable

If this method fails during the write of a member, that
member and all following it will return false from
C<wasWritten()>. See writeCentralDirectory() for a way to
deal with this.
If you want, you can write data to the file handle before
passing it to writeToFileHandle(); this could be used (for
instance) for making self-extracting archives. However, this
only works reliably when writing to a real file (as opposed
to STDOUT or some other possible non-file).

See examples/selfex.pl for how to write a self-extracting
archive.

=item writeCentralDirectory( $fileHandle [, $offset ] )

=item writeCentralDirectory( { fileHandle => $fileHandle
    [, offset => $offset ] } )

Writes the central directory structure to the given file
handle.

Returns AZ_OK on success. If given an $offset, will
seek to that point before writing. This can be used for
recovery in cases where writeToFileHandle or writeToFileNamed
returns an IO error because of running out of space on the
destination file.

You can truncate the zip by seeking backwards and then writing the
directory:

    my $fh = IO::File->new( 'someFile.zip', 'w' );
        my $retval = $zip->writeToFileHandle( $fh );
    if ( $retval == AZ_IO_ERROR ) {
        my @unwritten = grep { not $_->wasWritten() } $zip->members();
        if (@unwritten) {
            $zip->removeMember( $member ) foreach my $member ( @unwritten );
            $zip->writeCentralDirectory( $fh,
            $unwritten[0]->writeLocalHeaderRelativeOffset());
        }
    }

=item overwriteAs( $newName )

=item overwriteAs( { filename => $newName } )

Write the zip to the specified file, as safely as possible.
This is done by first writing to a temp file, then renaming
the original if it exists, then renaming the temp file, then
deleting the renamed original if it exists. Returns AZ_OK if
successful.

=item overwrite()

Write back to the original zip file. See overwriteAs() above.
If the zip was not ever read from a file, this generates an
error.

=item read( $fileName )

=item read( { filename => $fileName } )

Read zipfile headers from a zip file, appending new members.
Returns C<AZ_OK> or error code.

    my $zipFile = Archive::Zip->new();
    my $status = $zipFile->read( '/some/FileName.zip' );

=item readFromFileHandle( $fileHandle, $filename )

=item readFromFileHandle( { fileHandle => $fileHandle, filename => $filename } )

Read zipfile headers from an already-opened file handle,
appending new members. Does not close the file handle.
Returns C<AZ_OK> or error code. Note that this requires a
seekable file handle; reading from a stream is not yet
supported, but using in-memory data is.

    my $fh = IO::File->new( '/some/FileName.zip', 'r' );
    my $zip1 = Archive::Zip->new();
    my $status = $zip1->readFromFileHandle( $fh );
    my $zip2 = Archive::Zip->new();
    $status = $zip2->readFromFileHandle( $fh );

Read zip using in-memory data (recursable):

    open my $fh, "<", "archive.zip" or die $!;
    my $zip_data = do { local $.; <$fh> };
    my $zip = Archive::Zip->new;
    open my $dh, "+<", \$zip_data;
    $zip->readFromFileHandle ($dh);

=back

=head2 Zip Archive Tree operations

These used to be in Archive::Zip::Tree but got moved into
Archive::Zip. They enable operation on an entire tree of members or
files.
A usage example:

  use Archive::Zip;
  my $zip = Archive::Zip->new();

  # add all readable files and directories below . as xyz/*
  $zip->addTree( '.', 'xyz' );

  # add all readable plain files below /abc as def/*
  $zip->addTree( '/abc', 'def', sub { -f && -r } );

  # add all .c files below /tmp as stuff/*
  $zip->addTreeMatching( '/tmp', 'stuff', '\.c$' );

  # add all .o files below /tmp as stuff/* if they aren't writable
  $zip->addTreeMatching( '/tmp', 'stuff', '\.o$', sub { ! -w } );

  # add all .so files below /tmp that are smaller than 200 bytes as stuff/*
  $zip->addTreeMatching( '/tmp', 'stuff', '\.o$', sub { -s < 200 } );

  # and write them into a file
  $zip->writeToFileNamed('xxx.zip');

  # now extract the same files into /tmpx
  $zip->extractTree( 'stuff', '/tmpx' );

=over 4

=item $zip->addTree( $root, $dest [, $pred, $compressionLevel ] ) -- Add tree of files to a zip

=item $zip->addTree( { root => $root, zipName => $dest [, select => $pred,
    compressionLevel => $compressionLevel ] )

C<$root> is the root of the tree of files and directories to be
added. It is a valid directory name on your system. C<$dest> is
the name for the root in the zip file (undef or blank means
to use relative pathnames). It is a valid ZIP directory name
(that is, it uses forward slashes (/) for separating
directory components). C<$pred> is an optional subroutine
reference to select files: it is passed the name of the
prospective file or directory using C<$_>, and if it returns
true, the file or directory will be included. The default is
to add all readable files and directories. For instance,
using

  my $pred = sub { /\.txt/ };
  $zip->addTree( '.', '', $pred );

will add all the .txt files in and below the current
directory, using relative names, and making the names
identical in the zipfile:

  original name           zip member name
  ./xyz                   xyz
  ./a/                    a/
  ./a/b                   a/b

To translate absolute to relative pathnames, just pass them
in: $zip->addTree( '/c/d', 'a' );

  original name           zip member name
  /c/d/xyz                a/xyz
  /c/d/a/                 a/a/
  /c/d/a/b                a/a/b

Returns AZ_OK on success. Note that this will not follow
symbolic links to directories. Note also that this does not
check for the validity of filenames.

Note that you generally I<don't> want to make zip archive member names
absolute.

=item $zip->addTreeMatching( $root, $dest, $pattern [, $pred, $compressionLevel ] )

=item $zip->addTreeMatching( { root => $root, zipName => $dest, pattern =>
    $pattern [, select => $pred, compressionLevel => $compressionLevel ] } )

$root is the root of the tree of files and directories to be
added $dest is the name for the root in the zip file (undef
means to use relative pathnames) $pattern is a (non-anchored)
regular expression for filenames to match $pred is an
optional subroutine reference to select files: it is passed
the name of the prospective file or directory in C<$_>, and
if it returns true, the file or directory will be included.
The default is to add all readable files and directories. To
add all files in and below the current directory whose names
end in C<.pl>, and make them extract into a subdirectory
named C<xyz>, do this:

  $zip->addTreeMatching( '.', 'xyz', '\.pl$' )

To add all I<writable> files in and below the directory named
C</abc> whose names end in C<.pl>, and make them extract into
a subdirectory named C<xyz>, do this:

  $zip->addTreeMatching( '/abc', 'xyz', '\.pl$', sub { -w } )

Returns AZ_OK on success. Note that this will not follow
symbolic links to directories.

=item $zip->updateTree( $root [, $dest , $pred , $mirror, $compressionLevel ] );

=item $zip->updateTree( { root => $root [, zipName => $dest, select => $pred,
    mirror => $mirror, compressionLevel => $compressionLevel ] } );

Update a zip file from a directory tree.

C<updateTree()> takes the same arguments as C<addTree()>, but first
checks to see whether the file or directory already exists in the zip
file, and whether it has been changed.

If the fourth argument C<$mirror> is true, then delete all my members
if corresponding files were not found.

Returns an error code or AZ_OK if all is well.

=item $zip->extractTree( [ $root, $dest, $volume } ] )

=item $zip->extractTree( [ { root => $root, zipName => $dest, volume => $volume } ] )

If you don't give any arguments at all, will extract all the
files in the zip with their original names.

If you supply one argument for C<$root>, C<extractTree> will extract
all the members whose names start with C<$root> into the current
directory, stripping off C<$root> first.
C<$root> is in Zip (Unix) format.
For instance,

  $zip->extractTree( 'a' );

when applied to a zip containing the files:
a/x a/b/c ax/d/e d/e will extract:

a/x as ./x

a/b/c as ./b/c

If you give two arguments, C<extractTree> extracts all the members
whose names start with C<$root>. It will translate C<$root> into
C<$dest> to construct the destination file name.
C<$root> and C<$dest> are in Zip (Unix) format.
For instance,

   $zip->extractTree( 'a', 'd/e' );

when applied to a zip containing the files:
a/x a/b/c ax/d/e d/e will extract:

a/x to d/e/x

a/b/c to d/e/b/c and ignore ax/d/e and d/e

If you give three arguments, C<extractTree> extracts all the members
whose names start with C<$root>. It will translate C<$root> into
C<$dest> to construct the destination file name, and then it will
convert to local file system format, using C<$volume> as the name of
the destination volume.

C<$root> and C<$dest> are in Zip (Unix) format.

C<$volume> is in local file system format.

For instance, under Windows,

   $zip->extractTree( 'a', 'd/e', 'f:' );

when applied to a zip containing the files:
a/x a/b/c ax/d/e d/e will extract:

a/x to f:d/e/x

a/b/c to f:d/e/b/c and ignore ax/d/e and d/e

If you want absolute paths (the prior example used paths relative to
the current directory on the destination volume, you can specify these
in C<$dest>:

   $zip->extractTree( 'a', '/d/e', 'f:' );

when applied to a zip containing the files:
a/x a/b/c ax/d/e d/e will extract:

a/x to f:\d\e\x

a/b/c to f:\d\e\b\c and ignore ax/d/e and d/e

Returns an error code or AZ_OK if everything worked OK.

=back

=head1 Archive::Zip Global Variables

=over 4

=item $Archive::Zip::UNICODE

This variable governs how Unicode file and directory names are added
to or extracted from an archive. If set, file and directory names are considered
to be UTF-8 encoded. This is I<EXPERIMENTAL AND BUGGY (there are some edge cases
on Win32)>. Please report problems.

    {
        local $Archive::Zip::UNICODE = 1;
        $zip->addFile('Déjà vu.txt');
    }

=back

=head1 MEMBER OPERATIONS

=head2 Member Class Methods

Several constructors allow you to construct members without adding
them to a zip archive. These work the same as the addFile(),
addDirectory(), and addString() zip instance methods described above,
but they don't add the new members to a zip.

=over 4

=item Archive::Zip::Member->newFromString( $stringOrStringRef [, $fileName ] )

=item Archive::Zip::Member->newFromString( { string => $stringOrStringRef
    [, zipName => $fileName ] )

Construct a new member from the given string. Returns undef
on error.

    my $member = Archive::Zip::Member->newFromString( 'This is a test',

=item newFromFile( $fileName [, $zipName ] )

=item newFromFile( { filename => $fileName [, zipName => $zipName ] } )

Construct a new member from the given file. Returns undef on
error.

    my $member = Archive::Zip::Member->newFromFile( 'xyz.txt' );

=item newDirectoryNamed( $directoryName [, $zipname ] )

=item newDirectoryNamed( { directoryName => $directoryName
    [, zipName => $zipname ] } )

Construct a new member from the given directory.
C<$directoryName> must be a valid name on your file system; it does not
have to exist.

If given, C<$zipname> will be the name of the zip member; it must be a
valid Zip (Unix) name. If not given, it will be converted from
C<$directoryName>.

Returns undef on error.

    my $member = Archive::Zip::Member->newDirectoryNamed( 'CVS/' );

=back

=head2 Member Simple accessors

These methods get (and/or set) member attribute values.

=over 4

=item versionMadeBy()

Gets the field from the member header.

=item fileAttributeFormat( [ $format ] )

=item fileAttributeFormat( [ { format => $format ] } )

Gets or sets the field from the member header. These are
C<FA_*> values.

=item versionNeededToExtract()

Gets the field from the member header.

=item bitFlag()

Gets the general purpose bit field from the member header.
This is where the C<GPBF_*> bits live.

=item compressionMethod()

Returns the member compression method. This is the method
that is currently being used to compress the member data.
This will be COMPRESSION_STORED for added string or file
members, or any of the C<COMPRESSION_*> values for members
from a zip file. However, this module can only handle members
whose data is in COMPRESSION_STORED or COMPRESSION_DEFLATED
format.

=item desiredCompressionMethod( [ $method ] )

=item desiredCompressionMethod( [ { compressionMethod => $method } ] )

Get or set the member's C<desiredCompressionMethod>. This is
the compression method that will be used when the member is
written. Returns prior desiredCompressionMethod. Only
COMPRESSION_DEFLATED or COMPRESSION_STORED are valid
arguments. Changing to COMPRESSION_STORED will change the
member desiredCompressionLevel to 0; changing to
COMPRESSION_DEFLATED will change the member
desiredCompressionLevel to COMPRESSION_LEVEL_DEFAULT.

=item desiredCompressionLevel( [ $level ] )

=item desiredCompressionLevel( [ { compressionLevel => $level } ] )

Get or set the member's desiredCompressionLevel This is the
method that will be used to write. Returns prior
desiredCompressionLevel. Valid arguments are 0 through 9,
COMPRESSION_LEVEL_NONE, COMPRESSION_LEVEL_DEFAULT,
COMPRESSION_LEVEL_BEST_COMPRESSION, and
COMPRESSION_LEVEL_FASTEST. 0 or COMPRESSION_LEVEL_NONE will
change the desiredCompressionMethod to COMPRESSION_STORED.
All other arguments will change the desiredCompressionMethod
to COMPRESSION_DEFLATED.

=item externalFileName()

Return the member's external file name, if any, or undef.

=item fileName()

Get or set the member's internal filename. Returns the
(possibly new) filename. Names will have backslashes
converted to forward slashes, and will have multiple
consecutive slashes converted to single ones.

=item lastModFileDateTime()

Return the member's last modification date/time stamp in
MS-DOS format.

=item lastModTime()

Return the member's last modification date/time stamp,
converted to unix localtime format.

    print "Mod Time: " . scalar( localtime( $member->lastModTime() ) );

=item setLastModFileDateTimeFromUnix()

Set the member's lastModFileDateTime from the given unix
time.

    $member->setLastModFileDateTimeFromUnix( time() );

=item internalFileAttributes()

Return the internal file attributes field from the zip
header. This is only set for members read from a zip file.

=item externalFileAttributes()

Return member attributes as read from the ZIP file. Note that
these are NOT UNIX!

=item unixFileAttributes( [ $newAttributes ] )

=item unixFileAttributes( [ { attributes => $newAttributes } ] )

Get or set the member's file attributes using UNIX file
attributes. Returns old attributes.

    my $oldAttribs = $member->unixFileAttributes( 0666 );

Note that the return value has more than just the file
permissions, so you will have to mask off the lowest bits for
comparisons.

=item localExtraField( [ $newField ] )

=item localExtraField( [ { field => $newField } ] )

Gets or sets the extra field that was read from the local
header. This is not set for a member from a zip file until
after the member has been written out. The extra field must
be in the proper format.

=item cdExtraField( [ $newField ] )

=item cdExtraField( [ { field => $newField } ] )

Gets or sets the extra field that was read from the central
directory header. The extra field must be in the proper
format.

=item extraFields()

Return both local and CD extra fields, concatenated.

=item fileComment( [ $newComment ] )

=item fileComment( [ { comment => $newComment } ] )

Get or set the member's file comment.

=item hasDataDescriptor()

Get or set the data descriptor flag. If this is set, the
local header will not necessarily have the correct data
sizes. Instead, a small structure will be stored at the end
of the member data with these values. This should be
transparent in normal operation.

=item crc32()

Return the CRC-32 value for this member. This will not be set
for members that were constructed from strings or external
files until after the member has been written.

=item crc32String()

Return the CRC-32 value for this member as an 8 character
printable hex string. This will not be set for members that
were constructed from strings or external files until after
the member has been written.

=item compressedSize()

Return the compressed size for this member. This will not be
set for members that were constructed from strings or
external files until after the member has been written.

=item uncompressedSize()

Return the uncompressed size for this member.

=item password( [ $password ] )

Returns the password for this member to be used on decryption.
If $password is given, it will set the password for the decryption.

=item isEncrypted()

Return true if this member is encrypted. The Archive::Zip
module does not currently support creation of encrypted
members. Decryption works more or less like this:

  my $zip = Archive::Zip->new;
  $zip->read ("encrypted.zip");
  for my $m (map { $zip->memberNamed ($_) } $zip->memberNames) {
      $m->password ("secret");
      $m->contents;  # is "" when password was wrong

That shows that the password has to be set per member, and not per
archive. This might change in the future.

=item isTextFile( [ $flag ] )

=item isTextFile( [ { flag => $flag } ] )

Returns true if I am a text file. Also can set the status if
given an argument (then returns old state). Note that this
module does not currently do anything with this flag upon
extraction or storage. That is, bytes are stored in native
format whether or not they came from a text file.

=item isBinaryFile()

Returns true if I am a binary file. Also can set the status
if given an argument (then returns old state). Note that this
module does not currently do anything with this flag upon
extraction or storage. That is, bytes are stored in native
format whether or not they came from a text file.

=item extractToFileNamed( $fileName )

=item extractToFileNamed( { name => $fileName } )

Extract me to a file with the given name. The file will be
created with default modes. Directories will be created as
needed.
The C<$fileName> argument should be a valid file name on your
file system.
Returns AZ_OK on success.

=item isDirectory()

Returns true if I am a directory.

=item writeLocalHeaderRelativeOffset()

Returns the file offset in bytes the last time I was written.

=item wasWritten()

Returns true if I was successfully written. Reset at the
beginning of a write attempt.

=back

=head2 Low-level member data reading

It is possible to use lower-level routines to access member data
streams, rather than the extract* methods and contents(). For
instance, here is how to print the uncompressed contents of a member
in chunks using these methods:

    my ( $member, $status, $bufferRef );
    $member = $zip->memberNamed( 'xyz.txt' );
    $member->desiredCompressionMethod( COMPRESSION_STORED );
    $status = $member->rewindData();
    die "error $status" unless $status == AZ_OK;
    while ( ! $member->readIsDone() )
    {
    ( $bufferRef, $status ) = $member->readChunk();
    die "error $status"
                if $status != AZ_OK && $status != AZ_STREAM_END;
    # do something with $bufferRef:
    print $$bufferRef;
    }
    $member->endRead();

=over 4

=item readChunk( [ $chunkSize ] )

=item readChunk( [ { chunkSize => $chunkSize } ] )

This reads the next chunk of given size from the member's
data stream and compresses or uncompresses it as necessary,
returning a reference to the bytes read and a status. If size
argument is not given, defaults to global set by
Archive::Zip::setChunkSize. Status is AZ_OK on success until
the last chunk, where it returns AZ_STREAM_END. Returns C<(
\$bytes, $status)>.

    my ( $outRef, $status ) = $self->readChunk();
    print $$outRef if $status != AZ_OK && $status != AZ_STREAM_END;

=item rewindData()

Rewind data and set up for reading data streams or writing
zip files. Can take options for C<inflateInit()> or
C<deflateInit()>, but this is not likely to be necessary.
Subclass overrides should call this method. Returns C<AZ_OK>
on success.

=item endRead()

Reset the read variables and free the inflater or deflater.
Must be called to close files, etc. Returns AZ_OK on success.

=item readIsDone()

Return true if the read has run out of data or encountered an error.

=item contents()

Return the entire uncompressed member data or undef in scalar
context. When called in array context, returns C<( $string,
$status )>; status will be AZ_OK on success:

    my $string = $member->contents();
    # or
    my ( $string, $status ) = $member->contents();
    die "error $status" unless $status == AZ_OK;

Can also be used to set the contents of a member (this may
change the class of the member):

    $member->contents( "this is my new contents" );

=item extractToFileHandle( $fh )

=item extractToFileHandle( { fileHandle => $fh } )

Extract (and uncompress, if necessary) the member's contents
to the given file handle. Return AZ_OK on success.

=back

=head1 Archive::Zip::FileMember methods

The Archive::Zip::FileMember class extends Archive::Zip::Member. It is the
base class for both ZipFileMember and NewFileMember classes. This class adds
an C<externalFileName> and an C<fh> member to keep track of the external
file.

=over 4

=item externalFileName()

Return the member's external filename.

=item fh()

Return the member's read file handle. Automatically opens file if
necessary.

=back

=head1 Archive::Zip::ZipFileMember methods

The Archive::Zip::ZipFileMember class represents members that have been read
from external zip files.

=over 4

=item diskNumberStart()

Returns the disk number that the member's local header resides in.
Should be 0.

=item localHeaderRelativeOffset()

Returns the offset into the zip file where the member's local header
is.

=item dataOffset()

Returns the offset from the beginning of the zip file to the member's
data.

=back

=head1 REQUIRED MODULES

L<Archive::Zip> requires several other modules:

L<Carp>

L<Compress::Raw::Zlib>

L<Cwd>

L<File::Basename>

L<File::Copy>

L<File::Find>

L<File::Path>

L<File::Spec>

L<IO::File>

L<IO::Seekable>

L<Time::Local>

=head1 BUGS AND CAVEATS

=head2 When not to use Archive::Zip

If you are just going to be extracting zips (and/or other archives) you
are recommended to look at using L<Archive::Extract> instead, as it is much
easier to use and factors out archive-specific functionality.

=head2 Try to avoid IO::Scalar

One of the most common ways to use Archive::Zip is to generate Zip files
in-memory. Most people use L<IO::Scalar> for this purpose.

Unfortunately, as of 1.11 this module no longer works with L<IO::Scalar>
as it incorrectly implements seeking.

Anybody using L<IO::Scalar> should consider porting to L<IO::String>,
which is smaller, lighter, and is implemented to be perfectly compatible
with regular seekable filehandles.

Support for L<IO::Scalar> most likely will B<not> be restored in the
future, as L<IO::Scalar> itself cannot change the way it is implemented
due to back-compatibility issues.

=head2 Wrong password for encrypted members

When an encrypted member is read using the wrong password, you currently
have to re-read the entire archive to try again with the correct password.

=head1 TO DO

* auto-choosing storing vs compression

* extra field hooks (see notes.txt)

* check for duplicates on addition/renaming?

* Text file extraction (line end translation)

* Reading zip files from non-seekable inputs
  (Perhaps by proxying through IO::String?)

* separate unused constants into separate module

* cookbook style docs

* Handle tainted paths correctly

* Work on better compatibility with other IO:: modules

* Support encryption

* More user-friendly decryption

=head1 SUPPORT

Bugs should be reported via the CPAN bug tracker

L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=Archive-Zip>

For other issues contact the maintainer

=head1 AUTHOR

Currently maintained by Fred Moyer <fred@redhotpenguin.com>

Previously maintained by Adam Kennedy <adamk@cpan.org>

Previously maintained by Steve Peters E<lt>steve@fisharerojo.orgE<gt>.

File attributes code by Maurice Aubrey E<lt>maurice@lovelyfilth.comE<gt>.

Originally by Ned Konz E<lt>nedkonz@cpan.orgE<gt>.

=head1 COPYRIGHT

Some parts copyright 2006 - 2012 Adam Kennedy.

Some parts copyright 2005 Steve Peters.

Original work copyright 2000 - 2004 Ned Konz.

This program is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=head1 SEE ALSO

Look at L<Archive::Zip::MemberRead> which is a wrapper that allows one to
read Zip archive members as if they were files.

L<Compress::Raw::Zlib>, L<Archive::Tar>, L<Archive::Extract>

=cut

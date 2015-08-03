package Archive::Zip::ZipFileMember;

use strict;
use vars qw( $VERSION @ISA );

BEGIN {
    $VERSION = '1.48';
    @ISA     = qw ( Archive::Zip::FileMember );
}

use Archive::Zip qw(
  :CONSTANTS
  :ERROR_CODES
  :PKZIP_CONSTANTS
  :UTILITY_METHODS
);

# Create a new Archive::Zip::ZipFileMember
# given a filename and optional open file handle
#
sub _newFromZipFile {
    my $class              = shift;
    my $fh                 = shift;
    my $externalFileName   = shift;
    my $possibleEocdOffset = shift;    # normally 0

    my $self = $class->new(
        'crc32'                     => 0,
        'diskNumberStart'           => 0,
        'localHeaderRelativeOffset' => 0,
        'dataOffset' => 0,    # localHeaderRelativeOffset + header length
        @_
    );
    $self->{'externalFileName'}   = $externalFileName;
    $self->{'fh'}                 = $fh;
    $self->{'possibleEocdOffset'} = $possibleEocdOffset;
    return $self;
}

sub isDirectory {
    my $self = shift;
    return (substr($self->fileName, -1, 1) eq '/'
          and $self->uncompressedSize == 0);
}

# Seek to the beginning of the local header, just past the signature.
# Verify that the local header signature is in fact correct.
# Update the localHeaderRelativeOffset if necessary by adding the possibleEocdOffset.
# Returns status.

sub _seekToLocalHeader {
    my $self          = shift;
    my $where         = shift;    # optional
    my $previousWhere = shift;    # optional

    $where = $self->localHeaderRelativeOffset() unless defined($where);

    # avoid loop on certain corrupt files (from Julian Field)
    return _formatError("corrupt zip file")
      if defined($previousWhere) && $where == $previousWhere;

    my $status;
    my $signature;

    $status = $self->fh()->seek($where, IO::Seekable::SEEK_SET);
    return _ioError("seeking to local header") unless $status;

    ($status, $signature) =
      _readSignature($self->fh(), $self->externalFileName(),
        LOCAL_FILE_HEADER_SIGNATURE);
    return $status if $status == AZ_IO_ERROR;

    # retry with EOCD offset if any was given.
    if ($status == AZ_FORMAT_ERROR && $self->{'possibleEocdOffset'}) {
        $status = $self->_seekToLocalHeader(
            $self->localHeaderRelativeOffset() + $self->{'possibleEocdOffset'},
            $where
        );
        if ($status == AZ_OK) {
            $self->{'localHeaderRelativeOffset'} +=
              $self->{'possibleEocdOffset'};
            $self->{'possibleEocdOffset'} = 0;
        }
    }

    return $status;
}

# Because I'm going to delete the file handle, read the local file
# header if the file handle is seekable. If it is not, I assume that
# I've already read the local header.
# Return ( $status, $self )

sub _become {
    my $self     = shift;
    my $newClass = shift;
    return $self if ref($self) eq $newClass;

    my $status = AZ_OK;

    if (_isSeekable($self->fh())) {
        my $here = $self->fh()->tell();
        $status = $self->_seekToLocalHeader();
        $status = $self->_readLocalFileHeader() if $status == AZ_OK;
        $self->fh()->seek($here, IO::Seekable::SEEK_SET);
        return $status unless $status == AZ_OK;
    }

    delete($self->{'eocdCrc32'});
    delete($self->{'diskNumberStart'});
    delete($self->{'localHeaderRelativeOffset'});
    delete($self->{'dataOffset'});

    return $self->SUPER::_become($newClass);
}

sub diskNumberStart {
    shift->{'diskNumberStart'};
}

sub localHeaderRelativeOffset {
    shift->{'localHeaderRelativeOffset'};
}

sub dataOffset {
    shift->{'dataOffset'};
}

# Skip local file header, updating only extra field stuff.
# Assumes that fh is positioned before signature.
sub _skipLocalFileHeader {
    my $self = shift;
    my $header;
    my $bytesRead = $self->fh()->read($header, LOCAL_FILE_HEADER_LENGTH);
    if ($bytesRead != LOCAL_FILE_HEADER_LENGTH) {
        return _ioError("reading local file header");
    }
    my $fileNameLength;
    my $extraFieldLength;
    my $bitFlag;
    (
        undef,    # $self->{'versionNeededToExtract'},
        $bitFlag,
        undef,    # $self->{'compressionMethod'},
        undef,    # $self->{'lastModFileDateTime'},
        undef,    # $crc32,
        undef,    # $compressedSize,
        undef,    # $uncompressedSize,
        $fileNameLength,
        $extraFieldLength
    ) = unpack(LOCAL_FILE_HEADER_FORMAT, $header);

    if ($fileNameLength) {
        $self->fh()->seek($fileNameLength, IO::Seekable::SEEK_CUR)
          or return _ioError("skipping local file name");
    }

    if ($extraFieldLength) {
        $bytesRead =
          $self->fh()->read($self->{'localExtraField'}, $extraFieldLength);
        if ($bytesRead != $extraFieldLength) {
            return _ioError("reading local extra field");
        }
    }

    $self->{'dataOffset'} = $self->fh()->tell();

    if ($bitFlag & GPBF_HAS_DATA_DESCRIPTOR_MASK) {

        # Read the crc32, compressedSize, and uncompressedSize from the
        # extended data descriptor, which directly follows the compressed data.
        #
        # Skip over the compressed file data (assumes that EOCD compressedSize
        # was correct)
        $self->fh()->seek($self->{'compressedSize'}, IO::Seekable::SEEK_CUR)
          or return _ioError("seeking to extended local header");

        # these values should be set correctly from before.
        my $oldCrc32            = $self->{'eocdCrc32'};
        my $oldCompressedSize   = $self->{'compressedSize'};
        my $oldUncompressedSize = $self->{'uncompressedSize'};

        my $status = $self->_readDataDescriptor();
        return $status unless $status == AZ_OK;

        # The buffer withe encrypted data is prefixed with a new
        # encrypted 12 byte header. The size only changes when
        # the buffer is also compressed
        $self->isEncrypted && $oldUncompressedSize > $self->{uncompressedSize}
          and $oldUncompressedSize -= DATA_DESCRIPTOR_LENGTH;

        return _formatError(
            "CRC or size mismatch while skipping data descriptor")
          if ( $oldCrc32 != $self->{'crc32'}
            || $oldUncompressedSize != $self->{'uncompressedSize'});

        $self->{'crc32'} = 0 
            if $self->compressionMethod() == COMPRESSION_STORED ; 
    }

    return AZ_OK;
}

# Read from a local file header into myself. Returns AZ_OK if successful.
# Assumes that fh is positioned after signature.
# Note that crc32, compressedSize, and uncompressedSize will be 0 if
# GPBF_HAS_DATA_DESCRIPTOR_MASK is set in the bitFlag.

sub _readLocalFileHeader {
    my $self = shift;
    my $header;
    my $bytesRead = $self->fh()->read($header, LOCAL_FILE_HEADER_LENGTH);
    if ($bytesRead != LOCAL_FILE_HEADER_LENGTH) {
        return _ioError("reading local file header");
    }
    my $fileNameLength;
    my $crc32;
    my $compressedSize;
    my $uncompressedSize;
    my $extraFieldLength;
    (
        $self->{'versionNeededToExtract'}, $self->{'bitFlag'},
        $self->{'compressionMethod'},      $self->{'lastModFileDateTime'},
        $crc32,                            $compressedSize,
        $uncompressedSize,                 $fileNameLength,
        $extraFieldLength
    ) = unpack(LOCAL_FILE_HEADER_FORMAT, $header);

    if ($fileNameLength) {
        my $fileName;
        $bytesRead = $self->fh()->read($fileName, $fileNameLength);
        if ($bytesRead != $fileNameLength) {
            return _ioError("reading local file name");
        }
        $self->fileName($fileName);
    }

    if ($extraFieldLength) {
        $bytesRead =
          $self->fh()->read($self->{'localExtraField'}, $extraFieldLength);
        if ($bytesRead != $extraFieldLength) {
            return _ioError("reading local extra field");
        }
    }

    $self->{'dataOffset'} = $self->fh()->tell();

    if ($self->hasDataDescriptor()) {

        # Read the crc32, compressedSize, and uncompressedSize from the
        # extended data descriptor.
        # Skip over the compressed file data (assumes that EOCD compressedSize
        # was correct)
        $self->fh()->seek($self->{'compressedSize'}, IO::Seekable::SEEK_CUR)
          or return _ioError("seeking to extended local header");

        my $status = $self->_readDataDescriptor();
        return $status unless $status == AZ_OK;
    } else {
        return _formatError(
            "CRC or size mismatch after reading data descriptor")
          if ( $self->{'crc32'} != $crc32
            || $self->{'uncompressedSize'} != $uncompressedSize);
    }

    return AZ_OK;
}

# This will read the data descriptor, which is after the end of compressed file
# data in members that have GPBF_HAS_DATA_DESCRIPTOR_MASK set in their bitFlag.
# The only reliable way to find these is to rely on the EOCD compressedSize.
# Assumes that file is positioned immediately after the compressed data.
# Returns status; sets crc32, compressedSize, and uncompressedSize.
sub _readDataDescriptor {
    my $self = shift;
    my $signatureData;
    my $header;
    my $crc32;
    my $compressedSize;
    my $uncompressedSize;

    my $bytesRead = $self->fh()->read($signatureData, SIGNATURE_LENGTH);
    return _ioError("reading header signature")
      if $bytesRead != SIGNATURE_LENGTH;
    my $signature = unpack(SIGNATURE_FORMAT, $signatureData);

    # unfortunately, the signature appears to be optional.
    if ($signature == DATA_DESCRIPTOR_SIGNATURE
        && ($signature != $self->{'crc32'})) {
        $bytesRead = $self->fh()->read($header, DATA_DESCRIPTOR_LENGTH);
        return _ioError("reading data descriptor")
          if $bytesRead != DATA_DESCRIPTOR_LENGTH;

        ($crc32, $compressedSize, $uncompressedSize) =
          unpack(DATA_DESCRIPTOR_FORMAT, $header);
    } else {
        $bytesRead = $self->fh()->read($header, DATA_DESCRIPTOR_LENGTH_NO_SIG);
        return _ioError("reading data descriptor")
          if $bytesRead != DATA_DESCRIPTOR_LENGTH_NO_SIG;

        $crc32 = $signature;
        ($compressedSize, $uncompressedSize) =
          unpack(DATA_DESCRIPTOR_FORMAT_NO_SIG, $header);
    }

    $self->{'eocdCrc32'} = $self->{'crc32'}
      unless defined($self->{'eocdCrc32'});
    $self->{'crc32'}            = $crc32;
    $self->{'compressedSize'}   = $compressedSize;
    $self->{'uncompressedSize'} = $uncompressedSize;

    return AZ_OK;
}

# Read a Central Directory header. Return AZ_OK on success.
# Assumes that fh is positioned right after the signature.

sub _readCentralDirectoryFileHeader {
    my $self      = shift;
    my $fh        = $self->fh();
    my $header    = '';
    my $bytesRead = $fh->read($header, CENTRAL_DIRECTORY_FILE_HEADER_LENGTH);
    if ($bytesRead != CENTRAL_DIRECTORY_FILE_HEADER_LENGTH) {
        return _ioError("reading central dir header");
    }
    my ($fileNameLength, $extraFieldLength, $fileCommentLength);
    (
        $self->{'versionMadeBy'},
        $self->{'fileAttributeFormat'},
        $self->{'versionNeededToExtract'},
        $self->{'bitFlag'},
        $self->{'compressionMethod'},
        $self->{'lastModFileDateTime'},
        $self->{'crc32'},
        $self->{'compressedSize'},
        $self->{'uncompressedSize'},
        $fileNameLength,
        $extraFieldLength,
        $fileCommentLength,
        $self->{'diskNumberStart'},
        $self->{'internalFileAttributes'},
        $self->{'externalFileAttributes'},
        $self->{'localHeaderRelativeOffset'}
    ) = unpack(CENTRAL_DIRECTORY_FILE_HEADER_FORMAT, $header);

    $self->{'eocdCrc32'} = $self->{'crc32'};

    if ($fileNameLength) {
        $bytesRead = $fh->read($self->{'fileName'}, $fileNameLength);
        if ($bytesRead != $fileNameLength) {
            _ioError("reading central dir filename");
        }
    }
    if ($extraFieldLength) {
        $bytesRead = $fh->read($self->{'cdExtraField'}, $extraFieldLength);
        if ($bytesRead != $extraFieldLength) {
            return _ioError("reading central dir extra field");
        }
    }
    if ($fileCommentLength) {
        $bytesRead = $fh->read($self->{'fileComment'}, $fileCommentLength);
        if ($bytesRead != $fileCommentLength) {
            return _ioError("reading central dir file comment");
        }
    }

    # NK 10/21/04: added to avoid problems with manipulated headers
    if (    $self->{'uncompressedSize'} != $self->{'compressedSize'}
        and $self->{'compressionMethod'} == COMPRESSION_STORED) {
        $self->{'uncompressedSize'} = $self->{'compressedSize'};
    }

    $self->desiredCompressionMethod($self->compressionMethod());

    return AZ_OK;
}

sub rewindData {
    my $self = shift;

    my $status = $self->SUPER::rewindData(@_);
    return $status unless $status == AZ_OK;

    return AZ_IO_ERROR unless $self->fh();

    $self->fh()->clearerr();

    # Seek to local file header.
    # The only reason that I'm doing this this way is that the extraField
    # length seems to be different between the CD header and the LF header.
    $status = $self->_seekToLocalHeader();
    return $status unless $status == AZ_OK;

    # skip local file header
    $status = $self->_skipLocalFileHeader();
    return $status unless $status == AZ_OK;

    # Seek to beginning of file data
    $self->fh()->seek($self->dataOffset(), IO::Seekable::SEEK_SET)
      or return _ioError("seeking to beginning of file data");

    return AZ_OK;
}

# Return bytes read. Note that first parameter is a ref to a buffer.
# my $data;
# my ( $bytesRead, $status) = $self->readRawChunk( \$data, $chunkSize );
sub _readRawChunk {
    my ($self, $dataRef, $chunkSize) = @_;
    return (0, AZ_OK) unless $chunkSize;
    my $bytesRead = $self->fh()->read($$dataRef, $chunkSize)
      or return (0, _ioError("reading data"));
    return ($bytesRead, AZ_OK);
}

1;

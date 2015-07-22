package Archive::Zip::NewFileMember;

use strict;
use vars qw( $VERSION @ISA );

BEGIN {
    $VERSION = '1.48';
    @ISA     = qw ( Archive::Zip::FileMember );
}

use Archive::Zip qw(
  :CONSTANTS
  :ERROR_CODES
  :UTILITY_METHODS
);

# Given a file name, set up for eventual writing.
sub _newFromFileNamed {
    my $class    = shift;
    my $fileName = shift;    # local FS format
    my $newName  = shift;
    $newName = _asZipDirName($fileName) unless defined($newName);
    return undef unless (stat($fileName) && -r _ && !-d _ );
    my $self = $class->new(@_);
    $self->{'fileName'}          = $newName;
    $self->{'externalFileName'}  = $fileName;
    $self->{'compressionMethod'} = COMPRESSION_STORED;
    my @stat = stat(_);
    $self->{'compressedSize'} = $self->{'uncompressedSize'} = $stat[7];
    $self->desiredCompressionMethod(
        ($self->compressedSize() > 0)
        ? COMPRESSION_DEFLATED
        : COMPRESSION_STORED
    );
    $self->unixFileAttributes($stat[2]);
    $self->setLastModFileDateTimeFromUnix($stat[9]);
    $self->isTextFile(-T _ );
    return $self;
}

sub rewindData {
    my $self = shift;

    my $status = $self->SUPER::rewindData(@_);
    return $status unless $status == AZ_OK;

    return AZ_IO_ERROR unless $self->fh();
    $self->fh()->clearerr();
    $self->fh()->seek(0, IO::Seekable::SEEK_SET)
      or return _ioError("rewinding", $self->externalFileName());
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

# If I already exist, extraction is a no-op.
sub extractToFileNamed {
    my $self = shift;
    my $name = shift;    # local FS name
    if (File::Spec->rel2abs($name) eq
        File::Spec->rel2abs($self->externalFileName()) and -r $name) {
        return AZ_OK;
    } else {
        return $self->SUPER::extractToFileNamed($name, @_);
    }
}

1;

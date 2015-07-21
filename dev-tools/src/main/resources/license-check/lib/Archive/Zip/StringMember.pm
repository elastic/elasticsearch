package Archive::Zip::StringMember;

use strict;
use vars qw( $VERSION @ISA );

BEGIN {
    $VERSION = '1.48';
    @ISA     = qw( Archive::Zip::Member );
}

use Archive::Zip qw(
  :CONSTANTS
  :ERROR_CODES
);

# Create a new string member. Default is COMPRESSION_STORED.
# Can take a ref to a string as well.
sub _newFromString {
    my $class  = shift;
    my $string = shift;
    my $name   = shift;
    my $self   = $class->new(@_);
    $self->contents($string);
    $self->fileName($name) if defined($name);

    # Set the file date to now
    $self->setLastModFileDateTimeFromUnix(time());
    $self->unixFileAttributes($self->DEFAULT_FILE_PERMISSIONS);
    return $self;
}

sub _become {
    my $self     = shift;
    my $newClass = shift;
    return $self if ref($self) eq $newClass;
    delete($self->{'contents'});
    return $self->SUPER::_become($newClass);
}

# Get or set my contents. Note that we do not call the superclass
# version of this, because it calls us.
sub contents {
    my $self   = shift;
    my $string = shift;
    if (defined($string)) {
        $self->{'contents'} =
          pack('C0a*', (ref($string) eq 'SCALAR') ? $$string : $string);
        $self->{'uncompressedSize'} = $self->{'compressedSize'} =
          length($self->{'contents'});
        $self->{'compressionMethod'} = COMPRESSION_STORED;
    }
    return $self->{'contents'};
}

# Return bytes read. Note that first parameter is a ref to a buffer.
# my $data;
# my ( $bytesRead, $status) = $self->readRawChunk( \$data, $chunkSize );
sub _readRawChunk {
    my ($self, $dataRef, $chunkSize) = @_;
    $$dataRef = substr($self->contents(), $self->_readOffset(), $chunkSize);
    return (length($$dataRef), AZ_OK);
}

1;

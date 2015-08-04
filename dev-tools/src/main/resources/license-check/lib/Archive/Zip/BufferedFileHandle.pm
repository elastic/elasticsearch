package Archive::Zip::BufferedFileHandle;

# File handle that uses a string internally and can seek
# This is given as a demo for getting a zip file written
# to a string.
# I probably should just use IO::Scalar instead.
# Ned Konz, March 2000

use strict;
use IO::File;
use Carp;

use vars qw{$VERSION};

BEGIN {
    $VERSION = '1.48';
    $VERSION = eval $VERSION;
}

sub new {
    my $class = shift || __PACKAGE__;
    $class = ref($class) || $class;
    my $self = bless(
        {
            content  => '',
            position => 0,
            size     => 0
        },
        $class
    );
    return $self;
}

# Utility method to read entire file
sub readFromFile {
    my $self     = shift;
    my $fileName = shift;
    my $fh       = IO::File->new($fileName, "r");
    CORE::binmode($fh);
    if (!$fh) {
        Carp::carp("Can't open $fileName: $!\n");
        return undef;
    }
    local $/ = undef;
    $self->{content} = <$fh>;
    $self->{size}    = length($self->{content});
    return $self;
}

sub contents {
    my $self = shift;
    if (@_) {
        $self->{content} = shift;
        $self->{size}    = length($self->{content});
    }
    return $self->{content};
}

sub binmode { 1 }

sub close { 1 }

sub opened { 1 }

sub eof {
    my $self = shift;
    return $self->{position} >= $self->{size};
}

sub seek {
    my $self   = shift;
    my $pos    = shift;
    my $whence = shift;

    # SEEK_SET
    if ($whence == 0) { $self->{position} = $pos; }

    # SEEK_CUR
    elsif ($whence == 1) { $self->{position} += $pos; }

    # SEEK_END
    elsif ($whence == 2) { $self->{position} = $self->{size} + $pos; }
    else                 { return 0; }

    return 1;
}

sub tell { return shift->{position}; }

# Copy my data to given buffer
sub read {
    my $self = shift;
    my $buf  = \($_[0]);
    shift;
    my $len = shift;
    my $offset = shift || 0;

    $$buf = '' if not defined($$buf);
    my $bytesRead =
        ($self->{position} + $len > $self->{size})
      ? ($self->{size} - $self->{position})
      : $len;
    substr($$buf, $offset, $bytesRead) =
      substr($self->{content}, $self->{position}, $bytesRead);
    $self->{position} += $bytesRead;
    return $bytesRead;
}

# Copy given buffer to me
sub write {
    my $self = shift;
    my $buf  = \($_[0]);
    shift;
    my $len = shift;
    my $offset = shift || 0;

    $$buf = '' if not defined($$buf);
    my $bufLen = length($$buf);
    my $bytesWritten =
      ($offset + $len > $bufLen)
      ? $bufLen - $offset
      : $len;
    substr($self->{content}, $self->{position}, $bytesWritten) =
      substr($$buf, $offset, $bytesWritten);
    $self->{size} = length($self->{content});
    return $bytesWritten;
}

sub clearerr() { 1 }

1;

package Archive::Zip::FileMember;

use strict;
use vars qw( $VERSION @ISA );

BEGIN {
    $VERSION = '1.48';
    @ISA     = qw ( Archive::Zip::Member );
}

use Archive::Zip qw(
  :UTILITY_METHODS
);

sub externalFileName {
    shift->{'externalFileName'};
}

# Return true if I depend on the named file
sub _usesFileNamed {
    my $self     = shift;
    my $fileName = shift;
    my $xfn      = $self->externalFileName();
    return undef if ref($xfn);
    return $xfn eq $fileName;
}

sub fh {
    my $self = shift;
    $self->_openFile()
      if !defined($self->{'fh'}) || !$self->{'fh'}->opened();
    return $self->{'fh'};
}

# opens my file handle from my file name
sub _openFile {
    my $self = shift;
    my ($status, $fh) = _newFileHandle($self->externalFileName(), 'r');
    if (!$status) {
        _ioError("Can't open", $self->externalFileName());
        return undef;
    }
    $self->{'fh'} = $fh;
    _binmode($fh);
    return $fh;
}

# Make sure I close my file handle
sub endRead {
    my $self = shift;
    undef $self->{'fh'};    # _closeFile();
    return $self->SUPER::endRead(@_);
}

sub _become {
    my $self     = shift;
    my $newClass = shift;
    return $self if ref($self) eq $newClass;
    delete($self->{'externalFileName'});
    delete($self->{'fh'});
    return $self->SUPER::_become($newClass);
}

1;

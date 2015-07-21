package Archive::Zip::MockFileHandle;

# Output file handle that calls a custom write routine
# Ned Konz, March 2000
# This is provided to help with writing zip files
# when you have to process them a chunk at a time.

use strict;

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
            'position' => 0,
            'size'     => 0
        },
        $class
    );
    return $self;
}

sub eof {
    my $self = shift;
    return $self->{'position'} >= $self->{'size'};
}

# Copy given buffer to me
sub print {
    my $self         = shift;
    my $bytes        = join('', @_);
    my $bytesWritten = $self->writeHook($bytes);
    if ($self->{'position'} + $bytesWritten > $self->{'size'}) {
        $self->{'size'} = $self->{'position'} + $bytesWritten;
    }
    $self->{'position'} += $bytesWritten;
    return $bytesWritten;
}

# Called on each write.
# Override in subclasses.
# Return number of bytes written (0 on error).
sub writeHook {
    my $self  = shift;
    my $bytes = shift;
    return length($bytes);
}

sub binmode { 1 }

sub close { 1 }

sub clearerr { 1 }

# I'm write-only!
sub read { 0 }

sub tell { return shift->{'position'} }

sub opened { 1 }

1;

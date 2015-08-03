package Archive::Zip::DirectoryMember;

use strict;
use File::Path;

use vars qw( $VERSION @ISA );

BEGIN {
    $VERSION = '1.48';
    @ISA     = qw( Archive::Zip::Member );
}

use Archive::Zip qw(
  :ERROR_CODES
  :UTILITY_METHODS
);

sub _newNamed {
    my $class    = shift;
    my $fileName = shift;    # FS name
    my $newName  = shift;    # Zip name
    $newName = _asZipDirName($fileName) unless $newName;
    my $self = $class->new(@_);
    $self->{'externalFileName'} = $fileName;
    $self->fileName($newName);

    if (-e $fileName) {

        # -e does NOT do a full stat, so we need to do one now
        if (-d _ ) {
            my @stat = stat(_);
            $self->unixFileAttributes($stat[2]);
            my $mod_t = $stat[9];
            if ($^O eq 'MSWin32' and !$mod_t) {
                $mod_t = time();
            }
            $self->setLastModFileDateTimeFromUnix($mod_t);

        } else {    # hmm.. trying to add a non-directory?
            _error($fileName, ' exists but is not a directory');
            return undef;
        }
    } else {
        $self->unixFileAttributes($self->DEFAULT_DIRECTORY_PERMISSIONS);
        $self->setLastModFileDateTimeFromUnix(time());
    }
    return $self;
}

sub externalFileName {
    shift->{'externalFileName'};
}

sub isDirectory {
    return 1;
}

sub extractToFileNamed {
    my $self    = shift;
    my $name    = shift;                                 # local FS name
    my $attribs = $self->unixFileAttributes() & 07777;
    mkpath($name, 0, $attribs);                          # croaks on error
    utime($self->lastModTime(), $self->lastModTime(), $name);
    return AZ_OK;
}

sub fileName {
    my $self    = shift;
    my $newName = shift;
    $newName =~ s{/?$}{/} if defined($newName);
    return $self->SUPER::fileName($newName);
}

# So people don't get too confused. This way it looks like the problem
# is in their code...
sub contents {
    return wantarray ? (undef, AZ_OK) : undef;
}

1;

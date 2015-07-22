###########################################################
#    Archive::Ar - Pure perl module to handle ar achives
#    
#    Copyright 2003 - Jay Bonci <jaybonci@cpan.org>
#    Copyright 2014 - John Bazik <jbazik@cpan.org>
#    Licensed under the same terms as perl itself
#
###########################################################
package Archive::Ar;

use base qw(Exporter);
our @EXPORT_OK = qw(COMMON BSD GNU);

use strict;
use File::Spec;
use Time::Local;
use Carp qw(carp longmess);

use vars qw($VERSION);
$VERSION = '2.02';

use constant CAN_CHOWN => ($> == 0 and $^O ne 'MacOS' and $^O ne 'MSWin32');

use constant ARMAG => "!<arch>\n";
use constant SARMAG => length(ARMAG);
use constant ARFMAG => "`\n";
use constant AR_EFMT1 => "#1/";

use constant COMMON => 1;
use constant BSD => 2;
use constant GNU => 3;

my $has_io_string;
BEGIN {
    $has_io_string = eval {
        require IO::String;
        IO::String->import();
        1;
    } || 0;
}

sub new {
    my $class = shift;
    my $file = shift;
    my $opts = shift || 0;
    my $self = bless {}, $class;
    my $defopts = {
        chmod => 1,
        chown => 1,
        same_perms => ($> == 0) ? 1:0,
        symbols => undef,
    };
    $opts = {warn => $opts} unless ref $opts;

    $self->clear();
    $self->{opts} = {(%$defopts, %{$opts})};
    if ($file) {
        return unless $self->read($file);
    }
    return $self;
}

sub set_opt {
    my $self = shift;
    my $name = shift;
    my $val = shift;

    $self->{opts}->{$name} = $val;
}

sub get_opt {
    my $self = shift;
    my $name = shift;

    return $self->{opts}->{$name};
}

sub type {
    return shift->{type};
}

sub clear {
    my $self = shift;

    $self->{names} = [];
    $self->{files} = {};
    $self->{type} = undef;
}

sub read {
    my $self = shift;
    my $file = shift;

    my $fh = $self->_get_handle($file);
    local $/ = undef;
    my $data = <$fh>;
    close $fh;
        
    return $self->read_memory($data);
}

sub read_memory {
    my $self = shift;
    my $data = shift;

    $self->clear();
    return unless $self->_parse($data);
    return length($data);
}

sub contains_file {
    my $self = shift;
    my $filename = shift;

    return unless defined $filename;
    return exists $self->{files}->{$filename};
}

sub extract {
    my $self = shift;

    for my $filename (@_ ? @_ : @{$self->{names}}) {
        $self->extract_file($filename) or return;
    }
    return 1;
}

sub extract_file {
    my $self = shift;
    my $filename = shift;
    my $target = shift || $filename;

    my $meta = $self->{files}->{$filename};
    return $self->_error("$filename: not in archive") unless $meta;
    open my $fh, '>', $target or return $self->_error("$target: $!");
    binmode $fh;
    syswrite $fh, $meta->{data} or return $self->_error("$filename: $!");
    close $fh or return $self->_error("$filename: $!");
    if (CAN_CHOWN && $self->{opts}->{chown}) {
        chown $meta->{uid}, $meta->{gid}, $filename or
					return $self->_error("$filename: $!");
    }
    if ($self->{opts}->{chmod}) {
        my $mode = $meta->{mode};
        unless ($self->{opts}->{same_perms}) {
            $mode &= ~(oct(7000) | (umask | 0));
        }
        chmod $mode, $filename or return $self->_error("$filename: $!");
    }
    utime $meta->{date}, $meta->{date}, $filename or
					return $self->_error("$filename: $!");
    return 1;
}

sub rename {
    my $self = shift;
    my $filename = shift;
    my $target = shift;

    if ($self->{files}->{$filename}) {
        $self->{files}->{$target} = $self->{files}->{$filename};
        delete $self->{files}->{$filename};
        for (@{$self->{names}}) {
            if ($_ eq $filename) {
                $_ = $target;
                last;
            }
        }
    }
}

sub chmod {
    my $self = shift;
    my $filename = shift;
    my $mode = shift;	# octal string or numeric

    return unless $self->{files}->{$filename};
    $self->{files}->{$filename}->{mode} =
                                    $mode + 0 eq $mode ? $mode : oct($mode);
    return 1;
}

sub chown {
    my $self = shift;
    my $filename = shift;
    my $uid = shift;
    my $gid = shift;

    return unless $self->{files}->{$filename};
    $self->{files}->{$filename}->{uid} = $uid if $uid >= 0;
    $self->{files}->{$filename}->{gid} = $gid if defined $gid && $gid >= 0;
    return 1;
}

sub remove {
    my $self = shift;
    my $files = ref $_[0] ? shift : \@_;

    my $nfiles_orig = scalar @{$self->{names}};

    for my $file (@$files) {
        next unless $file;
        if (exists($self->{files}->{$file})) {
            delete $self->{files}->{$file};
        }
        else {
            $self->_error("$file: no such member")
        }
    }
    @{$self->{names}} = grep($self->{files}->{$_}, @{$self->{names}});

    return $nfiles_orig - scalar @{$self->{names}};
}

sub list_files {
    my $self = shift;

    return wantarray ? @{$self->{names}} : $self->{names};
}

sub add_files {
    my $self = shift;
    my $files = ref $_[0] ? shift : \@_;

    for my $path (@$files) {
        if (open my $fd, $path) {
            my @st = stat $fd or return $self->_error("$path: $!");
            local $/ = undef;
            binmode $fd;
            my $content = <$fd>;
            close $fd;

            my $filename = (File::Spec->splitpath($path))[2];

            $self->_add_data($filename, $content, @st[9,4,5,2,7]);
        }
        else {
            $self->_error("$path: $!");
        }
    }
    return scalar @{$self->{names}};
}

sub add_data {
    my $self = shift;
    my $path = shift;
    my $content = shift;
    my $params = shift || {};

    return $self->_error("No filename given") unless $path;

    my $filename = (File::Spec->splitpath($path))[2];

    $self->_add_data($filename, $content,
                     $params->{date} || timelocal(localtime()),
                     $params->{uid} || 0,
                     $params->{gid} || 0,
                     $params->{mode} || 0100644) or return;

    return $self->{files}->{$filename}->{size};
}

sub write {
    my $self = shift;
    my $filename = shift;
    my $opts = {(%{$self->{opts}}, %{shift || {}})};
    my $type = $opts->{type} || $self->{type} || COMMON;

    my @body = ( ARMAG );

    my %gnuindex;
    my @filenames = @{$self->{names}};
    if ($type eq GNU) {
        #
        # construct extended filename index, if needed
        #
        if (my @longs = grep(length($_) > 15, @filenames)) {
            my $ptr = 0;
            for my $long (@longs) {
                $gnuindex{$long} = $ptr;
                $ptr += length($long) + 2;
            }
            push @body, pack('A16A32A10A2', '//', '', $ptr, ARFMAG),
                        join("/\n", @longs, '');
            push @body, "\n" if $ptr % 2; # padding
        }
    }
    for my $fn (@filenames) {
        my $meta = $self->{files}->{$fn};
        my $mode = sprintf('%o', $meta->{mode});
        my $size = $meta->{size};
        my $name;

        if ($type eq GNU) {
            $fn = '' if defined $opts->{symbols} && $fn eq $opts->{symbols};
            $name = $fn . '/';
        }
        else {
            $name = $fn;
        }
        if (length($name) <= 16 || $type eq COMMON) {
            push @body, pack('A16A12A6A6A8A10A2', $name,
                              @$meta{qw/date uid gid/}, $mode, $size, ARFMAG);
        }
        elsif ($type eq GNU) {
            push @body, pack('A1A15A12A6A6A8A10A2', '/', $gnuindex{$fn},
                              @$meta{qw/date uid gid/}, $mode, $size, ARFMAG);
        }
        elsif ($type eq BSD) {
            $size += length($name);
            push @body, pack('A3A13A12A6A6A8A10A2', AR_EFMT1, length($name),
                              @$meta{qw/date uid gid/}, $mode, $size, ARFMAG),
                        $name;
        }
        else {
            return $self->_error("$type: unexpected ar type");
        }
        push @body, $meta->{data};
        push @body, "\n" if $size % 2; # padding
    }
    if ($filename) {
        my $fh = $self->_get_handle($filename, '>');
        print $fh @body;
        close $fh;
        my $len = 0;
        $len += length($_) for @body;
        return $len;
    }
    else {
        return join '', @body;
    }
}

sub get_content {
    my $self = shift;
    my ($filename) = @_;

    unless ($filename) {
        $self->_error("get_content can't continue without a filename");
        return;
    }

    unless (exists($self->{files}->{$filename})) {
        $self->_error(
                "get_content failed because there is not a file named $filename");
        return;
    }

    return $self->{files}->{$filename};
}

sub get_data {
    my $self = shift;
    my $filename = shift;

    return $self->_error("$filename: no such member")
			unless exists $self->{files}->{$filename};
    return $self->{files}->{$filename}->{data};
}

sub get_handle {
    my $self = shift;
    my $filename = shift;
    my $fh;

    return $self->_error("$filename: no such member")
			unless exists $self->{files}->{$filename};
    if ($has_io_string) {
        $fh = IO::String->new($self->{files}->{$filename}->{data});
    }
    else {
        my $data = $self->{files}->{$filename}->{data};
        open $fh, '<', \$data or return $self->_error("in-memory file: $!");
    }
    return $fh;
}

sub error {
    my $self = shift;

    return shift() ? $self->{longmess} : $self->{error};
}

#
# deprecated
#
sub DEBUG {
    my $self = shift;
    my $debug = shift;

    $self->{opts}->{warn} = 1 unless (defined($debug) and int($debug) == 0);
}

sub _parse {
    my $self = shift;
    my $data = shift;

    unless (substr($data, 0, SARMAG, '') eq ARMAG) {
        return $self->_error("Bad magic number - not an ar archive");
    }
    my $type;
    my $names;
    while ($data =~ /\S/) {
        my ($name, $date, $uid, $gid, $mode, $size, $magic) =
                    unpack('A16A12A6A6A8A10a2', substr($data, 0, 60, ''));
        unless ($magic eq "`\n") {
            return $self->_error("Bad file header");
        }
        if ($name =~ m|^/|) {
            $type = GNU;
            if ($name eq '//') {
                $names = substr($data, 0, $size, '');
                substr($data, 0, $size % 2, '');
                next;
            }
            elsif ($name eq '/') {
                $name = $self->{opts}->{symbols};
                unless (defined $name && $name) {
                    substr($data, 0, $size + $size % 2, '');
                    next;
                }
            }
            else {
                $name = substr($names, int(substr($name, 1)));
                $name =~ s/\n.*//;
                chop $name;
            }
        }
        elsif ($name =~ m|^#1/|) {
            $type = BSD;
            $name = substr($data, 0, int(substr($name, 3)), '');
            $size -= length($name);
        }
        else {
            if ($name =~ m|/$|) {
                $type ||= GNU;	# only gnu has trailing slashes
                chop $name;
            }
        }
        $uid = int($uid);
        $gid = int($gid);
        $mode = oct($mode);
        my $content = substr($data, 0, $size, '');
        substr($data, 0, $size % 2, '');

        $self->_add_data($name, $content, $date, $uid, $gid, $mode, $size);
    }
    $self->{type} = $type || COMMON;
    return scalar @{$self->{names}};
}

sub _add_data {
    my $self = shift;
    my $filename = shift;
    my $content = shift || '';
    my $date = shift;
    my $uid = shift;
    my $gid = shift;
    my $mode = shift;
    my $size = shift;

    if (exists($self->{files}->{$filename})) {
        return $self->_error("$filename: entry already exists");
    }
    $self->{files}->{$filename} = {
        name => $filename,
        date => defined $date ? $date : timelocal(localtime()),
        uid => defined $uid ? $uid : 0,
        gid => defined $gid ? $gid : 0,
        mode => defined $mode ? $mode : 0100644,
        size => defined $size ? $size : length($content),
        data => $content,
    };
    push @{$self->{names}}, $filename;
    return 1;
}

sub _get_handle {
    my $self = shift;
    my $file = shift;
    my $mode = shift || '<';

    if (ref $file) {
        return $file if eval{*$file{IO}} or $file->isa('IO::Handle');
        return $self->_error("Not a filehandle");
    }
    else {
        open my $fh, $mode, $file or return $self->_error("$file: $!");
        binmode $fh;
        return $fh;
    }
}

sub _error {
    my $self = shift;
    my $msg = shift;

    $self->{error} = $msg;
    $self->{longerror} = longmess($msg);
    if ($self->{opts}->{warn} > 1) {
        carp $self->{longerror};
    }
    elsif ($self->{opts}->{warn}) {
        carp $self->{error};
    }
    return;
}

1;

__END__

=head1 NAME

Archive::Ar - Interface for manipulating ar archives

=head1 SYNOPSIS

    use Archive::Ar;

    my $ar = Archive::Ar->new;

    $ar->read('./foo.ar');
    $ar->extract;

    $ar->add_files('./bar.tar.gz', 'bat.pl')
    $ar->add_data('newfile.txt','Some contents');

    $ar->chmod('file1', 0644);
    $ar->chown('file1', $uid, $gid);

    $ar->remove('file1', 'file2');

    my $filehash = $ar->get_content('bar.tar.gz');
    my $data = $ar->get_data('bar.tar.gz');
    my $handle = $ar->get_handle('bar.tar.gz');

    my @files = $ar->list_files();

    my $archive = $ar->write;
    my $size = $ar->write('outbound.ar');

    $ar->error();


=head1 DESCRIPTION

Archive::Ar is a pure-perl way to handle standard ar archives.  

This is useful if you have those types of archives on the system, but it 
is also useful because .deb packages for the Debian GNU/Linux distribution are 
ar archives. This is one building block in a future chain of modules to build, 
manipulate, extract, and test debian modules with no platform or architecture 
dependence.

You may notice that the API to Archive::Ar is similar to Archive::Tar, and
this was done intentionally to keep similarity between the Archive::*
modules.

=head1 METHODS

=head2 new

  $ar = Archive::Ar->new()
  $ar = Archive::Ar->new($filename)
  $ar = Archive::Ar->new($filehandle)

Returns a new Archive::Ar object.  Without an argument, it returns
an empty object.  If passed a filename or an open filehandle, it will
read the referenced archive into memory.  If the read fails for any
reason, returns undef.

=head2 set_opt

  $ar->set_opt($name, $val)

Assign option $name value $val.  Possible options are:

=over 4

=item * warn

Warning level.  Levels are zero for no warnings, 1 for brief warnings,
and 2 for warnings with a stack trace.  Default is zero.

=item * chmod

Change the file permissions of files created when extracting.  Default
is true (non-zero).

=item * same_perms

When setting file permissions, use the values in the archive unchanged.
If false, removes setuid bits and applies the user's umask.  Default is
true for the root user, false otherwise.

=item * chown

Change the owners of extracted files, if possible.  Default is true.

=item * type

Archive type.  May be GNU, BSD or COMMON, or undef if no archive has
been read.  Defaults to the type of the archive read, or undef.

=item * symbols

Provide a filename for the symbol table, if present.  If set, the symbol
table is treated as a file that can be read from or written to an archive.
It is an error if the filename provided matches the name of a file in the
archive.  If undefined, the symbol table is ignored.  Defaults to undef.

=back

=head2 get_opt

  $val = $ar->get_opt($name)

Returns the value of option $name.

=head2 type

  $type = $ar->type()

Returns the type of the ar archive.  The type is undefined until an
archive is loaded.  If the archive displays characteristics of a gnu-style
archive, GNU is returned.  If it looks like a bsd-style archive, BSD
is returned.  Otherwise, COMMON is returned.  Note that unless filenames
exceed 16 characters in length, bsd archives look like the common format.

=head2 clear

  $ar->clear()

Clears the current in-memory archive.

=head2 read

  $len = $ar->read($filename)
  $len = $ar->read($filehandle)

This reads a new file into the object, removing any ar archive already
represented in the object.  The argument may be a filename, filehandle
or IO::Handle object.  Returns the size of the file contents or undef
if it fails.

=head2 read_memory

  $len = $ar->read_memory($data)

Parses the string argument as an archive, reading it into memory.  Replaces
any previously loaded archive.  Returns the number of bytes read, or undef
if it fails.

=head2 contains_file

  $bool = $ar->contains_file($filename)

Returns true if the archive contains a file with $filename.  Returns
undef otherwise.

=head2 extract

  $ar->extract()
  $ar->extract_file($filename)

Extracts files from the archive.  The first form extracts all files, the
latter extracts just the named file.  Extracted files are assigned the
permissions and modification time stored in the archive, and, if possible,
the user and group ownership.  Returns non-zero upon success, or undef if
failure.

=head2 rename

  $ar->rename($filename, $newname)

Changes the name of a file in the in-memory archive.

=head2 chmod

  $ar->chmod($filename, $mode);

Change the mode of the member to C<$mode>.

=head2 chown

  $ar->chown($filename, $uid, $gid);
  $ar->chown($filename, $uid);

Change the ownership of the member to user id C<$uid> and (optionally)
group id C<$gid>.  Negative id values are ignored.

=head2 remove

  $ar->remove(@filenames)
  $ar->remove($arrayref)

Removes files from the in-memory archive.  Returns the number of files
removed.

=head2 list_files

  @filenames = $ar->list_files()

Returns a list of the names of all the files in the archive.
If called in a scalar context, returns a reference to an array.

=head2 add_files

  $ar->add_files(@filenames)
  $ar->add_files($arrayref)

Adds files to the archive.  The arguments can be paths, but only the
filenames are stored in the archive.  Stores the uid, gid, mode, size,
and modification timestamp of the file as returned by C<stat()>.

Returns the number of files successfully added, or undef if failure.

=head2 add_data

  $ar->add_data("filename", $data)
  $ar->add_data("filename", $data, $options)

Adds a file to the in-memory archive with name $filename and content
$data.  File properties can be set with $optional_hashref:

  $options = {
      'data' => $data,
      'uid' => $uid,    #defaults to zero
      'gid' => $gid,    #defaults to zero
      'date' => $date,  #date in epoch seconds. Defaults to now.
      'mode' => $mode,  #defaults to 0100644;
  }

You cannot add_data over another file however.  This returns the file length in 
bytes if it is successful, undef otherwise.

=head2 write

  $data = $ar->write()
  $len = $ar->write($filename)

Returns the archive as a string, or writes it to disk as $filename.
Returns the archive size upon success when writing to disk.  Returns
undef if failure.

=head2 get_content

  $content = $ar->get_content($filename)

This returns a hash with the file content in it, including the data
that the file would contain.  If the file does not exist or no filename
is given, this returns undef. On success, a hash is returned:

    $content = {
        'name' => $filename,
        'date' => $mtime,
        'uid' => $uid,
        'gid' => $gid,
        'mode' => $mode,
        'size' => $size,
        'data' => $file_contents,
    }

=head2 get_data

  $data = $ar->get_data("filename")

Returns a scalar containing the file data of the given archive
member.  Upon error, returns undef.

=head2 get_handle

  $handle = $ar->get_handle("filename")>

Returns a file handle to the in-memory file data of the given archive member.
Upon error, returns undef.  This can be useful for unpacking nested archives.
Uses IO::String if it's loaded.

=head2 error

  $errstr = $ar->error($trace)

Returns the current error string, which is usually the last error reported.
If a true value is provided, returns the error message and stack trace.

=head1 BUGS

See https://github.com/jbazik/Archive-Ar/issues/ to report and view bugs.

=head1 SOURCE

The source code repository for Archive::Ar can be found at http://github.com/jbazik/Archive-Ar/.

=head1 COPYRIGHT

Copyright 2009-2014 John Bazik E<lt>jbazik@cpan.orgE<gt>.

Copyright 2003 Jay Bonci E<lt>jaybonci@cpan.orgE<gt>. 

This program is free software; you can redistribute it and/or modify it under
the same terms as Perl itself.

See http://www.perl.com/perl/misc/Artistic.html

=cut

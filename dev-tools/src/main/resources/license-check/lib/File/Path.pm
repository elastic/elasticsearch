package File::Path;

use 5.005_04;
use strict;

use Cwd 'getcwd';
use File::Basename ();
use File::Spec     ();

BEGIN {
    if ( $] < 5.006 ) {

        # can't say 'opendir my $dh, $dirname'
        # need to initialise $dh
        eval 'use Symbol';
    }
}

use Exporter ();
use vars qw($VERSION @ISA @EXPORT @EXPORT_OK);
$VERSION   = '2.11';
$VERSION   = eval $VERSION;
@ISA       = qw(Exporter);
@EXPORT    = qw(mkpath rmtree);
@EXPORT_OK = qw(make_path remove_tree);

BEGIN {
  for (qw(VMS MacOS MSWin32 os2)) {
    no strict 'refs';
    *{"_IS_\U$_"} = $^O eq $_ ? sub () { 1 } : sub () { 0 };
  }

  # These OSes complain if you want to remove a file that you have no
  # write permission to:
  *_FORCE_WRITABLE = (
    grep { $^O eq $_ } qw(amigaos dos epoc MSWin32 MacOS os2)
  ) ? sub () { 1 } : sub () { 0 };

  # Unix-like systems need to stat each directory in order to detect
  # race condition. MS-Windows is immune to this particular attack.
  *_NEED_STAT_CHECK = !(_IS_MSWIN32()) ? sub () { 1 } : sub () { 0 };
}

sub _carp {
    require Carp;
    goto &Carp::carp;
}

sub _croak {
    require Carp;
    goto &Carp::croak;
}

sub _error {
    my $arg     = shift;
    my $message = shift;
    my $object  = shift;

    if ( $arg->{error} ) {
        $object = '' unless defined $object;
        $message .= ": $!" if $!;
        push @{ ${ $arg->{error} } }, { $object => $message };
    }
    else {
        _carp( defined($object) ? "$message for $object: $!" : "$message: $!" );
    }
}

sub __is_arg {
    my ($arg) = @_;

    # If client code blessed an array ref to HASH, this will not work
    # properly. We could have done $arg->isa() wrapped in eval, but
    # that would be expensive. This implementation should suffice.
    # We could have also used Scalar::Util:blessed, but we choose not
    # to add this dependency
    return ( ref $arg eq 'HASH' );
}

sub make_path {
    push @_, {} unless @_ and __is_arg( $_[-1] );
    goto &mkpath;
}

sub mkpath {
    my $old_style = !( @_ and __is_arg( $_[-1] ) );

    my $arg;
    my $paths;

    if ($old_style) {
        my ( $verbose, $mode );
        ( $paths, $verbose, $mode ) = @_;
        $paths = [$paths] unless UNIVERSAL::isa( $paths, 'ARRAY' );
        $arg->{verbose} = $verbose;
        $arg->{mode} = defined $mode ? $mode : oct '777';
    }
    else {
        my %args_permitted = map { $_ => 1 } ( qw|
            chmod
            error
            group
            mask
            mode
            owner
            uid
            user
            verbose
        | );
        my @bad_args = ();
        $arg = pop @_;
        for my $k (sort keys %{$arg}) {
            push @bad_args, $k unless $args_permitted{$k};
        }
        _carp("Unrecognized option(s) passed to make_path(): @bad_args")
            if @bad_args;
        $arg->{mode} = delete $arg->{mask} if exists $arg->{mask};
        $arg->{mode} = oct '777' unless exists $arg->{mode};
        ${ $arg->{error} } = [] if exists $arg->{error};
        $arg->{owner} = delete $arg->{user} if exists $arg->{user};
        $arg->{owner} = delete $arg->{uid}  if exists $arg->{uid};
        if ( exists $arg->{owner} and $arg->{owner} =~ /\D/ ) {
            my $uid = ( getpwnam $arg->{owner} )[2];
            if ( defined $uid ) {
                $arg->{owner} = $uid;
            }
            else {
                _error( $arg,
"unable to map $arg->{owner} to a uid, ownership not changed"
                );
                delete $arg->{owner};
            }
        }
        if ( exists $arg->{group} and $arg->{group} =~ /\D/ ) {
            my $gid = ( getgrnam $arg->{group} )[2];
            if ( defined $gid ) {
                $arg->{group} = $gid;
            }
            else {
                _error( $arg,
"unable to map $arg->{group} to a gid, group ownership not changed"
                );
                delete $arg->{group};
            }
        }
        if ( exists $arg->{owner} and not exists $arg->{group} ) {
            $arg->{group} = -1;    # chown will leave group unchanged
        }
        if ( exists $arg->{group} and not exists $arg->{owner} ) {
            $arg->{owner} = -1;    # chown will leave owner unchanged
        }
        $paths = [@_];
    }
    return _mkpath( $arg, $paths );
}

sub _mkpath {
    my $arg   = shift;
    my $paths = shift;

    my ( @created );
    foreach my $path ( @{$paths} ) {
        next unless defined($path) and length($path);
        $path .= '/' if _IS_OS2 and $path =~ /^\w:\z/s; # feature of CRT

        # Logic wants Unix paths, so go with the flow.
        if (_IS_VMS) {
            next if $path eq '/';
            $path = VMS::Filespec::unixify($path);
        }
        next if -d $path;
        my $parent = File::Basename::dirname($path);
        unless ( -d $parent or $path eq $parent ) {
            push( @created, _mkpath( $arg, [$parent] ) );
        }
        print "mkdir $path\n" if $arg->{verbose};
        if ( mkdir( $path, $arg->{mode} ) ) {
            push( @created, $path );
            if ( exists $arg->{owner} ) {

                # NB: $arg->{group} guaranteed to be set during initialisation
                if ( !chown $arg->{owner}, $arg->{group}, $path ) {
                    _error( $arg,
"Cannot change ownership of $path to $arg->{owner}:$arg->{group}"
                    );
                }
            }
            if ( exists $arg->{chmod} ) {
                if ( !chmod $arg->{chmod}, $path ) {
                    _error( $arg,
                        "Cannot change permissions of $path to $arg->{chmod}" );
                }
            }
        }
        else {
            my $save_bang = $!;
            my ( $e, $e1 ) = ( $save_bang, $^E );
            $e .= "; $e1" if $e ne $e1;

            # allow for another process to have created it meanwhile
            if ( ! -d $path ) {
                $! = $save_bang;
                if ( $arg->{error} ) {
                    push @{ ${ $arg->{error} } }, { $path => $e };
                }
                else {
                    _croak("mkdir $path: $e");
                }
            }
        }
    }
    return @created;
}

sub remove_tree {
    push @_, {} unless @_ and __is_arg( $_[-1] );
    goto &rmtree;
}

sub _is_subdir {
    my ( $dir, $test ) = @_;

    my ( $dv, $dd ) = File::Spec->splitpath( $dir,  1 );
    my ( $tv, $td ) = File::Spec->splitpath( $test, 1 );

    # not on same volume
    return 0 if $dv ne $tv;

    my @d = File::Spec->splitdir($dd);
    my @t = File::Spec->splitdir($td);

    # @t can't be a subdir if it's shorter than @d
    return 0 if @t < @d;

    return join( '/', @d ) eq join( '/', splice @t, 0, +@d );
}

sub rmtree {
    my $old_style = !( @_ and __is_arg( $_[-1] ) );

    my $arg;
    my $paths;

    if ($old_style) {
        my ( $verbose, $safe );
        ( $paths, $verbose, $safe ) = @_;
        $arg->{verbose} = $verbose;
        $arg->{safe} = defined $safe ? $safe : 0;

        if ( defined($paths) and length($paths) ) {
            $paths = [$paths] unless UNIVERSAL::isa( $paths, 'ARRAY' );
        }
        else {
            _carp("No root path(s) specified\n");
            return 0;
        }
    }
    else {
        my %args_permitted = map { $_ => 1 } ( qw|
            error
            keep_root
            result
            safe
            verbose
        | );
        my @bad_args = ();
        $arg = pop @_;
        for my $k (sort keys %{$arg}) {
            push @bad_args, $k unless $args_permitted{$k};
        }
        _carp("Unrecognized option(s) passed to remove_tree(): @bad_args")
            if @bad_args;
        ${ $arg->{error} }  = [] if exists $arg->{error};
        ${ $arg->{result} } = [] if exists $arg->{result};
        $paths = [@_];
    }

    $arg->{prefix} = '';
    $arg->{depth}  = 0;

    my @clean_path;
    $arg->{cwd} = getcwd() or do {
        _error( $arg, "cannot fetch initial working directory" );
        return 0;
    };
    for ( $arg->{cwd} ) { /\A(.*)\Z/s; $_ = $1 }    # untaint

    for my $p (@$paths) {

        # need to fixup case and map \ to / on Windows
        my $ortho_root = _IS_MSWIN32 ? _slash_lc($p) : $p;
        my $ortho_cwd =
          _IS_MSWIN32 ? _slash_lc( $arg->{cwd} ) : $arg->{cwd};
        my $ortho_root_length = length($ortho_root);
        $ortho_root_length-- if _IS_VMS;   # don't compare '.' with ']'
        if ( $ortho_root_length && _is_subdir( $ortho_root, $ortho_cwd ) ) {
            local $! = 0;
            _error( $arg, "cannot remove path when cwd is $arg->{cwd}", $p );
            next;
        }

        if (_IS_MACOS) {
            $p = ":$p" unless $p =~ /:/;
            $p .= ":" unless $p =~ /:\z/;
        }
        elsif ( _IS_MSWIN32 ) {
            $p =~ s{[/\\]\z}{};
        }
        else {
            $p =~ s{/\z}{};
        }
        push @clean_path, $p;
    }

    @{$arg}{qw(device inode perm)} = ( lstat $arg->{cwd} )[ 0, 1 ] or do {
        _error( $arg, "cannot stat initial working directory", $arg->{cwd} );
        return 0;
    };

    return _rmtree( $arg, \@clean_path );
}

sub _rmtree {
    my $arg   = shift;
    my $paths = shift;

    my $count  = 0;
    my $curdir = File::Spec->curdir();
    my $updir  = File::Spec->updir();

    my ( @files, $root );
  ROOT_DIR:
    foreach my $root (@$paths) {

        # since we chdir into each directory, it may not be obvious
        # to figure out where we are if we generate a message about
        # a file name. We therefore construct a semi-canonical
        # filename, anchored from the directory being unlinked (as
        # opposed to being truly canonical, anchored from the root (/).

        my $canon =
          $arg->{prefix}
          ? File::Spec->catfile( $arg->{prefix}, $root )
          : $root;

        my ( $ldev, $lino, $perm ) = ( lstat $root )[ 0, 1, 2 ]
          or ( _error( $arg, "$root", $root ) and next ROOT_DIR );

        if ( -d _ ) {
            $root = VMS::Filespec::vmspath( VMS::Filespec::pathify($root) )
              if _IS_VMS;

            if ( !chdir($root) ) {

                # see if we can escalate privileges to get in
                # (e.g. funny protection mask such as -w- instead of rwx)
                $perm &= oct '7777';
                my $nperm = $perm | oct '700';
                if (
                    !(
                           $arg->{safe}
                        or $nperm == $perm
                        or chmod( $nperm, $root )
                    )
                  )
                {
                    _error( $arg,
                        "cannot make child directory read-write-exec", $canon );
                    next ROOT_DIR;
                }
                elsif ( !chdir($root) ) {
                    _error( $arg, "cannot chdir to child", $canon );
                    next ROOT_DIR;
                }
            }

            my ( $cur_dev, $cur_inode, $perm ) = ( stat $curdir )[ 0, 1, 2 ]
              or do {
                _error( $arg, "cannot stat current working directory", $canon );
                next ROOT_DIR;
              };

            if (_NEED_STAT_CHECK) {
                ( $ldev eq $cur_dev and $lino eq $cur_inode )
                  or _croak(
"directory $canon changed before chdir, expected dev=$ldev ino=$lino, actual dev=$cur_dev ino=$cur_inode, aborting."
                  );
            }

            $perm &= oct '7777';    # don't forget setuid, setgid, sticky bits
            my $nperm = $perm | oct '700';

            # notabene: 0700 is for making readable in the first place,
            # it's also intended to change it to writable in case we have
            # to recurse in which case we are better than rm -rf for
            # subtrees with strange permissions

            if (
                !(
                       $arg->{safe}
                    or $nperm == $perm
                    or chmod( $nperm, $curdir )
                )
              )
            {
                _error( $arg, "cannot make directory read+writeable", $canon );
                $nperm = $perm;
            }

            my $d;
            $d = gensym() if $] < 5.006;
            if ( !opendir $d, $curdir ) {
                _error( $arg, "cannot opendir", $canon );
                @files = ();
            }
            else {
                if ( !defined ${^TAINT} or ${^TAINT} ) {
                    # Blindly untaint dir names if taint mode is active
                    @files = map { /\A(.*)\z/s; $1 } readdir $d;
                }
                else {
                    @files = readdir $d;
                }
                closedir $d;
            }

            if (_IS_VMS) {

                # Deleting large numbers of files from VMS Files-11
                # filesystems is faster if done in reverse ASCIIbetical order.
                # include '.' to '.;' from blead patch #31775
                @files = map { $_ eq '.' ? '.;' : $_ } reverse @files;
            }

            @files = grep { $_ ne $updir and $_ ne $curdir } @files;

            if (@files) {

                # remove the contained files before the directory itself
                my $narg = {%$arg};
                @{$narg}{qw(device inode cwd prefix depth)} =
                  ( $cur_dev, $cur_inode, $updir, $canon, $arg->{depth} + 1 );
                $count += _rmtree( $narg, \@files );
            }

            # restore directory permissions of required now (in case the rmdir
            # below fails), while we are still in the directory and may do so
            # without a race via '.'
            if ( $nperm != $perm and not chmod( $perm, $curdir ) ) {
                _error( $arg, "cannot reset chmod", $canon );
            }

            # don't leave the client code in an unexpected directory
            chdir( $arg->{cwd} )
              or
              _croak("cannot chdir to $arg->{cwd} from $canon: $!, aborting.");

            # ensure that a chdir upwards didn't take us somewhere other
            # than we expected (see CVE-2002-0435)
            ( $cur_dev, $cur_inode ) = ( stat $curdir )[ 0, 1 ]
              or _croak(
                "cannot stat prior working directory $arg->{cwd}: $!, aborting."
              );

            if (_NEED_STAT_CHECK) {
                ( $arg->{device} eq $cur_dev and $arg->{inode} eq $cur_inode )
                  or _croak(  "previous directory $arg->{cwd} "
                            . "changed before entering $canon, "
                            . "expected dev=$ldev ino=$lino, "
                            . "actual dev=$cur_dev ino=$cur_inode, aborting."
                  );
            }

            if ( $arg->{depth} or !$arg->{keep_root} ) {
                if ( $arg->{safe}
                    && ( _IS_VMS
                        ? !&VMS::Filespec::candelete($root)
                        : !-w $root ) )
                {
                    print "skipped $root\n" if $arg->{verbose};
                    next ROOT_DIR;
                }
                if ( _FORCE_WRITABLE and !chmod $perm | oct '700', $root ) {
                    _error( $arg, "cannot make directory writeable", $canon );
                }
                print "rmdir $root\n" if $arg->{verbose};
                if ( rmdir $root ) {
                    push @{ ${ $arg->{result} } }, $root if $arg->{result};
                    ++$count;
                }
                else {
                    _error( $arg, "cannot remove directory", $canon );
                    if (
                        _FORCE_WRITABLE
                        && !chmod( $perm,
                            ( _IS_VMS ? VMS::Filespec::fileify($root) : $root )
                        )
                      )
                    {
                        _error(
                            $arg,
                            sprintf( "cannot restore permissions to 0%o",
                                $perm ),
                            $canon
                        );
                    }
                }
            }
        }
        else {
            # not a directory
            $root = VMS::Filespec::vmsify("./$root")
              if _IS_VMS
              && !File::Spec->file_name_is_absolute($root)
              && ( $root !~ m/(?<!\^)[\]>]+/ );    # not already in VMS syntax

            if (
                $arg->{safe}
                && (
                    _IS_VMS
                    ? !&VMS::Filespec::candelete($root)
                    : !( -l $root || -w $root )
                )
              )
            {
                print "skipped $root\n" if $arg->{verbose};
                next ROOT_DIR;
            }

            my $nperm = $perm & oct '7777' | oct '600';
            if (    _FORCE_WRITABLE
                and $nperm != $perm
                and not chmod $nperm, $root )
            {
                _error( $arg, "cannot make file writeable", $canon );
            }
            print "unlink $canon\n" if $arg->{verbose};

            # delete all versions under VMS
            for ( ; ; ) {
                if ( unlink $root ) {
                    push @{ ${ $arg->{result} } }, $root if $arg->{result};
                }
                else {
                    _error( $arg, "cannot unlink file", $canon );
                    _FORCE_WRITABLE and chmod( $perm, $root )
                      or _error( $arg,
                        sprintf( "cannot restore permissions to 0%o", $perm ),
                        $canon );
                    last;
                }
                ++$count;
                last unless _IS_VMS && lstat $root;
            }
        }
    }
    return $count;
}

sub _slash_lc {

    # fix up slashes and case on MSWin32 so that we can determine that
    # c:\path\to\dir is underneath C:/Path/To
    my $path = shift;
    $path =~ tr{\\}{/};
    return lc($path);
}

1;

__END__

=head1 NAME

File::Path - Create or remove directory trees

=head1 VERSION

This document describes version 2.09 of File::Path, released
2013-01-17.

=head1 SYNOPSIS

  use File::Path qw(make_path remove_tree);

  @created = make_path('foo/bar/baz', '/zug/zwang');
  @created = make_path('foo/bar/baz', '/zug/zwang', {
      verbose => 1,
      mode => 0711,
  });
  make_path('foo/bar/baz', '/zug/zwang', {
      chmod => 0777,
  });

  $removed_count = remove_tree('foo/bar/baz', '/zug/zwang');
  $removed_count = remove_tree('foo/bar/baz', '/zug/zwang', {
      verbose => 1,
      error  => \my $err_list,
  });

  # legacy (interface promoted before v2.00)
  @created = mkpath('/foo/bar/baz');
  @created = mkpath('/foo/bar/baz', 1, 0711);
  @created = mkpath(['/foo/bar/baz', 'blurfl/quux'], 1, 0711);
  $removed_count = rmtree('foo/bar/baz', 1, 1);
  $removed_count = rmtree(['foo/bar/baz', 'blurfl/quux'], 1, 1);

  # legacy (interface promoted before v2.06)
  @created = mkpath('foo/bar/baz', '/zug/zwang', { verbose => 1, mode => 0711 });
  $removed_count = rmtree('foo/bar/baz', '/zug/zwang', { verbose => 1, mode => 0711 });

=head1 DESCRIPTION

This module provide a convenient way to create directories of
arbitrary depth and to delete an entire directory subtree from the
filesystem.

The following functions are provided:

=over

=item make_path( $dir1, $dir2, .... )

=item make_path( $dir1, $dir2, ...., \%opts )

The C<make_path> function creates the given directories if they don't
exists before, much like the Unix command C<mkdir -p>.

The function accepts a list of directories to be created. Its
behaviour may be tuned by an optional hashref appearing as the last
parameter on the call.

The function returns the list of directories actually created during
the call; in scalar context the number of directories created.

The following keys are recognised in the option hash:

=over

=item mode => $num

The numeric permissions mode to apply to each created directory
(defaults to 0777), to be modified by the current C<umask>. If the
directory already exists (and thus does not need to be created),
the permissions will not be modified.

C<mask> is recognised as an alias for this parameter.

=item chmod => $num

Takes a numeric mode to apply to each created directory (not
modified by the current C<umask>). If the directory already exists
(and thus does not need to be created), the permissions will
not be modified.

=item verbose => $bool

If present, will cause C<make_path> to print the name of each directory
as it is created. By default nothing is printed.

=item error => \$err

If present, it should be a reference to a scalar.
This scalar will be made to reference an array, which will
be used to store any errors that are encountered.  See the L</"ERROR
HANDLING"> section for more information.

If this parameter is not used, certain error conditions may raise
a fatal error that will cause the program to halt, unless trapped
in an C<eval> block.

=item owner => $owner

=item user => $owner

=item uid => $owner

If present, will cause any created directory to be owned by C<$owner>.
If the value is numeric, it will be interpreted as a uid, otherwise
as username is assumed. An error will be issued if the username cannot be
mapped to a uid, or the uid does not exist, or the process lacks the
privileges to change ownership.

Ownership of directories that already exist will not be changed.

C<user> and C<uid> are aliases of C<owner>.

=item group => $group

If present, will cause any created directory to be owned by the group C<$group>.
If the value is numeric, it will be interpreted as a gid, otherwise
as group name is assumed. An error will be issued if the group name cannot be
mapped to a gid, or the gid does not exist, or the process lacks the
privileges to change group ownership.

Group ownership of directories that already exist will not be changed.

    make_path '/var/tmp/webcache', {owner=>'nobody', group=>'nogroup'};

=back

=item mkpath( $dir )

=item mkpath( $dir, $verbose, $mode )

=item mkpath( [$dir1, $dir2,...], $verbose, $mode )

=item mkpath( $dir1, $dir2,..., \%opt )

The mkpath() function provide the legacy interface of make_path() with
a different interpretation of the arguments passed.  The behaviour and
return value of the function is otherwise identical to make_path().

=item remove_tree( $dir1, $dir2, .... )

=item remove_tree( $dir1, $dir2, ...., \%opts )

The C<remove_tree> function deletes the given directories and any
files and subdirectories they might contain, much like the Unix
command C<rm -r> or the Windows commands C<rmdir /s> and C<rd /s>.

The function accepts a list of directories to be
removed. Its behaviour may be tuned by an optional hashref
appearing as the last parameter on the call.

The functions returns the number of files successfully deleted.

The following keys are recognised in the option hash:

=over

=item verbose => $bool

If present, will cause C<remove_tree> to print the name of each file as
it is unlinked. By default nothing is printed.

=item safe => $bool

When set to a true value, will cause C<remove_tree> to skip the files
for which the process lacks the required privileges needed to delete
files, such as delete privileges on VMS. In other words, the code
will make no attempt to alter file permissions. Thus, if the process
is interrupted, no filesystem object will be left in a more
permissive mode.

=item keep_root => $bool

When set to a true value, will cause all files and subdirectories
to be removed, except the initially specified directories. This comes
in handy when cleaning out an application's scratch directory.

  remove_tree( '/tmp', {keep_root => 1} );

=item result => \$res

If present, it should be a reference to a scalar.
This scalar will be made to reference an array, which will
be used to store all files and directories unlinked
during the call. If nothing is unlinked, the array will be empty.

  remove_tree( '/tmp', {result => \my $list} );
  print "unlinked $_\n" for @$list;

This is a useful alternative to the C<verbose> key.

=item error => \$err

If present, it should be a reference to a scalar.
This scalar will be made to reference an array, which will
be used to store any errors that are encountered.  See the L</"ERROR
HANDLING"> section for more information.

Removing things is a much more dangerous proposition than
creating things. As such, there are certain conditions that
C<remove_tree> may encounter that are so dangerous that the only
sane action left is to kill the program.

Use C<error> to trap all that is reasonable (problems with
permissions and the like), and let it die if things get out
of hand. This is the safest course of action.

=back

=item rmtree( $dir )

=item rmtree( $dir, $verbose, $safe )

=item rmtree( [$dir1, $dir2,...], $verbose, $safe )

=item rmtree( $dir1, $dir2,..., \%opt )

The rmtree() function provide the legacy interface of remove_tree()
with a different interpretation of the arguments passed. The behaviour
and return value of the function is otherwise identical to
remove_tree().

=back

=head2 ERROR HANDLING

=over 4

=item B<NOTE:>

The following error handling mechanism is considered
experimental and is subject to change pending feedback from
users.

=back

If C<make_path> or C<remove_tree> encounter an error, a diagnostic
message will be printed to C<STDERR> via C<carp> (for non-fatal
errors), or via C<croak> (for fatal errors).

If this behaviour is not desirable, the C<error> attribute may be
used to hold a reference to a variable, which will be used to store
the diagnostics. The variable is made a reference to an array of hash
references.  Each hash contain a single key/value pair where the key
is the name of the file, and the value is the error message (including
the contents of C<$!> when appropriate).  If a general error is
encountered the diagnostic key will be empty.

An example usage looks like:

  remove_tree( 'foo/bar', 'bar/rat', {error => \my $err} );
  if (@$err) {
      for my $diag (@$err) {
          my ($file, $message) = %$diag;
          if ($file eq '') {
              print "general error: $message\n";
          }
          else {
              print "problem unlinking $file: $message\n";
          }
      }
  }
  else {
      print "No error encountered\n";
  }

Note that if no errors are encountered, C<$err> will reference an
empty array.  This means that C<$err> will always end up TRUE; so you
need to test C<@$err> to determine if errors occurred.

=head2 NOTES

C<File::Path> blindly exports C<mkpath> and C<rmtree> into the
current namespace. These days, this is considered bad style, but
to change it now would break too much code. Nonetheless, you are
invited to specify what it is you are expecting to use:

  use File::Path 'rmtree';

The routines C<make_path> and C<remove_tree> are B<not> exported
by default. You must specify which ones you want to use.

  use File::Path 'remove_tree';

Note that a side-effect of the above is that C<mkpath> and C<rmtree>
are no longer exported at all. This is due to the way the C<Exporter>
module works. If you are migrating a codebase to use the new
interface, you will have to list everything explicitly. But that's
just good practice anyway.

  use File::Path qw(remove_tree rmtree);

=head3 API CHANGES

The API was changed in the 2.0 branch. For a time, C<mkpath> and
C<rmtree> tried, unsuccessfully, to deal with the two different
calling mechanisms. This approach was considered a failure.

The new semantics are now only available with C<make_path> and
C<remove_tree>. The old semantics are only available through
C<mkpath> and C<rmtree>. Users are strongly encouraged to upgrade
to at least 2.08 in order to avoid surprises.

=head3 SECURITY CONSIDERATIONS

There were race conditions 1.x implementations of File::Path's
C<rmtree> function (although sometimes patched depending on the OS
distribution or platform). The 2.0 version contains code to avoid the
problem mentioned in CVE-2002-0435.

See the following pages for more information:

  http://bugs.debian.org/cgi-bin/bugreport.cgi?bug=286905
  http://www.nntp.perl.org/group/perl.perl5.porters/2005/01/msg97623.html
  http://www.debian.org/security/2005/dsa-696

Additionally, unless the C<safe> parameter is set (or the
third parameter in the traditional interface is TRUE), should a
C<remove_tree> be interrupted, files that were originally in read-only
mode may now have their permissions set to a read-write (or "delete
OK") mode.

=head1 DIAGNOSTICS

FATAL errors will cause the program to halt (C<croak>), since the
problem is so severe that it would be dangerous to continue. (This
can always be trapped with C<eval>, but it's not a good idea. Under
the circumstances, dying is the best thing to do).

SEVERE errors may be trapped using the modern interface. If the
they are not trapped, or the old interface is used, such an error
will cause the program will halt.

All other errors may be trapped using the modern interface, otherwise
they will be C<carp>ed about. Program execution will not be halted.

=over 4

=item mkdir [path]: [errmsg] (SEVERE)

C<make_path> was unable to create the path. Probably some sort of
permissions error at the point of departure, or insufficient resources
(such as free inodes on Unix).

=item No root path(s) specified

C<make_path> was not given any paths to create. This message is only
emitted if the routine is called with the traditional interface.
The modern interface will remain silent if given nothing to do.

=item No such file or directory

On Windows, if C<make_path> gives you this warning, it may mean that
you have exceeded your filesystem's maximum path length.

=item cannot fetch initial working directory: [errmsg]

C<remove_tree> attempted to determine the initial directory by calling
C<Cwd::getcwd>, but the call failed for some reason. No attempt
will be made to delete anything.

=item cannot stat initial working directory: [errmsg]

C<remove_tree> attempted to stat the initial directory (after having
successfully obtained its name via C<getcwd>), however, the call
failed for some reason. No attempt will be made to delete anything.

=item cannot chdir to [dir]: [errmsg]

C<remove_tree> attempted to set the working directory in order to
begin deleting the objects therein, but was unsuccessful. This is
usually a permissions issue. The routine will continue to delete
other things, but this directory will be left intact.

=item directory [dir] changed before chdir, expected dev=[n] ino=[n], actual dev=[n] ino=[n], aborting. (FATAL)

C<remove_tree> recorded the device and inode of a directory, and then
moved into it. It then performed a C<stat> on the current directory
and detected that the device and inode were no longer the same. As
this is at the heart of the race condition problem, the program
will die at this point.

=item cannot make directory [dir] read+writeable: [errmsg]

C<remove_tree> attempted to change the permissions on the current directory
to ensure that subsequent unlinkings would not run into problems,
but was unable to do so. The permissions remain as they were, and
the program will carry on, doing the best it can.

=item cannot read [dir]: [errmsg]

C<remove_tree> tried to read the contents of the directory in order
to acquire the names of the directory entries to be unlinked, but
was unsuccessful. This is usually a permissions issue. The
program will continue, but the files in this directory will remain
after the call.

=item cannot reset chmod [dir]: [errmsg]

C<remove_tree>, after having deleted everything in a directory, attempted
to restore its permissions to the original state but failed. The
directory may wind up being left behind.

=item cannot remove [dir] when cwd is [dir]

The current working directory of the program is F</some/path/to/here>
and you are attempting to remove an ancestor, such as F</some/path>.
The directory tree is left untouched.

The solution is to C<chdir> out of the child directory to a place
outside the directory tree to be removed.

=item cannot chdir to [parent-dir] from [child-dir]: [errmsg], aborting. (FATAL)

C<remove_tree>, after having deleted everything and restored the permissions
of a directory, was unable to chdir back to the parent. The program
halts to avoid a race condition from occurring.

=item cannot stat prior working directory [dir]: [errmsg], aborting. (FATAL)

C<remove_tree> was unable to stat the parent directory after have returned
from the child. Since there is no way of knowing if we returned to
where we think we should be (by comparing device and inode) the only
way out is to C<croak>.

=item previous directory [parent-dir] changed before entering [child-dir], expected dev=[n] ino=[n], actual dev=[n] ino=[n], aborting. (FATAL)

When C<remove_tree> returned from deleting files in a child directory, a
check revealed that the parent directory it returned to wasn't the one
it started out from. This is considered a sign of malicious activity.

=item cannot make directory [dir] writeable: [errmsg]

Just before removing a directory (after having successfully removed
everything it contained), C<remove_tree> attempted to set the permissions
on the directory to ensure it could be removed and failed. Program
execution continues, but the directory may possibly not be deleted.

=item cannot remove directory [dir]: [errmsg]

C<remove_tree> attempted to remove a directory, but failed. This may because
some objects that were unable to be removed remain in the directory, or
a permissions issue. The directory will be left behind.

=item cannot restore permissions of [dir] to [0nnn]: [errmsg]

After having failed to remove a directory, C<remove_tree> was unable to
restore its permissions from a permissive state back to a possibly
more restrictive setting. (Permissions given in octal).

=item cannot make file [file] writeable: [errmsg]

C<remove_tree> attempted to force the permissions of a file to ensure it
could be deleted, but failed to do so. It will, however, still attempt
to unlink the file.

=item cannot unlink file [file]: [errmsg]

C<remove_tree> failed to remove a file. Probably a permissions issue.

=item cannot restore permissions of [file] to [0nnn]: [errmsg]

After having failed to remove a file, C<remove_tree> was also unable
to restore the permissions on the file to a possibly less permissive
setting. (Permissions given in octal).

=item unable to map [owner] to a uid, ownership not changed");

C<make_path> was instructed to give the ownership of created
directories to the symbolic name [owner], but C<getpwnam> did
not return the corresponding numeric uid. The directory will
be created, but ownership will not be changed.

=item unable to map [group] to a gid, group ownership not changed

C<make_path> was instructed to give the group ownership of created
directories to the symbolic name [group], but C<getgrnam> did
not return the corresponding numeric gid. The directory will
be created, but group ownership will not be changed.

=back

=head1 SEE ALSO

=over 4

=item *

L<File::Remove>

Allows files and directories to be moved to the Trashcan/Recycle
Bin (where they may later be restored if necessary) if the operating
system supports such functionality. This feature may one day be
made available directly in C<File::Path>.

=item *

L<File::Find::Rule>

When removing directory trees, if you want to examine each file to
decide whether to delete it (and possibly leaving large swathes
alone), F<File::Find::Rule> offers a convenient and flexible approach
to examining directory trees.

=back

=head1 BUGS AND LIMITATIONS

The following describes F<File::Path> limitations and how to report bugs.

=head2 MULTITHREAD APPLICATIONS

F<File::Path> B<rmtree> and B<remove_tree> will not work with multithreaded
applications due to its use of B<chdir>.  At this time, no warning or error
results and you will certainly encounter unexpected results.

The implementation that surfaces this limitation may change in a future
release.

=head2 NFS Mount Points

F<File::Path> is not responsible for triggering the automounts, mirror mounts,
and the contents of network mounted filesystems.  If your NFS implementation
requires an action to be performed on the filesystem in order for
F<File::Path> to perform operations, it is strongly suggested you assure
filesystem availability by reading the root of the mounted filesystem.

=head2 REPORTING BUGS

Please report all bugs on the RT queue, either via the web interface:

L<http://rt.cpan.org/NoAuth/Bugs.html?Dist=File-Path>

or by email:

    bug-File-Path@rt.cpan.org

In either case, please B<attach> patches to the bug report rather than
including them inline in the web post or the body of the email.

You can also send pull requests to the Github repository:

L<https://github.com/rpcme/File-Path>

=head1 ACKNOWLEDGEMENTS

Paul Szabo identified the race condition originally, and Brendan
O'Dea wrote an implementation for Debian that addressed the problem.
That code was used as a basis for the current code. Their efforts
are greatly appreciated.

Gisle Aas made a number of improvements to the documentation for
2.07 and his advice and assistance is also greatly appreciated.

=head1 AUTHORS

Prior authors and maintainers: Tim Bunce, Charles Bailey, and
David Landgren <F<david@landgren.net>>.

Current maintainers are Richard Elberger <F<riche@cpan.org>> and
James (Jim) Keenan <F<jkeenan@cpan.org>>.

=head1 CONTRIBUTORS

Contributors to File::Path, in alphabetical order.

=over 1

=item <F<bulkdd@cpan.org>>

=item Richard Elberger <F<riche@cpan.org>>

=item Ryan Yee <F<ryee@cpan.org>>

=item Skye Shaw <F<shaw@cpan.org>>

=item Tom Lutz <F<tommylutz@gmail.com>>

=back

=head1 COPYRIGHT

This module is copyright (C) Charles Bailey, Tim Bunce, David Landgren,
James Keenan, and Richard Elberger 1995-2015. All rights reserved.

=head1 LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut

package File::Temp;
# ABSTRACT: return name and handle of a temporary file safely
our $VERSION = '0.2304'; # VERSION


# Toolchain targets v5.8.1, but we'll try to support back to v5.6 anyway.
# It might be possible to make this v5.5, but many v5.6isms are creeping
# into the code and tests.
use 5.006;
use strict;
use Carp;
use File::Spec 0.8;
use Cwd ();
use File::Path 2.06 qw/ rmtree /;
use Fcntl 1.03;
use IO::Seekable;               # For SEEK_*
use Errno;
use Scalar::Util 'refaddr';
require VMS::Stdio if $^O eq 'VMS';

# pre-emptively load Carp::Heavy. If we don't when we run out of file
# handles and attempt to call croak() we get an error message telling
# us that Carp::Heavy won't load rather than an error telling us we
# have run out of file handles. We either preload croak() or we
# switch the calls to croak from _gettemp() to use die.
eval { require Carp::Heavy; };

# Need the Symbol package if we are running older perl
require Symbol if $] < 5.006;

### For the OO interface
use parent 0.221 qw/ IO::Handle IO::Seekable /;
use overload '""' => "STRINGIFY", '0+' => "NUMIFY",
  fallback => 1;

# use 'our' on v5.6.0
use vars qw(@EXPORT_OK %EXPORT_TAGS $DEBUG $KEEP_ALL);

$DEBUG = 0;
$KEEP_ALL = 0;

# We are exporting functions

use Exporter 5.57 'import';   # 5.57 lets us import 'import'

# Export list - to allow fine tuning of export table

@EXPORT_OK = qw{
                 tempfile
                 tempdir
                 tmpnam
                 tmpfile
                 mktemp
                 mkstemp
                 mkstemps
                 mkdtemp
                 unlink0
                 cleanup
                 SEEK_SET
                 SEEK_CUR
                 SEEK_END
             };

# Groups of functions for export

%EXPORT_TAGS = (
                'POSIX' => [qw/ tmpnam tmpfile /],
                'mktemp' => [qw/ mktemp mkstemp mkstemps mkdtemp/],
                'seekable' => [qw/ SEEK_SET SEEK_CUR SEEK_END /],
               );

# add contents of these tags to @EXPORT
Exporter::export_tags('POSIX','mktemp','seekable');

# This is a list of characters that can be used in random filenames

my @CHARS = (qw/ A B C D E F G H I J K L M N O P Q R S T U V W X Y Z
                 a b c d e f g h i j k l m n o p q r s t u v w x y z
                 0 1 2 3 4 5 6 7 8 9 _
               /);

# Maximum number of tries to make a temp file before failing

use constant MAX_TRIES => 1000;

# Minimum number of X characters that should be in a template
use constant MINX => 4;

# Default template when no template supplied

use constant TEMPXXX => 'X' x 10;

# Constants for the security level

use constant STANDARD => 0;
use constant MEDIUM   => 1;
use constant HIGH     => 2;

# OPENFLAGS. If we defined the flag to use with Sysopen here this gives
# us an optimisation when many temporary files are requested

my $OPENFLAGS = O_CREAT | O_EXCL | O_RDWR;
my $LOCKFLAG;

unless ($^O eq 'MacOS') {
  for my $oflag (qw/ NOFOLLOW BINARY LARGEFILE NOINHERIT /) {
    my ($bit, $func) = (0, "Fcntl::O_" . $oflag);
    no strict 'refs';
    $OPENFLAGS |= $bit if eval {
      # Make sure that redefined die handlers do not cause problems
      # e.g. CGI::Carp
      local $SIG{__DIE__} = sub {};
      local $SIG{__WARN__} = sub {};
      $bit = &$func();
      1;
    };
  }
  # Special case O_EXLOCK
  $LOCKFLAG = eval {
    local $SIG{__DIE__} = sub {};
    local $SIG{__WARN__} = sub {};
    &Fcntl::O_EXLOCK();
  };
}

# On some systems the O_TEMPORARY flag can be used to tell the OS
# to automatically remove the file when it is closed. This is fine
# in most cases but not if tempfile is called with UNLINK=>0 and
# the filename is requested -- in the case where the filename is to
# be passed to another routine. This happens on windows. We overcome
# this by using a second open flags variable

my $OPENTEMPFLAGS = $OPENFLAGS;
unless ($^O eq 'MacOS') {
  for my $oflag (qw/ TEMPORARY /) {
    my ($bit, $func) = (0, "Fcntl::O_" . $oflag);
    local($@);
    no strict 'refs';
    $OPENTEMPFLAGS |= $bit if eval {
      # Make sure that redefined die handlers do not cause problems
      # e.g. CGI::Carp
      local $SIG{__DIE__} = sub {};
      local $SIG{__WARN__} = sub {};
      $bit = &$func();
      1;
    };
  }
}

# Private hash tracking which files have been created by each process id via the OO interface
my %FILES_CREATED_BY_OBJECT;

# INTERNAL ROUTINES - not to be used outside of package

# Generic routine for getting a temporary filename
# modelled on OpenBSD _gettemp() in mktemp.c

# The template must contain X's that are to be replaced
# with the random values

#  Arguments:

#  TEMPLATE   - string containing the XXXXX's that is converted
#           to a random filename and opened if required

# Optionally, a hash can also be supplied containing specific options
#   "open" => if true open the temp file, else just return the name
#             default is 0
#   "mkdir"=> if true, we are creating a temp directory rather than tempfile
#             default is 0
#   "suffixlen" => number of characters at end of PATH to be ignored.
#                  default is 0.
#   "unlink_on_close" => indicates that, if possible,  the OS should remove
#                        the file as soon as it is closed. Usually indicates
#                        use of the O_TEMPORARY flag to sysopen.
#                        Usually irrelevant on unix
#   "use_exlock" => Indicates that O_EXLOCK should be used. Default is true.

# Optionally a reference to a scalar can be passed into the function
# On error this will be used to store the reason for the error
#   "ErrStr"  => \$errstr

# "open" and "mkdir" can not both be true
# "unlink_on_close" is not used when "mkdir" is true.

# The default options are equivalent to mktemp().

# Returns:
#   filehandle - open file handle (if called with doopen=1, else undef)
#   temp name  - name of the temp file or directory

# For example:
#   ($fh, $name) = _gettemp($template, "open" => 1);

# for the current version, failures are associated with
# stored in an error string and returned to give the reason whilst debugging
# This routine is not called by any external function
sub _gettemp {

  croak 'Usage: ($fh, $name) = _gettemp($template, OPTIONS);'
    unless scalar(@_) >= 1;

  # the internal error string - expect it to be overridden
  # Need this in case the caller decides not to supply us a value
  # need an anonymous scalar
  my $tempErrStr;

  # Default options
  my %options = (
                 "open" => 0,
                 "mkdir" => 0,
                 "suffixlen" => 0,
                 "unlink_on_close" => 0,
                 "use_exlock" => 1,
                 "ErrStr" => \$tempErrStr,
                );

  # Read the template
  my $template = shift;
  if (ref($template)) {
    # Use a warning here since we have not yet merged ErrStr
    carp "File::Temp::_gettemp: template must not be a reference";
    return ();
  }

  # Check that the number of entries on stack are even
  if (scalar(@_) % 2 != 0) {
    # Use a warning here since we have not yet merged ErrStr
    carp "File::Temp::_gettemp: Must have even number of options";
    return ();
  }

  # Read the options and merge with defaults
  %options = (%options, @_)  if @_;

  # Make sure the error string is set to undef
  ${$options{ErrStr}} = undef;

  # Can not open the file and make a directory in a single call
  if ($options{"open"} && $options{"mkdir"}) {
    ${$options{ErrStr}} = "doopen and domkdir can not both be true\n";
    return ();
  }

  # Find the start of the end of the  Xs (position of last X)
  # Substr starts from 0
  my $start = length($template) - 1 - $options{"suffixlen"};

  # Check that we have at least MINX x X (e.g. 'XXXX") at the end of the string
  # (taking suffixlen into account). Any fewer is insecure.

  # Do it using substr - no reason to use a pattern match since
  # we know where we are looking and what we are looking for

  if (substr($template, $start - MINX + 1, MINX) ne 'X' x MINX) {
    ${$options{ErrStr}} = "The template must end with at least ".
      MINX . " 'X' characters\n";
    return ();
  }

  # Replace all the X at the end of the substring with a
  # random character or just all the XX at the end of a full string.
  # Do it as an if, since the suffix adjusts which section to replace
  # and suffixlen=0 returns nothing if used in the substr directly
  # and generate a full path from the template

  my $path = _replace_XX($template, $options{"suffixlen"});


  # Split the path into constituent parts - eventually we need to check
  # whether the directory exists
  # We need to know whether we are making a temp directory
  # or a tempfile

  my ($volume, $directories, $file);
  my $parent;                   # parent directory
  if ($options{"mkdir"}) {
    # There is no filename at the end
    ($volume, $directories, $file) = File::Spec->splitpath( $path, 1);

    # The parent is then $directories without the last directory
    # Split the directory and put it back together again
    my @dirs = File::Spec->splitdir($directories);

    # If @dirs only has one entry (i.e. the directory template) that means
    # we are in the current directory
    if ($#dirs == 0) {
      $parent = File::Spec->curdir;
    } else {

      if ($^O eq 'VMS') {     # need volume to avoid relative dir spec
        $parent = File::Spec->catdir($volume, @dirs[0..$#dirs-1]);
        $parent = 'sys$disk:[]' if $parent eq '';
      } else {

        # Put it back together without the last one
        $parent = File::Spec->catdir(@dirs[0..$#dirs-1]);

        # ...and attach the volume (no filename)
        $parent = File::Spec->catpath($volume, $parent, '');
      }

    }

  } else {

    # Get rid of the last filename (use File::Basename for this?)
    ($volume, $directories, $file) = File::Spec->splitpath( $path );

    # Join up without the file part
    $parent = File::Spec->catpath($volume,$directories,'');

    # If $parent is empty replace with curdir
    $parent = File::Spec->curdir
      unless $directories ne '';

  }

  # Check that the parent directories exist
  # Do this even for the case where we are simply returning a name
  # not a file -- no point returning a name that includes a directory
  # that does not exist or is not writable

  unless (-e $parent) {
    ${$options{ErrStr}} = "Parent directory ($parent) does not exist";
    return ();
  }
  unless (-d $parent) {
    ${$options{ErrStr}} = "Parent directory ($parent) is not a directory";
    return ();
  }

  # Check the stickiness of the directory and chown giveaway if required
  # If the directory is world writable the sticky bit
  # must be set

  if (File::Temp->safe_level == MEDIUM) {
    my $safeerr;
    unless (_is_safe($parent,\$safeerr)) {
      ${$options{ErrStr}} = "Parent directory ($parent) is not safe ($safeerr)";
      return ();
    }
  } elsif (File::Temp->safe_level == HIGH) {
    my $safeerr;
    unless (_is_verysafe($parent, \$safeerr)) {
      ${$options{ErrStr}} = "Parent directory ($parent) is not safe ($safeerr)";
      return ();
    }
  }


  # Now try MAX_TRIES time to open the file
  for (my $i = 0; $i < MAX_TRIES; $i++) {

    # Try to open the file if requested
    if ($options{"open"}) {
      my $fh;

      # If we are running before perl5.6.0 we can not auto-vivify
      if ($] < 5.006) {
        $fh = &Symbol::gensym;
      }

      # Try to make sure this will be marked close-on-exec
      # XXX: Win32 doesn't respect this, nor the proper fcntl,
      #      but may have O_NOINHERIT. This may or may not be in Fcntl.
      local $^F = 2;

      # Attempt to open the file
      my $open_success = undef;
      if ( $^O eq 'VMS' and $options{"unlink_on_close"} && !$KEEP_ALL) {
        # make it auto delete on close by setting FAB$V_DLT bit
        $fh = VMS::Stdio::vmssysopen($path, $OPENFLAGS, 0600, 'fop=dlt');
        $open_success = $fh;
      } else {
        my $flags = ( ($options{"unlink_on_close"} && !$KEEP_ALL) ?
                      $OPENTEMPFLAGS :
                      $OPENFLAGS );
        $flags |= $LOCKFLAG if (defined $LOCKFLAG && $options{use_exlock});
        $open_success = sysopen($fh, $path, $flags, 0600);
      }
      if ( $open_success ) {

        # in case of odd umask force rw
        chmod(0600, $path);

        # Opened successfully - return file handle and name
        return ($fh, $path);

      } else {

        # Error opening file - abort with error
        # if the reason was anything but EEXIST
        unless ($!{EEXIST}) {
          ${$options{ErrStr}} = "Could not create temp file $path: $!";
          return ();
        }

        # Loop round for another try

      }
    } elsif ($options{"mkdir"}) {

      # Open the temp directory
      if (mkdir( $path, 0700)) {
        # in case of odd umask
        chmod(0700, $path);

        return undef, $path;
      } else {

        # Abort with error if the reason for failure was anything
        # except EEXIST
        unless ($!{EEXIST}) {
          ${$options{ErrStr}} = "Could not create directory $path: $!";
          return ();
        }

        # Loop round for another try

      }

    } else {

      # Return true if the file can not be found
      # Directory has been checked previously

      return (undef, $path) unless -e $path;

      # Try again until MAX_TRIES

    }

    # Did not successfully open the tempfile/dir
    # so try again with a different set of random letters
    # No point in trying to increment unless we have only
    # 1 X say and the randomness could come up with the same
    # file MAX_TRIES in a row.

    # Store current attempt - in principal this implies that the
    # 3rd time around the open attempt that the first temp file
    # name could be generated again. Probably should store each
    # attempt and make sure that none are repeated

    my $original = $path;
    my $counter = 0;            # Stop infinite loop
    my $MAX_GUESS = 50;

    do {

      # Generate new name from original template
      $path = _replace_XX($template, $options{"suffixlen"});

      $counter++;

    } until ($path ne $original || $counter > $MAX_GUESS);

    # Check for out of control looping
    if ($counter > $MAX_GUESS) {
      ${$options{ErrStr}} = "Tried to get a new temp name different to the previous value $MAX_GUESS times.\nSomething wrong with template?? ($template)";
      return ();
    }

  }

  # If we get here, we have run out of tries
  ${ $options{ErrStr} } = "Have exceeded the maximum number of attempts ("
    . MAX_TRIES . ") to open temp file/dir";

  return ();

}

# Internal routine to replace the XXXX... with random characters
# This has to be done by _gettemp() every time it fails to
# open a temp file/dir

# Arguments:  $template (the template with XXX),
#             $ignore   (number of characters at end to ignore)

# Returns:    modified template

sub _replace_XX {

  croak 'Usage: _replace_XX($template, $ignore)'
    unless scalar(@_) == 2;

  my ($path, $ignore) = @_;

  # Do it as an if, since the suffix adjusts which section to replace
  # and suffixlen=0 returns nothing if used in the substr directly
  # Alternatively, could simply set $ignore to length($path)-1
  # Don't want to always use substr when not required though.
  my $end = ( $] >= 5.006 ? "\\z" : "\\Z" );

  if ($ignore) {
    substr($path, 0, - $ignore) =~ s/X(?=X*$end)/$CHARS[ int( rand( @CHARS ) ) ]/ge;
  } else {
    $path =~ s/X(?=X*$end)/$CHARS[ int( rand( @CHARS ) ) ]/ge;
  }
  return $path;
}

# Internal routine to force a temp file to be writable after
# it is created so that we can unlink it. Windows seems to occasionally
# force a file to be readonly when written to certain temp locations
sub _force_writable {
  my $file = shift;
  chmod 0600, $file;
}


# internal routine to check to see if the directory is safe
# First checks to see if the directory is not owned by the
# current user or root. Then checks to see if anyone else
# can write to the directory and if so, checks to see if
# it has the sticky bit set

# Will not work on systems that do not support sticky bit

#Args:  directory path to check
#       Optionally: reference to scalar to contain error message
# Returns true if the path is safe and false otherwise.
# Returns undef if can not even run stat() on the path

# This routine based on version written by Tom Christiansen

# Presumably, by the time we actually attempt to create the
# file or directory in this directory, it may not be safe
# anymore... Have to run _is_safe directly after the open.

sub _is_safe {

  my $path = shift;
  my $err_ref = shift;

  # Stat path
  my @info = stat($path);
  unless (scalar(@info)) {
    $$err_ref = "stat(path) returned no values";
    return 0;
  }
  ;
  return 1 if $^O eq 'VMS';     # owner delete control at file level

  # Check to see whether owner is neither superuser (or a system uid) nor me
  # Use the effective uid from the $> variable
  # UID is in [4]
  if ($info[4] > File::Temp->top_system_uid() && $info[4] != $>) {

    Carp::cluck(sprintf "uid=$info[4] topuid=%s euid=$> path='$path'",
                File::Temp->top_system_uid());

    $$err_ref = "Directory owned neither by root nor the current user"
      if ref($err_ref);
    return 0;
  }

  # check whether group or other can write file
  # use 066 to detect either reading or writing
  # use 022 to check writability
  # Do it with S_IWOTH and S_IWGRP for portability (maybe)
  # mode is in info[2]
  if (($info[2] & &Fcntl::S_IWGRP) ||  # Is group writable?
      ($info[2] & &Fcntl::S_IWOTH) ) { # Is world writable?
    # Must be a directory
    unless (-d $path) {
      $$err_ref = "Path ($path) is not a directory"
        if ref($err_ref);
      return 0;
    }
    # Must have sticky bit set
    unless (-k $path) {
      $$err_ref = "Sticky bit not set on $path when dir is group|world writable"
        if ref($err_ref);
      return 0;
    }
  }

  return 1;
}

# Internal routine to check whether a directory is safe
# for temp files. Safer than _is_safe since it checks for
# the possibility of chown giveaway and if that is a possibility
# checks each directory in the path to see if it is safe (with _is_safe)

# If _PC_CHOWN_RESTRICTED is not set, does the full test of each
# directory anyway.

# Takes optional second arg as scalar ref to error reason

sub _is_verysafe {

  # Need POSIX - but only want to bother if really necessary due to overhead
  require POSIX;

  my $path = shift;
  print "_is_verysafe testing $path\n" if $DEBUG;
  return 1 if $^O eq 'VMS';     # owner delete control at file level

  my $err_ref = shift;

  # Should Get the value of _PC_CHOWN_RESTRICTED if it is defined
  # and If it is not there do the extensive test
  local($@);
  my $chown_restricted;
  $chown_restricted = &POSIX::_PC_CHOWN_RESTRICTED()
    if eval { &POSIX::_PC_CHOWN_RESTRICTED(); 1};

  # If chown_resticted is set to some value we should test it
  if (defined $chown_restricted) {

    # Return if the current directory is safe
    return _is_safe($path,$err_ref) if POSIX::sysconf( $chown_restricted );

  }

  # To reach this point either, the _PC_CHOWN_RESTRICTED symbol
  # was not available or the symbol was there but chown giveaway
  # is allowed. Either way, we now have to test the entire tree for
  # safety.

  # Convert path to an absolute directory if required
  unless (File::Spec->file_name_is_absolute($path)) {
    $path = File::Spec->rel2abs($path);
  }

  # Split directory into components - assume no file
  my ($volume, $directories, undef) = File::Spec->splitpath( $path, 1);

  # Slightly less efficient than having a function in File::Spec
  # to chop off the end of a directory or even a function that
  # can handle ../ in a directory tree
  # Sometimes splitdir() returns a blank at the end
  # so we will probably check the bottom directory twice in some cases
  my @dirs = File::Spec->splitdir($directories);

  # Concatenate one less directory each time around
  foreach my $pos (0.. $#dirs) {
    # Get a directory name
    my $dir = File::Spec->catpath($volume,
                                  File::Spec->catdir(@dirs[0.. $#dirs - $pos]),
                                  ''
                                 );

    print "TESTING DIR $dir\n" if $DEBUG;

    # Check the directory
    return 0 unless _is_safe($dir,$err_ref);

  }

  return 1;
}



# internal routine to determine whether unlink works on this
# platform for files that are currently open.
# Returns true if we can, false otherwise.

# Currently WinNT, OS/2 and VMS can not unlink an opened file
# On VMS this is because the O_EXCL flag is used to open the
# temporary file. Currently I do not know enough about the issues
# on VMS to decide whether O_EXCL is a requirement.

sub _can_unlink_opened_file {

  if (grep { $^O eq $_ } qw/MSWin32 os2 VMS dos MacOS haiku/) {
    return 0;
  } else {
    return 1;
  }

}

# internal routine to decide which security levels are allowed
# see safe_level() for more information on this

# Controls whether the supplied security level is allowed

#   $cando = _can_do_level( $level )

sub _can_do_level {

  # Get security level
  my $level = shift;

  # Always have to be able to do STANDARD
  return 1 if $level == STANDARD;

  # Currently, the systems that can do HIGH or MEDIUM are identical
  if ( $^O eq 'MSWin32' || $^O eq 'os2' || $^O eq 'cygwin' || $^O eq 'dos' || $^O eq 'MacOS' || $^O eq 'mpeix') {
    return 0;
  } else {
    return 1;
  }

}

# This routine sets up a deferred unlinking of a specified
# filename and filehandle. It is used in the following cases:
#  - Called by unlink0 if an opened file can not be unlinked
#  - Called by tempfile() if files are to be removed on shutdown
#  - Called by tempdir() if directories are to be removed on shutdown

# Arguments:
#   _deferred_unlink( $fh, $fname, $isdir );
#
#   - filehandle (so that it can be explicitly closed if open
#   - filename   (the thing we want to remove)
#   - isdir      (flag to indicate that we are being given a directory)
#                 [and hence no filehandle]

# Status is not referred to since all the magic is done with an END block

{
  # Will set up two lexical variables to contain all the files to be
  # removed. One array for files, another for directories They will
  # only exist in this block.

  #  This means we only have to set up a single END block to remove
  #  all files. 

  # in order to prevent child processes inadvertently deleting the parent
  # temp files we use a hash to store the temp files and directories
  # created by a particular process id.

  # %files_to_unlink contains values that are references to an array of
  # array references containing the filehandle and filename associated with
  # the temp file.
  my (%files_to_unlink, %dirs_to_unlink);

  # Set up an end block to use these arrays
  END {
    local($., $@, $!, $^E, $?);
    cleanup(at_exit => 1);
  }

  # Cleanup function. Always triggered on END (with at_exit => 1) but
  # can be invoked manually.
  sub cleanup {
    my %h = @_;
    my $at_exit = delete $h{at_exit};
    $at_exit = 0 if not defined $at_exit;
    { my @k = sort keys %h; die "unrecognized parameters: @k" if @k }

    if (!$KEEP_ALL) {
      # Files
      my @files = (exists $files_to_unlink{$$} ?
                   @{ $files_to_unlink{$$} } : () );
      foreach my $file (@files) {
        # close the filehandle without checking its state
        # in order to make real sure that this is closed
        # if its already closed then I don't care about the answer
        # probably a better way to do this
        close($file->[0]);      # file handle is [0]

        if (-f $file->[1]) {       # file name is [1]
          _force_writable( $file->[1] ); # for windows
          unlink $file->[1] or warn "Error removing ".$file->[1];
        }
      }
      # Dirs
      my @dirs = (exists $dirs_to_unlink{$$} ?
                  @{ $dirs_to_unlink{$$} } : () );
      my ($cwd, $cwd_to_remove);
      foreach my $dir (@dirs) {
        if (-d $dir) {
          # Some versions of rmtree will abort if you attempt to remove
          # the directory you are sitting in. For automatic cleanup
          # at program exit, we avoid this by chdir()ing out of the way
          # first. If not at program exit, it's best not to mess with the
          # current directory, so just let it fail with a warning.
          if ($at_exit) {
            $cwd = Cwd::abs_path(File::Spec->curdir) if not defined $cwd;
            my $abs = Cwd::abs_path($dir);
            if ($abs eq $cwd) {
              $cwd_to_remove = $dir;
              next;
            }
          }
          eval { rmtree($dir, $DEBUG, 0); };
          warn $@ if ($@ && $^W);
        }
      }

      if (defined $cwd_to_remove) {
        # We do need to clean up the current directory, and everything
        # else is done, so get out of there and remove it.
        chdir $cwd_to_remove or die "cannot chdir to $cwd_to_remove: $!";
        my $updir = File::Spec->updir;
        chdir $updir or die "cannot chdir to $updir: $!";
        eval { rmtree($cwd_to_remove, $DEBUG, 0); };
        warn $@ if ($@ && $^W);
      }

      # clear the arrays
      @{ $files_to_unlink{$$} } = ()
        if exists $files_to_unlink{$$};
      @{ $dirs_to_unlink{$$} } = ()
        if exists $dirs_to_unlink{$$};
    }
  }


  # This is the sub called to register a file for deferred unlinking
  # This could simply store the input parameters and defer everything
  # until the END block. For now we do a bit of checking at this
  # point in order to make sure that (1) we have a file/dir to delete
  # and (2) we have been called with the correct arguments.
  sub _deferred_unlink {

    croak 'Usage:  _deferred_unlink($fh, $fname, $isdir)'
      unless scalar(@_) == 3;

    my ($fh, $fname, $isdir) = @_;

    warn "Setting up deferred removal of $fname\n"
      if $DEBUG;

    # make sure we save the absolute path for later cleanup
    # OK to untaint because we only ever use this internally
    # as a file path, never interpolating into the shell
    $fname = Cwd::abs_path($fname);
    ($fname) = $fname =~ /^(.*)$/;

    # If we have a directory, check that it is a directory
    if ($isdir) {

      if (-d $fname) {

        # Directory exists so store it
        # first on VMS turn []foo into [.foo] for rmtree
        $fname = VMS::Filespec::vmspath($fname) if $^O eq 'VMS';
        $dirs_to_unlink{$$} = [] 
          unless exists $dirs_to_unlink{$$};
        push (@{ $dirs_to_unlink{$$} }, $fname);

      } else {
        carp "Request to remove directory $fname could not be completed since it does not exist!\n" if $^W;
      }

    } else {

      if (-f $fname) {

        # file exists so store handle and name for later removal
        $files_to_unlink{$$} = []
          unless exists $files_to_unlink{$$};
        push(@{ $files_to_unlink{$$} }, [$fh, $fname]);

      } else {
        carp "Request to remove file $fname could not be completed since it is not there!\n" if $^W;
      }

    }

  }


}

# normalize argument keys to upper case and do consistent handling
# of leading template vs TEMPLATE
sub _parse_args {
  my $leading_template = (scalar(@_) % 2 == 1 ? shift(@_) : '' );
  my %args = @_;
  %args = map { uc($_), $args{$_} } keys %args;

  # template (store it in an array so that it will
  # disappear from the arg list of tempfile)
  my @template = (
    exists $args{TEMPLATE}  ? $args{TEMPLATE} :
    $leading_template       ? $leading_template : ()
  );
  delete $args{TEMPLATE};

  return( \@template, \%args );
}


sub new {
  my $proto = shift;
  my $class = ref($proto) || $proto;

  my ($maybe_template, $args) = _parse_args(@_);

  # see if they are unlinking (defaulting to yes)
  my $unlink = (exists $args->{UNLINK} ? $args->{UNLINK} : 1 );
  delete $args->{UNLINK};

  # Protect OPEN
  delete $args->{OPEN};

  # Open the file and retain file handle and file name
  my ($fh, $path) = tempfile( @$maybe_template, %$args );

  print "Tmp: $fh - $path\n" if $DEBUG;

  # Store the filename in the scalar slot
  ${*$fh} = $path;

  # Cache the filename by pid so that the destructor can decide whether to remove it
  $FILES_CREATED_BY_OBJECT{$$}{$path} = 1;

  # Store unlink information in hash slot (plus other constructor info)
  %{*$fh} = %$args;

  # create the object
  bless $fh, $class;

  # final method-based configuration
  $fh->unlink_on_destroy( $unlink );

  return $fh;
}


sub newdir {
  my $self = shift;

  my ($maybe_template, $args) = _parse_args(@_);

  # handle CLEANUP without passing CLEANUP to tempdir
  my $cleanup = (exists $args->{CLEANUP} ? $args->{CLEANUP} : 1 );
  delete $args->{CLEANUP};

  my $tempdir = tempdir( @$maybe_template, %$args);

  # get a safe absolute path for cleanup, just like
  # happens in _deferred_unlink
  my $real_dir = Cwd::abs_path( $tempdir );
  ($real_dir) = $real_dir =~ /^(.*)$/;

  return bless { DIRNAME => $tempdir,
                 REALNAME => $real_dir,
                 CLEANUP => $cleanup,
                 LAUNCHPID => $$,
               }, "File::Temp::Dir";
}


sub filename {
  my $self = shift;
  return ${*$self};
}

sub STRINGIFY {
  my $self = shift;
  return $self->filename;
}

# For reference, can't use '0+'=>\&Scalar::Util::refaddr directly because
# refaddr() demands one parameter only, whereas overload.pm calls with three
# even for unary operations like '0+'.
sub NUMIFY {
  return refaddr($_[0]);
}


sub unlink_on_destroy {
  my $self = shift;
  if (@_) {
    ${*$self}{UNLINK} = shift;
  }
  return ${*$self}{UNLINK};
}


sub DESTROY {
  local($., $@, $!, $^E, $?);
  my $self = shift;

  # Make sure we always remove the file from the global hash
  # on destruction. This prevents the hash from growing uncontrollably
  # and post-destruction there is no reason to know about the file.
  my $file = $self->filename;
  my $was_created_by_proc;
  if (exists $FILES_CREATED_BY_OBJECT{$$}{$file}) {
    $was_created_by_proc = 1;
    delete $FILES_CREATED_BY_OBJECT{$$}{$file};
  }

  if (${*$self}{UNLINK} && !$KEEP_ALL) {
    print "# --------->   Unlinking $self\n" if $DEBUG;

    # only delete if this process created it
    return unless $was_created_by_proc;

    # The unlink1 may fail if the file has been closed
    # by the caller. This leaves us with the decision
    # of whether to refuse to remove the file or simply
    # do an unlink without test. Seems to be silly
    # to do this when we are trying to be careful
    # about security
    _force_writable( $file ); # for windows
    unlink1( $self, $file )
      or unlink($file);
  }
}


sub tempfile {
  if ( @_ && $_[0] eq 'File::Temp' ) {
      croak "'tempfile' can't be called as a method";
  }
  # Can not check for argument count since we can have any
  # number of args

  # Default options
  my %options = (
                 "DIR"    => undef, # Directory prefix
                 "SUFFIX" => '',    # Template suffix
                 "UNLINK" => 0,     # Do not unlink file on exit
                 "OPEN"   => 1,     # Open file
                 "TMPDIR" => 0, # Place tempfile in tempdir if template specified
                 "EXLOCK" => 1, # Open file with O_EXLOCK
                );

  # Check to see whether we have an odd or even number of arguments
  my ($maybe_template, $args) = _parse_args(@_);
  my $template = @$maybe_template ? $maybe_template->[0] : undef;

  # Read the options and merge with defaults
  %options = (%options, %$args);

  # First decision is whether or not to open the file
  if (! $options{"OPEN"}) {

    warn "tempfile(): temporary filename requested but not opened.\nPossibly unsafe, consider using tempfile() with OPEN set to true\n"
      if $^W;

  }

  if ($options{"DIR"} and $^O eq 'VMS') {

    # on VMS turn []foo into [.foo] for concatenation
    $options{"DIR"} = VMS::Filespec::vmspath($options{"DIR"});
  }

  # Construct the template

  # Have a choice of trying to work around the mkstemp/mktemp/tmpnam etc
  # functions or simply constructing a template and using _gettemp()
  # explicitly. Go for the latter

  # First generate a template if not defined and prefix the directory
  # If no template must prefix the temp directory
  if (defined $template) {
    # End up with current directory if neither DIR not TMPDIR are set
    if ($options{"DIR"}) {

      $template = File::Spec->catfile($options{"DIR"}, $template);

    } elsif ($options{TMPDIR}) {

      $template = File::Spec->catfile(File::Spec->tmpdir, $template );

    }

  } else {

    if ($options{"DIR"}) {

      $template = File::Spec->catfile($options{"DIR"}, TEMPXXX);

    } else {

      $template = File::Spec->catfile(File::Spec->tmpdir, TEMPXXX);

    }

  }

  # Now add a suffix
  $template .= $options{"SUFFIX"};

  # Determine whether we should tell _gettemp to unlink the file
  # On unix this is irrelevant and can be worked out after the file is
  # opened (simply by unlinking the open filehandle). On Windows or VMS
  # we have to indicate temporary-ness when we open the file. In general
  # we only want a true temporary file if we are returning just the
  # filehandle - if the user wants the filename they probably do not
  # want the file to disappear as soon as they close it (which may be
  # important if they want a child process to use the file)
  # For this reason, tie unlink_on_close to the return context regardless
  # of OS.
  my $unlink_on_close = ( wantarray ? 0 : 1);

  # Create the file
  my ($fh, $path, $errstr);
  croak "Error in tempfile() using template $template: $errstr"
    unless (($fh, $path) = _gettemp($template,
                                    "open" => $options{'OPEN'},
                                    "mkdir"=> 0 ,
                                    "unlink_on_close" => $unlink_on_close,
                                    "suffixlen" => length($options{'SUFFIX'}),
                                    "ErrStr" => \$errstr,
                                    "use_exlock" => $options{EXLOCK},
                                   ) );

  # Set up an exit handler that can do whatever is right for the
  # system. This removes files at exit when requested explicitly or when
  # system is asked to unlink_on_close but is unable to do so because
  # of OS limitations.
  # The latter should be achieved by using a tied filehandle.
  # Do not check return status since this is all done with END blocks.
  _deferred_unlink($fh, $path, 0) if $options{"UNLINK"};

  # Return
  if (wantarray()) {

    if ($options{'OPEN'}) {
      return ($fh, $path);
    } else {
      return (undef, $path);
    }

  } else {

    # Unlink the file. It is up to unlink0 to decide what to do with
    # this (whether to unlink now or to defer until later)
    unlink0($fh, $path) or croak "Error unlinking file $path using unlink0";

    # Return just the filehandle.
    return $fh;
  }


}


# '

sub tempdir  {
  if ( @_ && $_[0] eq 'File::Temp' ) {
      croak "'tempdir' can't be called as a method";
  }

  # Can not check for argument count since we can have any
  # number of args

  # Default options
  my %options = (
                 "CLEANUP"    => 0, # Remove directory on exit
                 "DIR"        => '', # Root directory
                 "TMPDIR"     => 0,  # Use tempdir with template
                );

  # Check to see whether we have an odd or even number of arguments
  my ($maybe_template, $args) = _parse_args(@_);
  my $template = @$maybe_template ? $maybe_template->[0] : undef;

  # Read the options and merge with defaults
  %options = (%options, %$args);

  # Modify or generate the template

  # Deal with the DIR and TMPDIR options
  if (defined $template) {

    # Need to strip directory path if using DIR or TMPDIR
    if ($options{'TMPDIR'} || $options{'DIR'}) {

      # Strip parent directory from the filename
      #
      # There is no filename at the end
      $template = VMS::Filespec::vmspath($template) if $^O eq 'VMS';
      my ($volume, $directories, undef) = File::Spec->splitpath( $template, 1);

      # Last directory is then our template
      $template = (File::Spec->splitdir($directories))[-1];

      # Prepend the supplied directory or temp dir
      if ($options{"DIR"}) {

        $template = File::Spec->catdir($options{"DIR"}, $template);

      } elsif ($options{TMPDIR}) {

        # Prepend tmpdir
        $template = File::Spec->catdir(File::Spec->tmpdir, $template);

      }

    }

  } else {

    if ($options{"DIR"}) {

      $template = File::Spec->catdir($options{"DIR"}, TEMPXXX);

    } else {

      $template = File::Spec->catdir(File::Spec->tmpdir, TEMPXXX);

    }

  }

  # Create the directory
  my $tempdir;
  my $suffixlen = 0;
  if ($^O eq 'VMS') {           # dir names can end in delimiters
    $template =~ m/([\.\]:>]+)$/;
    $suffixlen = length($1);
  }
  if ( ($^O eq 'MacOS') && (substr($template, -1) eq ':') ) {
    # dir name has a trailing ':'
    ++$suffixlen;
  }

  my $errstr;
  croak "Error in tempdir() using $template: $errstr"
    unless ((undef, $tempdir) = _gettemp($template,
                                         "open" => 0,
                                         "mkdir"=> 1 ,
                                         "suffixlen" => $suffixlen,
                                         "ErrStr" => \$errstr,
                                        ) );

  # Install exit handler; must be dynamic to get lexical
  if ( $options{'CLEANUP'} && -d $tempdir) {
    _deferred_unlink(undef, $tempdir, 1);
  }

  # Return the dir name
  return $tempdir;

}




sub mkstemp {

  croak "Usage: mkstemp(template)"
    if scalar(@_) != 1;

  my $template = shift;

  my ($fh, $path, $errstr);
  croak "Error in mkstemp using $template: $errstr"
    unless (($fh, $path) = _gettemp($template,
                                    "open" => 1,
                                    "mkdir"=> 0 ,
                                    "suffixlen" => 0,
                                    "ErrStr" => \$errstr,
                                   ) );

  if (wantarray()) {
    return ($fh, $path);
  } else {
    return $fh;
  }

}



sub mkstemps {

  croak "Usage: mkstemps(template, suffix)"
    if scalar(@_) != 2;


  my $template = shift;
  my $suffix   = shift;

  $template .= $suffix;

  my ($fh, $path, $errstr);
  croak "Error in mkstemps using $template: $errstr"
    unless (($fh, $path) = _gettemp($template,
                                    "open" => 1,
                                    "mkdir"=> 0 ,
                                    "suffixlen" => length($suffix),
                                    "ErrStr" => \$errstr,
                                   ) );

  if (wantarray()) {
    return ($fh, $path);
  } else {
    return $fh;
  }

}


#' # for emacs

sub mkdtemp {

  croak "Usage: mkdtemp(template)"
    if scalar(@_) != 1;

  my $template = shift;
  my $suffixlen = 0;
  if ($^O eq 'VMS') {           # dir names can end in delimiters
    $template =~ m/([\.\]:>]+)$/;
    $suffixlen = length($1);
  }
  if ( ($^O eq 'MacOS') && (substr($template, -1) eq ':') ) {
    # dir name has a trailing ':'
    ++$suffixlen;
  }
  my ($junk, $tmpdir, $errstr);
  croak "Error creating temp directory from template $template\: $errstr"
    unless (($junk, $tmpdir) = _gettemp($template,
                                        "open" => 0,
                                        "mkdir"=> 1 ,
                                        "suffixlen" => $suffixlen,
                                        "ErrStr" => \$errstr,
                                       ) );

  return $tmpdir;

}


sub mktemp {

  croak "Usage: mktemp(template)"
    if scalar(@_) != 1;

  my $template = shift;

  my ($tmpname, $junk, $errstr);
  croak "Error getting name to temp file from template $template: $errstr"
    unless (($junk, $tmpname) = _gettemp($template,
                                         "open" => 0,
                                         "mkdir"=> 0 ,
                                         "suffixlen" => 0,
                                         "ErrStr" => \$errstr,
                                        ) );

  return $tmpname;
}


sub tmpnam {

  # Retrieve the temporary directory name
  my $tmpdir = File::Spec->tmpdir;

  croak "Error temporary directory is not writable"
    if $tmpdir eq '';

  # Use a ten character template and append to tmpdir
  my $template = File::Spec->catfile($tmpdir, TEMPXXX);

  if (wantarray() ) {
    return mkstemp($template);
  } else {
    return mktemp($template);
  }

}


sub tmpfile {

  # Simply call tmpnam() in a list context
  my ($fh, $file) = tmpnam();

  # Make sure file is removed when filehandle is closed
  # This will fail on NFS
  unlink0($fh, $file)
    or return undef;

  return $fh;

}


sub tempnam {

  croak 'Usage tempnam($dir, $prefix)' unless scalar(@_) == 2;

  my ($dir, $prefix) = @_;

  # Add a string to the prefix
  $prefix .= 'XXXXXXXX';

  # Concatenate the directory to the file
  my $template = File::Spec->catfile($dir, $prefix);

  return mktemp($template);

}


sub unlink0 {

  croak 'Usage: unlink0(filehandle, filename)'
    unless scalar(@_) == 2;

  # Read args
  my ($fh, $path) = @_;

  cmpstat($fh, $path) or return 0;

  # attempt remove the file (does not work on some platforms)
  if (_can_unlink_opened_file()) {

    # return early (Without unlink) if we have been instructed to retain files.
    return 1 if $KEEP_ALL;

    # XXX: do *not* call this on a directory; possible race
    #      resulting in recursive removal
    croak "unlink0: $path has become a directory!" if -d $path;
    unlink($path) or return 0;

    # Stat the filehandle
    my @fh = stat $fh;

    print "Link count = $fh[3] \n" if $DEBUG;

    # Make sure that the link count is zero
    # - Cygwin provides deferred unlinking, however,
    #   on Win9x the link count remains 1
    # On NFS the link count may still be 1 but we can't know that
    # we are on NFS.  Since we can't be sure, we'll defer it

    return 1 if $fh[3] == 0 || $^O eq 'cygwin';
  }
  # fall-through if we can't unlink now
  _deferred_unlink($fh, $path, 0);
  return 1;
}


sub cmpstat {

  croak 'Usage: cmpstat(filehandle, filename)'
    unless scalar(@_) == 2;

  # Read args
  my ($fh, $path) = @_;

  warn "Comparing stat\n"
    if $DEBUG;

  # Stat the filehandle - which may be closed if someone has manually
  # closed the file. Can not turn off warnings without using $^W
  # unless we upgrade to 5.006 minimum requirement
  my @fh;
  {
    local ($^W) = 0;
    @fh = stat $fh;
  }
  return unless @fh;

  if ($fh[3] > 1 && $^W) {
    carp "unlink0: fstat found too many links; SB=@fh" if $^W;
  }

  # Stat the path
  my @path = stat $path;

  unless (@path) {
    carp "unlink0: $path is gone already" if $^W;
    return;
  }

  # this is no longer a file, but may be a directory, or worse
  unless (-f $path) {
    confess "panic: $path is no longer a file: SB=@fh";
  }

  # Do comparison of each member of the array
  # On WinNT dev and rdev seem to be different
  # depending on whether it is a file or a handle.
  # Cannot simply compare all members of the stat return
  # Select the ones we can use
  my @okstat = (0..$#fh);       # Use all by default
  if ($^O eq 'MSWin32') {
    @okstat = (1,2,3,4,5,7,8,9,10);
  } elsif ($^O eq 'os2') {
    @okstat = (0, 2..$#fh);
  } elsif ($^O eq 'VMS') {      # device and file ID are sufficient
    @okstat = (0, 1);
  } elsif ($^O eq 'dos') {
    @okstat = (0,2..7,11..$#fh);
  } elsif ($^O eq 'mpeix') {
    @okstat = (0..4,8..10);
  }

  # Now compare each entry explicitly by number
  for (@okstat) {
    print "Comparing: $_ : $fh[$_] and $path[$_]\n" if $DEBUG;
    # Use eq rather than == since rdev, blksize, and blocks (6, 11,
    # and 12) will be '' on platforms that do not support them.  This
    # is fine since we are only comparing integers.
    unless ($fh[$_] eq $path[$_]) {
      warn "Did not match $_ element of stat\n" if $DEBUG;
      return 0;
    }
  }

  return 1;
}


sub unlink1 {
  croak 'Usage: unlink1(filehandle, filename)'
    unless scalar(@_) == 2;

  # Read args
  my ($fh, $path) = @_;

  cmpstat($fh, $path) or return 0;

  # Close the file
  close( $fh ) or return 0;

  # Make sure the file is writable (for windows)
  _force_writable( $path );

  # return early (without unlink) if we have been instructed to retain files.
  return 1 if $KEEP_ALL;

  # remove the file
  return unlink($path);
}


{
  # protect from using the variable itself
  my $LEVEL = STANDARD;
  sub safe_level {
    my $self = shift;
    if (@_) {
      my $level = shift;
      if (($level != STANDARD) && ($level != MEDIUM) && ($level != HIGH)) {
        carp "safe_level: Specified level ($level) not STANDARD, MEDIUM or HIGH - ignoring\n" if $^W;
      } else {
        # Don't allow this on perl 5.005 or earlier
        if ($] < 5.006 && $level != STANDARD) {
          # Cant do MEDIUM or HIGH checks
          croak "Currently requires perl 5.006 or newer to do the safe checks";
        }
        # Check that we are allowed to change level
        # Silently ignore if we can not.
        $LEVEL = $level if _can_do_level($level);
      }
    }
    return $LEVEL;
  }
}


{
  my $TopSystemUID = 10;
  $TopSystemUID = 197108 if $^O eq 'interix'; # "Administrator"
  sub top_system_uid {
    my $self = shift;
    if (@_) {
      my $newuid = shift;
      croak "top_system_uid: UIDs should be numeric"
        unless $newuid =~ /^\d+$/s;
      $TopSystemUID = $newuid;
    }
    return $TopSystemUID;
  }
}


package File::Temp::Dir;

use File::Path qw/ rmtree /;
use strict;
use overload '""' => "STRINGIFY",
  '0+' => \&File::Temp::NUMIFY,
  fallback => 1;

# private class specifically to support tempdir objects
# created by File::Temp->newdir

# ostensibly the same method interface as File::Temp but without
# inheriting all the IO::Seekable methods and other cruft

# Read-only - returns the name of the temp directory

sub dirname {
  my $self = shift;
  return $self->{DIRNAME};
}

sub STRINGIFY {
  my $self = shift;
  return $self->dirname;
}

sub unlink_on_destroy {
  my $self = shift;
  if (@_) {
    $self->{CLEANUP} = shift;
  }
  return $self->{CLEANUP};
}

sub DESTROY {
  my $self = shift;
  local($., $@, $!, $^E, $?);
  if ($self->unlink_on_destroy && 
      $$ == $self->{LAUNCHPID} && !$File::Temp::KEEP_ALL) {
    if (-d $self->{REALNAME}) {
      # Some versions of rmtree will abort if you attempt to remove
      # the directory you are sitting in. We protect that and turn it
      # into a warning. We do this because this occurs during object
      # destruction and so can not be caught by the user.
      eval { rmtree($self->{REALNAME}, $File::Temp::DEBUG, 0); };
      warn $@ if ($@ && $^W);
    }
  }
}

1;

__END__

=pod

=encoding utf-8

=head1 NAME

File::Temp - return name and handle of a temporary file safely

=head1 VERSION

version 0.2304

=head1 SYNOPSIS

  use File::Temp qw/ tempfile tempdir /;

  $fh = tempfile();
  ($fh, $filename) = tempfile();

  ($fh, $filename) = tempfile( $template, DIR => $dir);
  ($fh, $filename) = tempfile( $template, SUFFIX => '.dat');
  ($fh, $filename) = tempfile( $template, TMPDIR => 1 );

  binmode( $fh, ":utf8" );

  $dir = tempdir( CLEANUP => 1 );
  ($fh, $filename) = tempfile( DIR => $dir );

Object interface:

  require File::Temp;
  use File::Temp ();
  use File::Temp qw/ :seekable /;

  $fh = File::Temp->new();
  $fname = $fh->filename;

  $fh = File::Temp->new(TEMPLATE => $template);
  $fname = $fh->filename;

  $tmp = File::Temp->new( UNLINK => 0, SUFFIX => '.dat' );
  print $tmp "Some data\n";
  print "Filename is $tmp\n";
  $tmp->seek( 0, SEEK_END );

The following interfaces are provided for compatibility with
existing APIs. They should not be used in new code.

MkTemp family:

  use File::Temp qw/ :mktemp  /;

  ($fh, $file) = mkstemp( "tmpfileXXXXX" );
  ($fh, $file) = mkstemps( "tmpfileXXXXXX", $suffix);

  $tmpdir = mkdtemp( $template );

  $unopened_file = mktemp( $template );

POSIX functions:

  use File::Temp qw/ :POSIX /;

  $file = tmpnam();
  $fh = tmpfile();

  ($fh, $file) = tmpnam();

Compatibility functions:

  $unopened_file = File::Temp::tempnam( $dir, $pfx );

=head1 DESCRIPTION

C<File::Temp> can be used to create and open temporary files in a safe
way.  There is both a function interface and an object-oriented
interface.  The File::Temp constructor or the tempfile() function can
be used to return the name and the open filehandle of a temporary
file.  The tempdir() function can be used to create a temporary
directory.

The security aspect of temporary file creation is emphasized such that
a filehandle and filename are returned together.  This helps guarantee
that a race condition can not occur where the temporary file is
created by another process between checking for the existence of the
file and its opening.  Additional security levels are provided to
check, for example, that the sticky bit is set on world writable
directories.  See L<"safe_level"> for more information.

For compatibility with popular C library functions, Perl implementations of
the mkstemp() family of functions are provided. These are, mkstemp(),
mkstemps(), mkdtemp() and mktemp().

Additionally, implementations of the standard L<POSIX|POSIX>
tmpnam() and tmpfile() functions are provided if required.

Implementations of mktemp(), tmpnam(), and tempnam() are provided,
but should be used with caution since they return only a filename
that was valid when function was called, so cannot guarantee
that the file will not exist by the time the caller opens the filename.

Filehandles returned by these functions support the seekable methods.

=begin __INTERNALS

=head1 PORTABILITY

This section is at the top in order to provide easier access to
porters.  It is not expected to be rendered by a standard pod
formatting tool. Please skip straight to the SYNOPSIS section if you
are not trying to port this module to a new platform.

This module is designed to be portable across operating systems and it
currently supports Unix, VMS, DOS, OS/2, Windows and Mac OS
(Classic). When porting to a new OS there are generally three main
issues that have to be solved:
=over 4

=item *

Can the OS unlink an open file? If it can not then the
C<_can_unlink_opened_file> method should be modified.

=item *

Are the return values from C<stat> reliable? By default all the
return values from C<stat> are compared when unlinking a temporary
file using the filename and the handle. Operating systems other than
unix do not always have valid entries in all fields. If utility function
C<File::Temp::unlink0> fails then the C<stat> comparison should be
modified accordingly.

=item *

Security. Systems that can not support a test for the sticky bit
on a directory can not use the MEDIUM and HIGH security tests.
The C<_can_do_level> method should be modified accordingly.

=back

=end __INTERNALS

=head1 OBJECT-ORIENTED INTERFACE

This is the primary interface for interacting with
C<File::Temp>. Using the OO interface a temporary file can be created
when the object is constructed and the file can be removed when the
object is no longer required.

Note that there is no method to obtain the filehandle from the
C<File::Temp> object. The object itself acts as a filehandle.  The object
isa C<IO::Handle> and isa C<IO::Seekable> so all those methods are
available.

Also, the object is configured such that it stringifies to the name of the
temporary file and so can be compared to a filename directly.  It numifies
to the C<refaddr> the same as other handles and so can be compared to other
handles with C<==>.

    $fh eq $filename       # as a string
    $fh != \*STDOUT        # as a number

=over 4

=item B<new>

Create a temporary file object.

  my $tmp = File::Temp->new();

by default the object is constructed as if C<tempfile>
was called without options, but with the additional behaviour
that the temporary file is removed by the object destructor
if UNLINK is set to true (the default).

Supported arguments are the same as for C<tempfile>: UNLINK
(defaulting to true), DIR, EXLOCK and SUFFIX. Additionally, the filename
template is specified using the TEMPLATE option. The OPEN option
is not supported (the file is always opened).

 $tmp = File::Temp->new( TEMPLATE => 'tempXXXXX',
                        DIR => 'mydir',
                        SUFFIX => '.dat');

Arguments are case insensitive.

Can call croak() if an error occurs.

=item B<newdir>

Create a temporary directory using an object oriented interface.

  $dir = File::Temp->newdir();

By default the directory is deleted when the object goes out of scope.

Supports the same options as the C<tempdir> function. Note that directories
created with this method default to CLEANUP => 1.

  $dir = File::Temp->newdir( $template, %options );

A template may be specified either with a leading template or
with a TEMPLATE argument.

=item B<filename>

Return the name of the temporary file associated with this object
(if the object was created using the "new" constructor).

  $filename = $tmp->filename;

This method is called automatically when the object is used as
a string.

=item B<dirname>

Return the name of the temporary directory associated with this
object (if the object was created using the "newdir" constructor).

  $dirname = $tmpdir->dirname;

This method is called automatically when the object is used in string context.

=item B<unlink_on_destroy>

Control whether the file is unlinked when the object goes out of scope.
The file is removed if this value is true and $KEEP_ALL is not.

 $fh->unlink_on_destroy( 1 );

Default is for the file to be removed.

=item B<DESTROY>

When the object goes out of scope, the destructor is called. This
destructor will attempt to unlink the file (using L<unlink1|"unlink1">)
if the constructor was called with UNLINK set to 1 (the default state
if UNLINK is not specified).

No error is given if the unlink fails.

If the object has been passed to a child process during a fork, the
file will be deleted when the object goes out of scope in the parent.

For a temporary directory object the directory will be removed unless
the CLEANUP argument was used in the constructor (and set to false) or
C<unlink_on_destroy> was modified after creation.  Note that if a temp
directory is your current directory, it cannot be removed - a warning
will be given in this case.  C<chdir()> out of the directory before
letting the object go out of scope.

If the global variable $KEEP_ALL is true, the file or directory
will not be removed.

=back

=head1 FUNCTIONS

This section describes the recommended interface for generating
temporary files and directories.

=over 4

=item B<tempfile>

This is the basic function to generate temporary files.
The behaviour of the file can be changed using various options:

  $fh = tempfile();
  ($fh, $filename) = tempfile();

Create a temporary file in  the directory specified for temporary
files, as specified by the tmpdir() function in L<File::Spec>.

  ($fh, $filename) = tempfile($template);

Create a temporary file in the current directory using the supplied
template.  Trailing `X' characters are replaced with random letters to
generate the filename.  At least four `X' characters must be present
at the end of the template.

  ($fh, $filename) = tempfile($template, SUFFIX => $suffix)

Same as previously, except that a suffix is added to the template
after the `X' translation.  Useful for ensuring that a temporary
filename has a particular extension when needed by other applications.
But see the WARNING at the end.

  ($fh, $filename) = tempfile($template, DIR => $dir);

Translates the template as before except that a directory name
is specified.

  ($fh, $filename) = tempfile($template, TMPDIR => 1);

Equivalent to specifying a DIR of "File::Spec->tmpdir", writing the file
into the same temporary directory as would be used if no template was
specified at all.

  ($fh, $filename) = tempfile($template, UNLINK => 1);

Return the filename and filehandle as before except that the file is
automatically removed when the program exits (dependent on
$KEEP_ALL). Default is for the file to be removed if a file handle is
requested and to be kept if the filename is requested. In a scalar
context (where no filename is returned) the file is always deleted
either (depending on the operating system) on exit or when it is
closed (unless $KEEP_ALL is true when the temp file is created).

Use the object-oriented interface if fine-grained control of when
a file is removed is required.

If the template is not specified, a template is always
automatically generated. This temporary file is placed in tmpdir()
(L<File::Spec>) unless a directory is specified explicitly with the
DIR option.

  $fh = tempfile( DIR => $dir );

If called in scalar context, only the filehandle is returned and the
file will automatically be deleted when closed on operating systems
that support this (see the description of tmpfile() elsewhere in this
document).  This is the preferred mode of operation, as if you only
have a filehandle, you can never create a race condition by fumbling
with the filename. On systems that can not unlink an open file or can
not mark a file as temporary when it is opened (for example, Windows
NT uses the C<O_TEMPORARY> flag) the file is marked for deletion when
the program ends (equivalent to setting UNLINK to 1). The C<UNLINK>
flag is ignored if present.

  (undef, $filename) = tempfile($template, OPEN => 0);

This will return the filename based on the template but
will not open this file.  Cannot be used in conjunction with
UNLINK set to true. Default is to always open the file
to protect from possible race conditions. A warning is issued
if warnings are turned on. Consider using the tmpnam()
and mktemp() functions described elsewhere in this document
if opening the file is not required.

If the operating system supports it (for example BSD derived systems), the 
filehandle will be opened with O_EXLOCK (open with exclusive file lock). 
This can sometimes cause problems if the intention is to pass the filename 
to another system that expects to take an exclusive lock itself (such as 
DBD::SQLite) whilst ensuring that the tempfile is not reused. In this 
situation the "EXLOCK" option can be passed to tempfile. By default EXLOCK 
will be true (this retains compatibility with earlier releases).

  ($fh, $filename) = tempfile($template, EXLOCK => 0);

Options can be combined as required.

Will croak() if there is an error.

=item B<tempdir>

This is the recommended interface for creation of temporary
directories.  By default the directory will not be removed on exit
(that is, it won't be temporary; this behaviour can not be changed
because of issues with backwards compatibility). To enable removal
either use the CLEANUP option which will trigger removal on program
exit, or consider using the "newdir" method in the object interface which
will allow the directory to be cleaned up when the object goes out of
scope.

The behaviour of the function depends on the arguments:

  $tempdir = tempdir();

Create a directory in tmpdir() (see L<File::Spec|File::Spec>).

  $tempdir = tempdir( $template );

Create a directory from the supplied template. This template is
similar to that described for tempfile(). `X' characters at the end
of the template are replaced with random letters to construct the
directory name. At least four `X' characters must be in the template.

  $tempdir = tempdir ( DIR => $dir );

Specifies the directory to use for the temporary directory.
The temporary directory name is derived from an internal template.

  $tempdir = tempdir ( $template, DIR => $dir );

Prepend the supplied directory name to the template. The template
should not include parent directory specifications itself. Any parent
directory specifications are removed from the template before
prepending the supplied directory.

  $tempdir = tempdir ( $template, TMPDIR => 1 );

Using the supplied template, create the temporary directory in
a standard location for temporary files. Equivalent to doing

  $tempdir = tempdir ( $template, DIR => File::Spec->tmpdir);

but shorter. Parent directory specifications are stripped from the
template itself. The C<TMPDIR> option is ignored if C<DIR> is set
explicitly.  Additionally, C<TMPDIR> is implied if neither a template
nor a directory are supplied.

  $tempdir = tempdir( $template, CLEANUP => 1);

Create a temporary directory using the supplied template, but
attempt to remove it (and all files inside it) when the program
exits. Note that an attempt will be made to remove all files from
the directory even if they were not created by this module (otherwise
why ask to clean it up?). The directory removal is made with
the rmtree() function from the L<File::Path|File::Path> module.
Of course, if the template is not specified, the temporary directory
will be created in tmpdir() and will also be removed at program exit.

Will croak() if there is an error.

=back

=head1 MKTEMP FUNCTIONS

The following functions are Perl implementations of the
mktemp() family of temp file generation system calls.

=over 4

=item B<mkstemp>

Given a template, returns a filehandle to the temporary file and the name
of the file.

  ($fh, $name) = mkstemp( $template );

In scalar context, just the filehandle is returned.

The template may be any filename with some number of X's appended
to it, for example F</tmp/temp.XXXX>. The trailing X's are replaced
with unique alphanumeric combinations.

Will croak() if there is an error.

=item B<mkstemps>

Similar to mkstemp(), except that an extra argument can be supplied
with a suffix to be appended to the template.

  ($fh, $name) = mkstemps( $template, $suffix );

For example a template of C<testXXXXXX> and suffix of C<.dat>
would generate a file similar to F<testhGji_w.dat>.

Returns just the filehandle alone when called in scalar context.

Will croak() if there is an error.

=item B<mkdtemp>

Create a directory from a template. The template must end in
X's that are replaced by the routine.

  $tmpdir_name = mkdtemp($template);

Returns the name of the temporary directory created.

Directory must be removed by the caller.

Will croak() if there is an error.

=item B<mktemp>

Returns a valid temporary filename but does not guarantee
that the file will not be opened by someone else.

  $unopened_file = mktemp($template);

Template is the same as that required by mkstemp().

Will croak() if there is an error.

=back

=head1 POSIX FUNCTIONS

This section describes the re-implementation of the tmpnam()
and tmpfile() functions described in L<POSIX>
using the mkstemp() from this module.

Unlike the L<POSIX|POSIX> implementations, the directory used
for the temporary file is not specified in a system include
file (C<P_tmpdir>) but simply depends on the choice of tmpdir()
returned by L<File::Spec|File::Spec>. On some implementations this
location can be set using the C<TMPDIR> environment variable, which
may not be secure.
If this is a problem, simply use mkstemp() and specify a template.

=over 4

=item B<tmpnam>

When called in scalar context, returns the full name (including path)
of a temporary file (uses mktemp()). The only check is that the file does
not already exist, but there is no guarantee that that condition will
continue to apply.

  $file = tmpnam();

When called in list context, a filehandle to the open file and
a filename are returned. This is achieved by calling mkstemp()
after constructing a suitable template.

  ($fh, $file) = tmpnam();

If possible, this form should be used to prevent possible
race conditions.

See L<File::Spec/tmpdir> for information on the choice of temporary
directory for a particular operating system.

Will croak() if there is an error.

=item B<tmpfile>

Returns the filehandle of a temporary file.

  $fh = tmpfile();

The file is removed when the filehandle is closed or when the program
exits. No access to the filename is provided.

If the temporary file can not be created undef is returned.
Currently this command will probably not work when the temporary
directory is on an NFS file system.

Will croak() if there is an error.

=back

=head1 ADDITIONAL FUNCTIONS

These functions are provided for backwards compatibility
with common tempfile generation C library functions.

They are not exported and must be addressed using the full package
name.

=over 4

=item B<tempnam>

Return the name of a temporary file in the specified directory
using a prefix. The file is guaranteed not to exist at the time
the function was called, but such guarantees are good for one
clock tick only.  Always use the proper form of C<sysopen>
with C<O_CREAT | O_EXCL> if you must open such a filename.

  $filename = File::Temp::tempnam( $dir, $prefix );

Equivalent to running mktemp() with $dir/$prefixXXXXXXXX
(using unix file convention as an example)

Because this function uses mktemp(), it can suffer from race conditions.

Will croak() if there is an error.

=back

=head1 UTILITY FUNCTIONS

Useful functions for dealing with the filehandle and filename.

=over 4

=item B<unlink0>

Given an open filehandle and the associated filename, make a safe
unlink. This is achieved by first checking that the filename and
filehandle initially point to the same file and that the number of
links to the file is 1 (all fields returned by stat() are compared).
Then the filename is unlinked and the filehandle checked once again to
verify that the number of links on that file is now 0.  This is the
closest you can come to making sure that the filename unlinked was the
same as the file whose descriptor you hold.

  unlink0($fh, $path)
     or die "Error unlinking file $path safely";

Returns false on error but croaks() if there is a security
anomaly. The filehandle is not closed since on some occasions this is
not required.

On some platforms, for example Windows NT, it is not possible to
unlink an open file (the file must be closed first). On those
platforms, the actual unlinking is deferred until the program ends and
good status is returned. A check is still performed to make sure that
the filehandle and filename are pointing to the same thing (but not at
the time the end block is executed since the deferred removal may not
have access to the filehandle).

Additionally, on Windows NT not all the fields returned by stat() can
be compared. For example, the C<dev> and C<rdev> fields seem to be
different.  Also, it seems that the size of the file returned by stat()
does not always agree, with C<stat(FH)> being more accurate than
C<stat(filename)>, presumably because of caching issues even when
using autoflush (this is usually overcome by waiting a while after
writing to the tempfile before attempting to C<unlink0> it).

Finally, on NFS file systems the link count of the file handle does
not always go to zero immediately after unlinking. Currently, this
command is expected to fail on NFS disks.

This function is disabled if the global variable $KEEP_ALL is true
and an unlink on open file is supported. If the unlink is to be deferred
to the END block, the file is still registered for removal.

This function should not be called if you are using the object oriented
interface since the it will interfere with the object destructor deleting
the file.

=item B<cmpstat>

Compare C<stat> of filehandle with C<stat> of provided filename.  This
can be used to check that the filename and filehandle initially point
to the same file and that the number of links to the file is 1 (all
fields returned by stat() are compared).

  cmpstat($fh, $path)
     or die "Error comparing handle with file";

Returns false if the stat information differs or if the link count is
greater than 1. Calls croak if there is a security anomaly.

On certain platforms, for example Windows, not all the fields returned by stat()
can be compared. For example, the C<dev> and C<rdev> fields seem to be
different in Windows.  Also, it seems that the size of the file
returned by stat() does not always agree, with C<stat(FH)> being more
accurate than C<stat(filename)>, presumably because of caching issues
even when using autoflush (this is usually overcome by waiting a while
after writing to the tempfile before attempting to C<unlink0> it).

Not exported by default.

=item B<unlink1>

Similar to C<unlink0> except after file comparison using cmpstat, the
filehandle is closed prior to attempting to unlink the file. This
allows the file to be removed without using an END block, but does
mean that the post-unlink comparison of the filehandle state provided
by C<unlink0> is not available.

  unlink1($fh, $path)
     or die "Error closing and unlinking file";

Usually called from the object destructor when using the OO interface.

Not exported by default.

This function is disabled if the global variable $KEEP_ALL is true.

Can call croak() if there is a security anomaly during the stat()
comparison.

=item B<cleanup>

Calling this function will cause any temp files or temp directories
that are registered for removal to be removed. This happens automatically
when the process exits but can be triggered manually if the caller is sure
that none of the temp files are required. This method can be registered as
an Apache callback.

Note that if a temp directory is your current directory, it cannot be
removed.  C<chdir()> out of the directory first before calling
C<cleanup()>. (For the cleanup at program exit when the CLEANUP flag
is set, this happens automatically.)

On OSes where temp files are automatically removed when the temp file
is closed, calling this function will have no effect other than to remove
temporary directories (which may include temporary files).

  File::Temp::cleanup();

Not exported by default.

=back

=head1 PACKAGE VARIABLES

These functions control the global state of the package.

=over 4

=item B<safe_level>

Controls the lengths to which the module will go to check the safety of the
temporary file or directory before proceeding.
Options are:

=over 8

=item STANDARD

Do the basic security measures to ensure the directory exists and is
writable, that temporary files are opened only if they do not already
exist, and that possible race conditions are avoided.  Finally the
L<unlink0|"unlink0"> function is used to remove files safely.

=item MEDIUM

In addition to the STANDARD security, the output directory is checked
to make sure that it is owned either by root or the user running the
program. If the directory is writable by group or by other, it is then
checked to make sure that the sticky bit is set.

Will not work on platforms that do not support the C<-k> test
for sticky bit.

=item HIGH

In addition to the MEDIUM security checks, also check for the
possibility of ``chown() giveaway'' using the L<POSIX|POSIX>
sysconf() function. If this is a possibility, each directory in the
path is checked in turn for safeness, recursively walking back to the
root directory.

For platforms that do not support the L<POSIX|POSIX>
C<_PC_CHOWN_RESTRICTED> symbol (for example, Windows NT) it is
assumed that ``chown() giveaway'' is possible and the recursive test
is performed.

=back

The level can be changed as follows:

  File::Temp->safe_level( File::Temp::HIGH );

The level constants are not exported by the module.

Currently, you must be running at least perl v5.6.0 in order to
run with MEDIUM or HIGH security. This is simply because the
safety tests use functions from L<Fcntl|Fcntl> that are not
available in older versions of perl. The problem is that the version
number for Fcntl is the same in perl 5.6.0 and in 5.005_03 even though
they are different versions.

On systems that do not support the HIGH or MEDIUM safety levels
(for example Win NT or OS/2) any attempt to change the level will
be ignored. The decision to ignore rather than raise an exception
allows portable programs to be written with high security in mind
for the systems that can support this without those programs failing
on systems where the extra tests are irrelevant.

If you really need to see whether the change has been accepted
simply examine the return value of C<safe_level>.

  $newlevel = File::Temp->safe_level( File::Temp::HIGH );
  die "Could not change to high security"
      if $newlevel != File::Temp::HIGH;

=item TopSystemUID

This is the highest UID on the current system that refers to a root
UID. This is used to make sure that the temporary directory is
owned by a system UID (C<root>, C<bin>, C<sys> etc) rather than
simply by root.

This is required since on many unix systems C</tmp> is not owned
by root.

Default is to assume that any UID less than or equal to 10 is a root
UID.

  File::Temp->top_system_uid(10);
  my $topid = File::Temp->top_system_uid;

This value can be adjusted to reduce security checking if required.
The value is only relevant when C<safe_level> is set to MEDIUM or higher.

=item B<$KEEP_ALL>

Controls whether temporary files and directories should be retained
regardless of any instructions in the program to remove them
automatically.  This is useful for debugging but should not be used in
production code.

  $File::Temp::KEEP_ALL = 1;

Default is for files to be removed as requested by the caller.

In some cases, files will only be retained if this variable is true
when the file is created. This means that you can not create a temporary
file, set this variable and expect the temp file to still be around
when the program exits.

=item B<$DEBUG>

Controls whether debugging messages should be enabled.

  $File::Temp::DEBUG = 1;

Default is for debugging mode to be disabled.

=back

=head1 WARNING

For maximum security, endeavour always to avoid ever looking at,
touching, or even imputing the existence of the filename.  You do not
know that that filename is connected to the same file as the handle
you have, and attempts to check this can only trigger more race
conditions.  It's far more secure to use the filehandle alone and
dispense with the filename altogether.

If you need to pass the handle to something that expects a filename
then on a unix system you can use C<"/dev/fd/" . fileno($fh)> for
arbitrary programs. Perl code that uses the 2-argument version of
C<< open >> can be passed C<< "+<=&" . fileno($fh) >>. Otherwise you
will need to pass the filename. You will have to clear the
close-on-exec bit on that file descriptor before passing it to another
process.

    use Fcntl qw/F_SETFD F_GETFD/;
    fcntl($tmpfh, F_SETFD, 0)
        or die "Can't clear close-on-exec flag on temp fh: $!\n";

=head2 Temporary files and NFS

Some problems are associated with using temporary files that reside
on NFS file systems and it is recommended that a local filesystem
is used whenever possible. Some of the security tests will most probably
fail when the temp file is not local. Additionally, be aware that
the performance of I/O operations over NFS will not be as good as for
a local disk.

=head2 Forking

In some cases files created by File::Temp are removed from within an
END block. Since END blocks are triggered when a child process exits
(unless C<POSIX::_exit()> is used by the child) File::Temp takes care
to only remove those temp files created by a particular process ID. This
means that a child will not attempt to remove temp files created by the
parent process.

If you are forking many processes in parallel that are all creating
temporary files, you may need to reset the random number seed using
srand(EXPR) in each child else all the children will attempt to walk
through the same set of random file names and may well cause
themselves to give up if they exceed the number of retry attempts.

=head2 Directory removal

Note that if you have chdir'ed into the temporary directory and it is
subsequently cleaned up (either in the END block or as part of object
destruction), then you will get a warning from File::Path::rmtree().

=head2 Taint mode

If you need to run code under taint mode, updating to the latest
L<File::Spec> is highly recommended.

=head2 BINMODE

The file returned by File::Temp will have been opened in binary mode
if such a mode is available. If that is not correct, use the C<binmode()>
function to change the mode of the filehandle.

Note that you can modify the encoding of a file opened by File::Temp
also by using C<binmode()>.

=head1 HISTORY

Originally began life in May 1999 as an XS interface to the system
mkstemp() function. In March 2000, the OpenBSD mkstemp() code was
translated to Perl for total control of the code's
security checking, to ensure the presence of the function regardless of
operating system and to help with portability. The module was shipped
as a standard part of perl from v5.6.1.

Thanks to Tom Christiansen for suggesting that this module
should be written and providing ideas for code improvements and
security enhancements.

=head1 SEE ALSO

L<POSIX/tmpnam>, L<POSIX/tmpfile>, L<File::Spec>, L<File::Path>

See L<IO::File> and L<File::MkTemp>, L<Apache::TempFile> for
different implementations of temporary file handling.

See L<File::Tempdir> for an alternative object-oriented wrapper for
the C<tempdir> function.

=for Pod::Coverage STRINGIFY NUMIFY top_system_uid

# vim: ts=2 sts=2 sw=2 et:

=for :stopwords cpan testmatrix url annocpan anno bugtracker rt cpants kwalitee diff irc mailto metadata placeholders metacpan

=head1 SUPPORT

=head2 Bugs / Feature Requests

Please report any bugs or feature requests through the issue tracker
at L<http://rt.cpan.org/Public/Dist/Display.html?Name=File-Temp>.
You will be notified automatically of any progress on your issue.

=head2 Source Code

This is open source software.  The code repository is available for
public review and contribution under the terms of the license.

L<https://github.com/Perl-Toolchain-Gang/File-Temp>

  git clone https://github.com/Perl-Toolchain-Gang/File-Temp.git

=head1 AUTHOR

Tim Jenness <tjenness@cpan.org>

=head1 CONTRIBUTORS

=over 4

=item *

Ben Tilly <btilly@gmail.com>

=item *

David Golden <dagolden@cpan.org>

=item *

David Steinbrunner <dsteinbrunner@pobox.com>

=item *

Ed Avis <eda@linux01.wcl.local>

=item *

James E. Keenan <jkeen@verizon.net>

=item *

Karen Etheridge <ether@cpan.org>

=item *

Kevin Ryde <user42@zip.com.au>

=item *

Olivier Mengue <dolmen@cpan.org>

=item *

Peter John Acklam <pjacklam@online.no>

=item *

Peter Rabbitson <ribasushi@cpan.org>

=back

=head1 COPYRIGHT AND LICENSE

This software is copyright (c) 2013 by Tim Jenness and the UK Particle Physics and Astronomy Research Council.

This is free software; you can redistribute it and/or modify it under
the same terms as the Perl 5 programming language system itself.

=cut

package Archive::Zip::MemberRead;

=head1 NAME

Archive::Zip::MemberRead - A wrapper that lets you read Zip archive members as if they were files.

=cut

=head1 SYNOPSIS

  use Archive::Zip;
  use Archive::Zip::MemberRead;
  $zip = Archive::Zip->new("file.zip");
  $fh  = Archive::Zip::MemberRead->new($zip, "subdir/abc.txt");
  while (defined($line = $fh->getline()))
  {
      print $fh->input_line_number . "#: $line\n";
  }

  $read = $fh->read($buffer, 32*1024);
  print "Read $read bytes as :$buffer:\n";

=head1 DESCRIPTION

The Archive::Zip::MemberRead module lets you read Zip archive member data
just like you read data from files.

=head1 METHODS

=over 4

=cut

use strict;

use Archive::Zip qw( :ERROR_CODES :CONSTANTS );

use vars qw{$VERSION};

my $nl;

BEGIN {
    $VERSION = '1.48';
    $VERSION = eval $VERSION;

# Requirement for newline conversion. Should check for e.g., DOS and OS/2 as well, but am too lazy.
    $nl = $^O eq 'MSWin32' ? "\r\n" : "\n";
}

=item Archive::Zip::Member::readFileHandle()

You can get a C<Archive::Zip::MemberRead> from an archive member by
calling C<readFileHandle()>:

  my $member = $zip->memberNamed('abc/def.c');
  my $fh = $member->readFileHandle();
  while (defined($line = $fh->getline()))
  {
      # ...
  }
  $fh->close();

=cut

sub Archive::Zip::Member::readFileHandle {
    return Archive::Zip::MemberRead->new(shift());
}

=item Archive::Zip::MemberRead->new($zip, $fileName)

=item Archive::Zip::MemberRead->new($zip, $member)

=item Archive::Zip::MemberRead->new($member)

Construct a new Archive::Zip::MemberRead on the specified member.

  my $fh = Archive::Zip::MemberRead->new($zip, 'fred.c')

=cut

sub new {
    my ($class, $zip, $file) = @_;
    my ($self, $member);

    if ($zip && $file)    # zip and filename, or zip and member
    {
        $member = ref($file) ? $file : $zip->memberNamed($file);
    } elsif ($zip && !$file && ref($zip))    # just member
    {
        $member = $zip;
    } else {
        die(
            'Archive::Zip::MemberRead::new needs a zip and filename, zip and member, or member'
        );
    }

    $self = {};
    bless($self, $class);
    $self->set_member($member);
    return $self;
}

sub set_member {
    my ($self, $member) = @_;

    $self->{member} = $member;
    $self->set_compression(COMPRESSION_STORED);
    $self->rewind();
}

sub set_compression {
    my ($self, $compression) = @_;
    $self->{member}->desiredCompressionMethod($compression) if $self->{member};
}

=item setLineEnd(expr)

Set the line end character to use. This is set to \n by default
except on Windows systems where it is set to \r\n. You will
only need to set this on systems which are not Windows or Unix
based and require a line end different from \n.
This is a class method so call as C<Archive::Zip::MemberRead>->C<setLineEnd($nl)>

=cut

sub setLineEnd {
    shift;
    $nl = shift;
}

=item rewind()

Rewinds an C<Archive::Zip::MemberRead> so that you can read from it again
starting at the beginning.

=cut

sub rewind {
    my $self = shift;

    $self->_reset_vars();
    $self->{member}->rewindData() if $self->{member};
}

sub _reset_vars {
    my $self = shift;

    $self->{line_no} = 0;
    $self->{at_end}  = 0;

    delete $self->{buffer};
}

=item input_record_separator(expr)

If the argument is given, input_record_separator for this
instance is set to it. The current setting (which may be
the global $/) is always returned.

=cut

sub input_record_separator {
    my $self = shift;
    if (@_) {
        $self->{sep} = shift;
        $self->{sep_re} =
          _sep_as_re($self->{sep});    # Cache the RE as an optimization
    }
    return exists $self->{sep} ? $self->{sep} : $/;
}

# Return the input_record_separator in use as an RE fragment
# Note that if we have a per-instance input_record_separator
# we can just return the already converted value. Otherwise,
# the conversion must be done on $/ every time since we cannot
# know whether it has changed or not.
sub _sep_re {
    my $self = shift;

    # Important to phrase this way: sep's value may be undef.
    return exists $self->{sep} ? $self->{sep_re} : _sep_as_re($/);
}

# Convert the input record separator into an RE and return it.
sub _sep_as_re {
    my $sep = shift;
    if (defined $sep) {
        if ($sep eq '') {
            return "(?:$nl){2,}";
        } else {
            $sep =~ s/\n/$nl/og;
            return quotemeta $sep;
        }
    } else {
        return undef;
    }
}

=item input_line_number()

Returns the current line number, but only if you're using C<getline()>.
Using C<read()> will not update the line number.

=cut

sub input_line_number {
    my $self = shift;
    return $self->{line_no};
}

=item close()

Closes the given file handle.

=cut

sub close {
    my $self = shift;

    $self->_reset_vars();
    $self->{member}->endRead();
}

=item buffer_size([ $size ])

Gets or sets the buffer size used for reads.
Default is the chunk size used by Archive::Zip.

=cut

sub buffer_size {
    my ($self, $size) = @_;

    if (!$size) {
        return $self->{chunkSize} || Archive::Zip::chunkSize();
    } else {
        $self->{chunkSize} = $size;
    }
}

=item getline()

Returns the next line from the currently open member.
Makes sense only for text files.
A read error is considered fatal enough to die.
Returns undef on eof. All subsequent calls would return undef,
unless a rewind() is called.
Note: The line returned has the input_record_separator (default: newline) removed.

=item getline( { preserve_line_ending => 1 } )

Returns the next line including the line ending.

=cut

sub getline {
    my ($self, $argref) = @_;

    my $size = $self->buffer_size();
    my $sep  = $self->_sep_re();

    my $preserve_line_ending;
    if (ref $argref eq 'HASH') {
        $preserve_line_ending = $argref->{'preserve_line_ending'};
        $sep =~ s/\\([^A-Za-z_0-9])+/$1/g;
    }

    for (; ;) {
        if (   $sep
            && defined($self->{buffer})
            && $self->{buffer} =~ s/^(.*?)$sep//s) {
            my $line = $1;
            $self->{line_no}++;
            if ($preserve_line_ending) {
                return $line . $sep;
            } else {
                return $line;
            }
        } elsif ($self->{at_end}) {
            $self->{line_no}++ if $self->{buffer};
            return delete $self->{buffer};
        }
        my ($temp, $status) = $self->{member}->readChunk($size);
        if ($status != AZ_OK && $status != AZ_STREAM_END) {
            die "ERROR: Error reading chunk from archive - $status";
        }
        $self->{at_end} = $status == AZ_STREAM_END;
        $self->{buffer} .= $$temp;
    }
}

=item read($buffer, $num_bytes_to_read)

Simulates a normal C<read()> system call.
Returns the no. of bytes read. C<undef> on error, 0 on eof, I<e.g.>:

  $fh = Archive::Zip::MemberRead->new($zip, "sreeji/secrets.bin");
  while (1)
  {
    $read = $fh->read($buffer, 1024);
    die "FATAL ERROR reading my secrets !\n" if (!defined($read));
    last if (!$read);
    # Do processing.
    ....
   }

=cut

#
# All these $_ are required to emulate read().
#
sub read {
    my $self = $_[0];
    my $size = $_[2];
    my ($temp, $status, $ret);

    ($temp, $status) = $self->{member}->readChunk($size);
    if ($status != AZ_OK && $status != AZ_STREAM_END) {
        $_[1] = undef;
        $ret = undef;
    } else {
        $_[1] = $$temp;
        $ret = length($$temp);
    }
    return $ret;
}

1;

=back

=head1 AUTHOR

Sreeji K. Das E<lt>sreeji_k@yahoo.comE<gt>

See L<Archive::Zip> by Ned Konz without which this module does not make
any sense! 

Minor mods by Ned Konz.

=head1 COPYRIGHT

Copyright 2002 Sreeji K. Das.

This program is free software; you can redistribute it and/or modify it under
the same terms as Perl itself.

=cut

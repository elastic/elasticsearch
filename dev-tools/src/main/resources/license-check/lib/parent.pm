package parent;
use strict;
use vars qw($VERSION);
$VERSION = '0.234';

sub import {
    my $class = shift;

    my $inheritor = caller(0);

    if ( @_ and $_[0] eq '-norequire' ) {
        shift @_;
    } else {
        for ( my @filename = @_ ) {
            s{::|'}{/}g;
            require "$_.pm"; # dies if the file is not found
        }
    }

    {
        no strict 'refs';
        push @{"$inheritor\::ISA"}, @_;
    };
};

"All your base are belong to us"

__END__

=encoding utf8

=head1 NAME

parent - Establish an ISA relationship with base classes at compile time

=head1 SYNOPSIS

    package Baz;
    use parent qw(Foo Bar);

=head1 DESCRIPTION

Allows you to both load one or more modules, while setting up inheritance from
those modules at the same time.  Mostly similar in effect to

    package Baz;
    BEGIN {
        require Foo;
        require Bar;
        push @ISA, qw(Foo Bar);
    }

By default, every base class needs to live in a file of its own.
If you want to have a subclass and its parent class in the same file, you
can tell C<parent> not to load any modules by using the C<-norequire> switch:

  package Foo;
  sub exclaim { "I CAN HAS PERL" }

  package DoesNotLoadFooBar;
  use parent -norequire, 'Foo', 'Bar';
  # will not go looking for Foo.pm or Bar.pm

This is equivalent to the following code:

  package Foo;
  sub exclaim { "I CAN HAS PERL" }

  package DoesNotLoadFooBar;
  push @DoesNotLoadFooBar::ISA, 'Foo', 'Bar';

This is also helpful for the case where a package lives within
a differently named file:

  package MyHash;
  use Tie::Hash;
  use parent -norequire, 'Tie::StdHash';

This is equivalent to the following code:

  package MyHash;
  require Tie::Hash;
  push @ISA, 'Tie::StdHash';

If you want to load a subclass from a file that C<require> would
not consider an eligible filename (that is, it does not end in
either C<.pm> or C<.pmc>), use the following code:

  package MySecondPlugin;
  require './plugins/custom.plugin'; # contains Plugin::Custom
  use parent -norequire, 'Plugin::Custom';

=head1 HISTORY

This module was forked from L<base> to remove the cruft
that had accumulated in it.

=head1 CAVEATS

=head1 SEE ALSO

L<base>

=head1 AUTHORS AND CONTRIBUTORS

Rafaël Garcia-Suarez, Bart Lateur, Max Maischein, Anno Siegel, Michael Schwern

=head1 MAINTAINER

Max Maischein C< corion@cpan.org >

Copyright (c) 2007-10 Max Maischein C<< <corion@cpan.org> >>
Based on the idea of C<base.pm>, which was introduced with Perl 5.004_04.

=head1 LICENSE

This module is released under the same terms as Perl itself.

=cut

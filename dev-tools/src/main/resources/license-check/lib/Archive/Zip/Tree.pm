package Archive::Zip::Tree;

use strict;
use vars qw{$VERSION};

BEGIN {
	$VERSION = '1.48';
}

use Archive::Zip;

warn(
    "Archive::Zip::Tree is deprecated; its methods have been moved into Archive::Zip."
) if $^W;

1;

__END__

=head1 NAME

Archive::Zip::Tree - (DEPRECATED) methods for adding/extracting trees using Archive::Zip

=head1 DESCRIPTION

This module is deprecated, because all its methods were moved into the main
Archive::Zip module.

It is included in the distribution merely to avoid breaking old code.

See L<Archive::Zip>.

=head1 AUTHOR

Ned Konz, perl@bike-nomad.com

=head1 COPYRIGHT

Copyright (c) 2000-2002 Ned Konz. All rights reserved.  This program is free
software; you can redistribute it and/or modify it under the same terms
as Perl itself.

=head1 SEE ALSO

L<Archive::Zip>

=cut


#!/usr/bin/env perl

use strict;
use warnings;

use HTTP::Tiny;
use IO::Socket::SSL 1.52;
use utf8;
use Getopt::Long;

my $Base_URL  = "https://api.github.com/repos/";
my $User_Repo = 'elastic/elasticsearch/';
my $Issue_URL = "https://github.com/${User_Repo}issues";
use JSON();
use URI();
use URI::Escape qw(uri_escape_utf8);

our $json = JSON->new->utf8(1);
our $http = HTTP::Tiny->new(
    default_headers => {
        Accept        => "application/vnd.github.v3+json",
        Authorization => load_github_key()
    }
);

my %Opts = ( state => 'open' );

GetOptions(
    \%Opts,    #
    'state=s', 'labels=s', 'add=s', 'remove=s'
) || exit usage();

die usage('--state must be one of open|all|closed')
    unless $Opts{state} =~ /^(open|all|closed)$/;

die usage('--labels is required') unless $Opts{labels};
die usage('Either --add or --remove is required')
    unless $Opts{add} || $Opts{remove};

relabel();

#===================================
sub relabel {
#===================================
    my @remove = split /,/, ( $Opts{remove} || '' );
    my @add    = split /,/, ( $Opts{add}    || '' );
    my $add_json = $json->encode( \@add );
    my $url      = URI->new( $Base_URL . $User_Repo . 'issues' );
    $url->query_form(
        state    => $Opts{state},
        labels   => $Opts{labels},
        per_page => 100
    );

    my $spool = Spool->new($url);
    while ( my $issue = $spool->next ) {
        my $id = $issue->{number};
        print "$Issue_URL/$id\n";
        if (@add) {
            add_label( $id, $add_json );
        }
        for (@remove) {
            remove_label( $id, $_ );
        }
    }
    print "Done\n";
}

#===================================
sub add_label {
#===================================
    my ( $id, $json ) = @_;
    my $response = $http->post(
        $Base_URL . $User_Repo . "issues/$id/labels",
        {   content => $json,
            headers => { "Content-Type" => "application/json; charset=utf-8" }
        }
    );

    die "$response->{status} $response->{reason}\n"
        unless $response->{success};

}

#===================================
sub remove_label {
#===================================
    my ( $id, $name ) = @_;
    my $url
        = $Base_URL
        . $User_Repo
        . "issues/$id/labels/"
        . uri_escape_utf8($name);
    my $response = $http->delete($url);

    die "$response->{status} $response->{reason}\n"
        unless $response->{success};

}

#===================================
sub load_github_key {
#===================================
    my ($file) = glob("~/.github_auth");
    unless ( -e $file ) {
        warn "File ~/.github_auth doesn't exist - using anonymous API. "
            . "Generate a Personal Access Token at https://github.com/settings/applications\n";
        return '';
    }
    open my $fh, $file or die "Couldn't open $file: $!";
    my ($key) = <$fh> || die "Couldn't read $file: $!";
    $key =~ s/^\s+//;
    $key =~ s/\s+$//;
    die "Invalid GitHub key: $key"
        unless $key =~ /^[0-9a-f]{40}$/;
    return "token $key";

}

#===================================
sub usage {
#===================================
    my $msg = shift || '';

    if ($msg) {
        $msg = "\nERROR: $msg\n\n";
    }
    return $msg . <<"USAGE";
$0 --state=open|closed|all --labels=foo,bar --add=new1,new2 --remove=old1,old2

USAGE

}

package Spool;

use strict;
use warnings;

#===================================
sub new {
#===================================
    my $class = shift;
    my $url   = shift;
    return bless {
        url    => $url,
        buffer => []
        },
        $class;
}

#===================================
sub next {
#===================================
    my $self = shift;
    if ( @{ $self->{buffer} } == 0 ) {
        $self->refill;
    }
    return shift @{ $self->{buffer} };
}

#===================================
sub refill {
#===================================
    my $self = shift;
    return unless $self->{url};
    my $response = $http->get( $self->{url} );
    die "$response->{status} $response->{reason}\n"
        unless $response->{success};

    $self->{url} = '';

    if ( my $link = $response->{headers}{link} ) {
        my @links = ref $link eq 'ARRAY' ? @$link : $link;
        for ($link) {
            next unless $link =~ /<([^>]+)>; rel="next"/;
            $self->{url} = $1;
            last;
        }
    }

    push @{ $self->{buffer} }, @{ $json->decode( $response->{content} ) };

}

#!/usr/bin/env perl
# Licensed to Elasticsearch under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance  with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on
# an 'AS IS' BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied. See the License for the specific
# language governing permissions and limitations under the License.

use strict;
use warnings;

use HTTP::Tiny 0.070;
use IO::Socket::SSL 1.52;
use utf8;

my $Github_Key = load_github_key();
my $Base_URL   = "https://${Github_Key}api.github.com/repos/";
my $User_Repo  = 'elastic/elasticsearch/';
my $Issue_URL  = "http://github.com/${User_Repo}issues/";

my @Groups = (
    ">breaking",    ">breaking-java", ">deprecation", ">feature",
    ">enhancement", ">bug",           ">regression",  ">upgrade"
);
my %Ignore = map { $_ => 1 }
    ( ">non-issue", ">refactoring", ">docs", ">test", ">test-failure", ">test-mute", ":Core/Infra/Build", "backport", "WIP" );

my %Group_Labels = (
    '>breaking'      => 'Breaking changes',
    '>breaking-java' => 'Breaking Java changes',
    '>deprecation'   => 'Deprecations',
    '>feature'       => 'New features',
    '>enhancement'   => 'Enhancements',
    '>bug'           => 'Bug fixes',
    '>regression'    => 'Regressions',
    '>upgrade'       => 'Upgrades',
    'other'          => 'NOT CLASSIFIED',
);

my %Area_Overrides = (
    ':ml'            => 'Machine Learning',
    ':Beats'         => 'Beats Plugin',
    ':Docs'          => 'Docs Infrastructure'
);

use JSON();
use Encode qw(encode_utf8);

my $json = JSON->new->utf8(1);

my %All_Labels = fetch_labels();

my $version = shift @ARGV
    or dump_labels();

dump_labels("Unknown version '$version'")
    unless $All_Labels{$version};

my $issues = fetch_issues($version);
dump_issues( $version, $issues );

#===================================
sub dump_issues {
#===================================
    my $version = shift;
    my $issues  = shift;

    $version =~ s/v//;
    my $branch = $version;
    $branch =~ s/\.\d+$//;

    my %header_reverse_lookup;
    while (my ($label, $override) = each %Area_Overrides) {
        $header_reverse_lookup{$override} = substr $label, 1;
    }

    my ( $day, $month, $year ) = (gmtime)[ 3 .. 5 ];
    $month++;
    $year += 1900;

    print <<"ASCIIDOC";
:issue: https://github.com/${User_Repo}issues/
:pull:  https://github.com/${User_Repo}pull/

[[release-notes-$version]]
== {es} version $version

coming[$version]

Also see <<breaking-changes-$branch,Breaking changes in $branch>>.

ASCIIDOC

    for my $group ( @Groups, 'other' ) {
        my $group_issues = $issues->{$group} or next;
        my $group_id = $group;
        $group_id =~ s/^>//;
        print "[[$group_id-$version]]\n"
            . "[float]\n"
            . "=== $Group_Labels{$group}\n\n";

        for my $header ( sort keys %$group_issues ) {
            my $header_issues = $group_issues->{$header};
            print( $header || 'HEADER MISSING', "::\n" );

            for my $issue (@$header_issues) {
                my $title = $issue->{title};

                # Remove redundant prefixes from the title. For example,
                # given:
                #
                #     SQL: add support for foo queries
                #
                # the prefix is redundant under the "SQL" section.
                my $header_prefix = $header_reverse_lookup{$header} || $header;
                $title =~ s/^\[$header_prefix\]\s+//i;
                $title =~ s/^$header_prefix:\s+//i;

                # Remove any issue number prefix
                $title =~ s/^#\d+\s+//;

                $title = ucfirst $title;

                if ( $issue->{state} eq 'open' ) {
                    $title .= " [OPEN]";
                }
                unless ( $issue->{pull_request} ) {
                    $title .= " [ISSUE]";
                }
                my $number = $issue->{number};

                print encode_utf8("* $title {pull}${number}[#${number}]");

                if ( my $related = $issue->{related_issues} ) {
                    my %uniq = map { $_ => 1 } @$related;
                    print keys %uniq > 1
                        ? " (issues: "
                        : " (issue: ";
                    print join ", ", map {"{issue}${_}[#${_}]"}
                        sort keys %uniq;
                    print ")";
                }
                print "\n";
            }
            print "\n";
        }
        print "\n\n";
    }
}

#===================================
sub fetch_issues {
#===================================
    my $version = shift;
    my @issues;
    my %seen;
    for my $state ( 'open', 'closed' ) {
        my $page = 1;
        while (1) {
            my $tranche
                = fetch( $User_Repo
                    . 'issues?labels='
                    . $version
                    . '&pagesize=100&state='
                    . $state
                    . '&page='
                    . $page )
                or die "Couldn't fetch issues for version '$version'";
            push @issues, @$tranche;

            for my $issue (@$tranche) {
                next unless $issue->{pull_request};
                for ( $issue->{body} =~ m{(?:#|${User_Repo}issues/)(\d+)}g ) {
                    $seen{$_}++;
                    push @{ $issue->{related_issues} }, $_;
                }
            }
            $page++;
            last unless @$tranche;
        }
    }

    my %group;
ISSUE:
    for my $issue (@issues) {
        next if $seen{ $issue->{number} } && !$issue->{pull_request};

        foreach my $label ( @{ $issue->{labels} } ) {
            next ISSUE if $Ignore{ $label->{name} };

            # If this PR was backported to an earlier version, don't
            # include it in the docs.
            next ISSUE if is_pr_released_in_earlier_version($issue);
        }

        # uncomment for including/excluding PRs already issued in other versions
        # next if grep {$_->{name}=~/^v2/} @{$issue->{labels}};
        my %labels = map { $_->{name} => 1 } @{ $issue->{labels} };
        my @area_labels = grep {/^:/} sort keys %labels;
        my ($header) = map { m{:[^/]+/(.+)} && $1 } @area_labels;
        if (scalar @area_labels > 1) {
            $header = "MULTIPLE AREA LABELS";
        }
        if (scalar @area_labels == 1 && exists $Area_Overrides{$area_labels[0]}) {
            $header = $Area_Overrides{$area_labels[0]};
        }
        $header ||= 'NOT CLASSIFIED';
        for (@Groups) {
            if ( $labels{$_} ) {
                push @{ $group{$_}{$header} }, $issue;
                next ISSUE;
            }
        }
        push @{ $group{other}{$header} }, $issue;
    }

    return \%group;
}

#===================================
sub fetch_labels {
#===================================
    my %all;
    my $page = 1;
    while (1) {
        my $labels = fetch( $User_Repo . 'labels?page=' . $page++ )
            or die "Couldn't retrieve version labels";
        last unless @$labels;
        for (@$labels) {
            my $name = $_->{name};
            next unless $name =~ /^v/;
            $all{$name} = 1;
        }
    }
    return %all;
}

#===================================
sub fetch {
#===================================
    my $url      = $Base_URL . shift();
    my $response = HTTP::Tiny->new->get($url);
    die "$response->{status} $response->{reason}\n"
        unless $response->{success};

    #    print $response->{content};
    return $json->decode( $response->{content} );
}

#===================================
sub load_github_key {
#===================================
    my $file = "$ENV{HOME}/.github_auth";
    unless ( -e $file ) {
        warn "File ~/.github_auth doesn't exist - using anonymous API. "
            . "Generate a Personal Access Token at https://github.com/settings/applications\n";
        return '';
    }

    open KEYFILE, '<', $file or die "Couldn't open $file: $!";
    my ($key) = <KEYFILE> || die "Couldn't read $file: $!";
    close KEYFILE;

    $key =~ s/^\s+//;
    $key =~ s/\s+$//;
    die "Invalid GitHub key: $key"
        unless $key =~ /^[0-9a-f]{40}$/;

    return "$key:x-oauth-basic@";
}

#===================================
sub dump_labels {
#===================================
    my $error = shift || '';
    if ($error) {
        $error = "\nERROR: $error\n";
    }
    my $labels = join '', map { "    - $_\n" } sort keys %All_Labels;
    die <<USAGE
$error
USAGE: $0 version > outfile

Known versions:
$labels
USAGE

}

#===================================
sub is_pr_released_in_earlier_version {
#===================================
    my ($pr) = @_;

    my @labels =
        grep /^v \d+ \. \d+ \. \d+ $/x,
        map { $_->{name} }
        @{ $pr->{labels} };

    my $is_releasing_new_major_series = $version =~ m/^v \d+ \. 0 \. 0 $/x;

    # We assume that if we're releasing the first version in a major
    # series, there does not (yet) exist any later major series, and any
    # other release versions are for a prior major. We should therefore
    # skip this PR as being already released.
    if ($is_releasing_new_major_series and scalar(@labels) > 1) {
        return 0;
    }

    my ($current_major) = $version =~ m/^(v\d+\.)/;

    my @sortable_versions;

    foreach my $label (@labels) {
        # We filter by the current major, because we might release a change
        # at roughly the same time to a major series and the prior major
        # series. A user shouldn't have to consult release notes for the
        # prior major in order to see all the relevant changes.
        next unless $label =~ m/^$current_major/;

        $label =~ m/^v (\d+) \. (\d+) \. (\d+) $/x;

        push @sortable_versions, sprintf('%2d%2d%2d', $1, $2, $3);
    }

    # I apologise for this line. I'm just picking the earliest version,
    # picking it apart and joining it back together as a version label
    # string.
    my $earliest_version = sprintf 'v%d.%d.%d', unpack 'A2A2A2', (sort @sortable_versions)[0];

    return $earliest_version ne $version;
}

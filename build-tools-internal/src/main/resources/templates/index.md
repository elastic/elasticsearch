---
navigation_title: "Elasticsearch"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/es-connectors-release-notes.html
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/es-release-notes.html
  - https://www.elastic.co/guide/en/elasticsearch/reference/master/release-notes-${unqualifiedVersion}.html
  - https://www.elastic.co/guide/en/elasticsearch/reference/master/migrating-${version.major}.${version.minor}.html
---

# Elasticsearch release notes [elasticsearch-release-notes]

Review the changes, fixes, and more in each version of Elasticsearch.

To check for security updates, go to [Security announcements for the Elastic stack](https://discuss.elastic.co/c/announcements/security-announcements/31).

% Release notes include only features, enhancements, and fixes. Add breaking changes, deprecations, and known issues to the applicable release notes sections.

% ## version.next [elasticsearch-next-release-notes]
% **Release date:** Month day, year

% ### Features and enhancements [elasticsearch-next-features-enhancements]
% *

% ### Fixes [elasticsearch-next-fixes]
% *

## ${unqualifiedVersion} [elasticsearch-${versionWithoutSeparator}-release-notes]
**Release date:** April 01, 2025

<%
if (!notableHighlights.isEmpty() || !nonNotableHighlights.isEmpty()) {
    print "### Highlights\n"
}

for (highlights in [notableHighlights, nonNotableHighlights]) {
  if (!highlights.isEmpty()) {
    for (highlight in highlights) { %>
::::{dropdown} ${highlight.title}
${highlight.body.trim()}

For more information, check [PR #${highlight.pr}](https://github.com/elastic/elasticsearch/pull/${highlight.pr}).
::::
<% }
  }
}

for (changeType in ['features-enhancements', 'fixes', 'regression']) {
    if (changelogsByTypeByArea[changeType] == null || changelogsByTypeByArea[changeType].empty) {
        continue;
    }
%>
### ${ TYPE_LABELS.getOrDefault(changeType, 'No mapping for TYPE_LABELS[' + changeType + ']') } [elasticsearch-${versionWithoutSeparator}-${changeType}]
<% for (team in changelogsByTypeByArea[changeType].keySet()) {
    print "\n${team}:\n";

    for (change in changelogsByTypeByArea[changeType][team]) {
        print "* ${change.summary} [#${change.pr}](https://github.com/elastic/elasticsearch/pull/${change.pr})"
        if (change.issues != null && change.issues.empty == false) {
            print change.issues.size() == 1 ? " (issue: " : " (issues: "
            print change.issues.collect { "[#${change.pr}](https://github.com/elastic/elasticsearch/pull/${change.pr})" }.join(", ")
            print ")"
        }
        print "\n"
    }
}
}
print "\n\n"

---
navigation_title: "Elasticsearch"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/es-release-notes.html
---

# Elasticsearch release notes [elasticsearch-release-notes]

Review the changes, fixes, and more in each version of Elasticsearch.

To check for security updates, go to [Security announcements for the Elastic stack](https://discuss.elastic.co/c/announcements/security-announcements/31).

% Release notes include only features, enhancements, and fixes. Add breaking changes, deprecations, and known issues to the applicable release notes sections.

% ## version.next [elasticsearch-next-release-notes]

% ### Features and enhancements [elasticsearch-next-features-enhancements]
% *

% ### Fixes [elasticsearch-next-fixes]
% *
<%
for(bundle in changelogBundles) {
    def version = bundle.version
    def versionForIds = bundle.version.toString().equals('9.0.0') ? bundle.versionWithoutSeparator : bundle.version
    def changelogsByTypeByArea = bundle.changelogsByTypeByArea
    def notableHighlights = bundle.notableHighlights
    def nonNotableHighlights = bundle.nonNotableHighlights
    def unqualifiedVersion = bundle.unqualifiedVersion
    def coming = !bundle.bundle.released
%>
## ${unqualifiedVersion} [elasticsearch-${versionForIds}-release-notes]
<%

if (coming) {
    print "```{applies_to}\n"
    print "stack: coming ${version}\n"
    print "```"
    print "\n"
}

if (!notableHighlights.isEmpty() || !nonNotableHighlights.isEmpty()) {
    print "\n### Highlights [elasticsearch-${versionForIds}-highlights]\n"
}

for (highlights in [notableHighlights, nonNotableHighlights]) {
  if (!highlights.isEmpty()) {
    for (highlight in highlights) { %>
::::{dropdown} ${highlight.title}
${highlight.body.trim()}
::::
<% }
  }
}

for (changeType in ['features-enhancements', 'fixes', 'regression']) {
    if (changelogsByTypeByArea[changeType] == null || changelogsByTypeByArea[changeType].empty) {
        continue;
    }
%>
### ${ TYPE_LABELS.getOrDefault(changeType, 'No mapping for TYPE_LABELS[' + changeType + ']') } [elasticsearch-${versionForIds}-${changeType}]
<% for (team in changelogsByTypeByArea[changeType].keySet()) {
    print "\n${team}:\n";

    for (change in changelogsByTypeByArea[changeType][team]) {
        print "* ${change.summary} [#${change.pr}](https://github.com/elastic/elasticsearch/pull/${change.pr})"
        if (change.issues != null && change.issues.empty == false) {
            print change.issues.size() == 1 ? " (issue: " : " (issues: "
            print change.issues.collect { "[#${it}](https://github.com/elastic/elasticsearch/issues/${it})" }.join(", ")
            print ")"
        }
        print "\n"
    }
}
}
print "\n"
}

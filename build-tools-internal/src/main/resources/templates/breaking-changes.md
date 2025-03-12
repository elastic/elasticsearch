---
navigation_title: "Elasticsearch"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/breaking-changes.html
---

# Elasticsearch breaking changes [elasticsearch-breaking-changes]
Before you upgrade, carefully review the Elasticsearch breaking changes and take the necessary steps to mitigate any issues.

To learn how to upgrade, check out <upgrade docs>.

% ## Next version [elasticsearch-nextversion-breaking-changes]
% **Release date:** Month day, year

## ${unqualifiedVersion} [elasticsearch-${versionWithoutSeparator}-breaking-changes]
**Release date:** April 01, 2025
<%
    if (!changelogsByTypeByArea['breaking']) {
        print "\nNo breaking changes in this version.\n"
    } else {
        for (team in (changelogsByTypeByArea['breaking'] ?: [:]).keySet()) {
            print "\n${team}:\n";

            for (change in changelogsByTypeByArea['breaking'][team]) {
                print "* ${change.summary} [#${change.pr}](https://github.com/elastic/elasticsearch/pull/${change.pr})"
                if (change.issues != null && change.issues.empty == false) {
                    print change.issues.size() == 1 ? " (issue: " : " (issues: "
                    print change.issues.collect { "{es-issue}${it}[#${it}]" }.join(", ")
                    print ")"
                }
                print "\n"
            }
        }

        print "\n\n"
    }

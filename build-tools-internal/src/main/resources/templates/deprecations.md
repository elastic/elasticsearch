---
navigation_title: "Elasticsearch"
---

# {{es}} deprecations [elasticsearch-deprecations]
Review the deprecated functionality for your {{es}} version. While deprecations have no immediate impact, we strongly encourage you update your implementation after you upgrade.

To learn how to upgrade, check out <upgrade docs>.

To give you insight into what deprecated features you’re using, {{es}}:

* Returns a `Warn` HTTP header whenever you submit a request that uses deprecated functionality.
* [Logs deprecation warnings](docs-content://deploy-manage/monitor/logging-configuration/update-elasticsearch-logging-levels.md#deprecation-logging) when deprecated functionality is used.
* [Provides a deprecation info API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-migration-deprecations) that scans a cluster’s configuration and mappings for deprecated functionality.

% ## Next version [elasticsearch-nextversion-deprecations]
% **Release date:** Month day, year

## ${unqualifiedVersion} [elasticsearch-${versionWithoutSeparator}-deprecations]
**Release date:** April 01, 2025
<%
    if (!changelogsByTypeByArea['deprecation']) {
        print "\nNo deprecations in this version.\n"
    } else {
        for (team in (changelogsByTypeByArea['deprecation'] ?: [:]).keySet()) {
            print "\n${team}:\n";

            for (change in changelogsByTypeByArea['deprecation'][team]) {
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

---
navigation_title: "Deprecations"
---

# {{es}} deprecations [elasticsearch-deprecations]

Over time, certain Elastic functionality becomes outdated and is replaced or removed. To help with the transition, Elastic deprecates functionality for a period before removal, giving you time to update your applications.

Review the deprecated functionality for Elasticsearch. While deprecations have no immediate impact, we strongly encourage you update your implementation after you upgrade. To learn how to upgrade, check out [Upgrade](docs-content://deploy-manage/upgrade.md).

To give you insight into what deprecated features you’re using, {{es}}:

* Returns a `Warn` HTTP header whenever you submit a request that uses deprecated functionality.
* [Logs deprecation warnings](docs-content://deploy-manage/monitor/logging-configuration/update-elasticsearch-logging-levels.md#deprecation-logging) when deprecated functionality is used.
* [Provides a deprecation info API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-migration-deprecations) that scans a cluster’s configuration and mappings for deprecated functionality.

% ## Next version [elasticsearch-nextversion-deprecations]
<%
for(bundle in changelogBundles) {
    def version = bundle.version
    def versionForIds = bundle.version.toString().equals('9.0.0') ? bundle.versionWithoutSeparator : bundle.version
    def changelogsByTypeByArea = bundle.changelogsByTypeByArea
    def unqualifiedVersion = bundle.unqualifiedVersion
    def coming = !bundle.bundle.released

    if (coming) {
        print "\n"
        print "```{applies_to}\n"
        print "stack: coming ${version}\n"
        print "```"
    }
%>
## ${unqualifiedVersion} [elasticsearch-${versionForIds}-deprecations]
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
                    print change.issues.collect { "[#${it}](https://github.com/elastic/elasticsearch/issues/${it})" }.join(", ")
                    print ")"
                }
                print "\n"
            }
        }
        print "\n\n"
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.conventions.precommit;

import com.diffplug.gradle.spotless.SpotlessExtension;
import com.diffplug.gradle.spotless.SpotlessPlugin;

import org.gradle.api.Plugin;
import org.gradle.api.Project;

import java.util.List;

/**
 * This plugin configures formatting for Java source using Spotless
 * for Gradle. Since the act of formatting existing source can interfere
 * with developers' workflows, we don't automatically format all code
 * (yet). Instead, we maintain a list of projects that are excluded from
 * formatting, until we reach a point where we can comfortably format them
 * in one go without too much disruption.
 *
 * <p>Any new sub-projects must not be added to the exclusions list!
 *
 * <p>To perform a reformat, run:
 *
 * <pre>    ./gradlew spotlessApply</pre>
 *
 * <p>To check the current format, run:
 *
 * <pre>    ./gradlew spotlessJavaCheck</pre>
 *
 * <p>This is also carried out by the `precommit` task.
 *
 * <p>See also the <a href="https://github.com/diffplug/spotless/tree/master/plugin-gradle"
 * >Spotless project page</a>.
 */
public class FormattingPrecommitPlugin implements Plugin<Project> {

    @Override
    public void apply(Project project) {
        final boolean shouldFormatProject = PROJECT_PATHS_TO_EXCLUDE.contains(project.getPath()) == false
            || project.getProviders().systemProperty("es.format.everything").forUseAtConfigurationTime().isPresent();

        if (shouldFormatProject) {
            project.getPlugins().apply(PrecommitTaskPlugin.class);
            project.getPlugins().apply(SpotlessPlugin.class);

            project.getExtensions().getByType(SpotlessExtension.class).java(java -> {
                String importOrderPath = "build-conventions/elastic.importorder";
                String formatterConfigPath = "build-conventions/formatterConfig.xml";

                // When applied to e.g. `:build-tools`, we need to modify the path to our config files
                if (project.getRootProject().file(importOrderPath).exists() == false) {
                    importOrderPath = "../" + importOrderPath;
                    formatterConfigPath = "../" + formatterConfigPath;
                }

                java.target(getTargets(project.getPath()));

                // Use `@formatter:off` and `@formatter:on` to toggle formatting - ONLY IF STRICTLY NECESSARY
                java.toggleOffOn("@formatter:off", "@formatter:on");

                java.removeUnusedImports();

                // We enforce a standard order for imports
                java.importOrderFile(project.getRootProject().file(importOrderPath));

                // Most formatting is done through the Eclipse formatter
                java.eclipse().configFile(project.getRootProject().file(formatterConfigPath));

                // Ensure blank lines are actually empty. Since formatters are applied in
                // order, apply this one last, otherwise non-empty blank lines can creep
                // in.
                java.trimTrailingWhitespace();
            });

            project.getTasks().named("precommit").configure(precommitTask -> precommitTask.dependsOn("spotlessJavaCheck"));
        }
    }

    @SuppressWarnings("CheckStyle")
    private Object[] getTargets(String projectPath) {
        if (projectPath.equals(":server")) {
            return new String[] {
                "src/*/java/org/elasticsearch/action/admin/cluster/repositories/**/*.java",
                "src/*/java/org/elasticsearch/action/admin/cluster/snapshots/**/*.java",
                "src/test/java/org/elasticsearch/common/xcontent/support/XContentMapValuesTests.java",
                "src/*/java/org/elasticsearch/index/snapshots/**/*.java",
                "src/*/java/org/elasticsearch/repositories/**/*.java",
                "src/*/java/org/elasticsearch/search/aggregations/**/*.java",
                "src/*/java/org/elasticsearch/snapshots/**/*.java" };
        } else if (projectPath.equals(":test:framework")) {
            return new String[] {
                "src/test/java/org/elasticsearch/common/xcontent/support/AbstractFilteringTestCase.java",
            };
        } else {
            // Normally this isn"t necessary, but we have Java sources in
            // non-standard places
            return new String[] { "src/**/*.java" };
        }
    }

    // Do not add new sub-projects here!
    private static final List<String> PROJECT_PATHS_TO_EXCLUDE = List.of(
        ":client:benchmark",
        ":client:client-benchmark-noop-api-plugin",
        ":client:rest",
        ":client:rest-high-level",
        ":client:rest-high-level:qa:ssl-enabled",
        ":client:sniffer",
        ":client:test",
        ":client:transport",
        ":distribution:archives:integ-test-zip",
        ":distribution:bwc:bugfix",
        ":distribution:bwc:maintenance",
        ":distribution:bwc:minor",
        ":distribution:bwc:staged",
        ":distribution:docker",
        ":docs",
        ":libs:elasticsearch-cli",
        ":libs:elasticsearch-core",
        ":libs:elasticsearch-dissect",
        ":libs:elasticsearch-geo",
        ":libs:elasticsearch-grok",
        ":libs:elasticsearch-lz4",
        ":libs:elasticsearch-nio",
        ":libs:elasticsearch-plugin-classloader",
        ":libs:elasticsearch-secure-sm",
        ":libs:elasticsearch-ssl-config",
        ":libs:elasticsearch-x-content",
        ":modules:analysis-common",
        ":modules:ingest-common",
        ":modules:ingest-geoip",
        ":modules:ingest-geoip:qa:file-based-update",
        ":modules:ingest-user-agent",
        ":modules:lang-expression",
        ":modules:lang-mustache",
        ":modules:lang-painless",
        ":modules:lang-painless:spi",
        ":modules:mapper-extras",
        ":modules:parent-join",
        ":modules:percolator",
        ":modules:rank-eval",
        ":modules:reindex",
        ":modules:repository-url",
        ":modules:systemd",
        ":modules:transport-netty4",
        ":plugins:analysis-icu",
        ":plugins:analysis-kuromoji",
        ":plugins:analysis-nori",
        ":plugins:analysis-phonetic",
        ":plugins:analysis-smartcn",
        ":plugins:analysis-stempel",
        ":plugins:analysis-ukrainian",
        ":plugins:discovery-azure-classic",
        ":plugins:discovery-ec2",
        ":plugins:discovery-ec2:qa:amazon-ec2",
        ":plugins:discovery-gce",
        ":plugins:discovery-gce:qa:gce",
        ":plugins:ingest-attachment",
        ":plugins:mapper-annotated-text",
        ":plugins:mapper-murmur3",
        ":plugins:mapper-size",
        ":plugins:repository-azure",
        ":plugins:repository-gcs",
        ":plugins:repository-hdfs",
        ":plugins:repository-hdfs:hadoop-common",
        ":plugins:repository-s3",
        ":plugins:store-smb",
        ":plugins:transport-nio",
        ":qa:ccs-old-version-remote-cluster",
        ":qa:ccs-rolling-upgrade-remote-cluster",
        ":qa:ccs-unavailable-clusters",
        ":qa:die-with-dignity",
        ":qa:evil-tests",
        ":qa:full-cluster-restart",
        ":qa:logging-config",
        ":qa:mixed-cluster",
        ":qa:multi-cluster-search",
        ":qa:no-bootstrap-tests",
        ":qa:remote-clusters",
        ":qa:repository-multi-version",
        ":qa:rolling-upgrade",
        ":qa:smoke-test-client",
        ":qa:smoke-test-http",
        ":qa:smoke-test-ingest-with-all-dependencies",
        ":qa:smoke-test-multinode",
        ":qa:smoke-test-plugins",
        ":qa:snapshot-based-recoveries",
        ":qa:snapshot-based-recoveries:azure",
        ":qa:snapshot-based-recoveries:fs",
        ":qa:snapshot-based-recoveries:gcs",
        ":qa:snapshot-based-recoveries:s3",
        ":qa:translog-policy",
        ":qa:verify-version-constants",
        ":rest-api-spec",
        ":test:fixtures:geoip-fixture",
        ":test:fixtures:krb5kdc-fixture",
        ":test:fixtures:old-elasticsearch",
        ":test:logger-usage",
        ":x-pack:docs",
        ":x-pack:license-tools",
        ":x-pack:plugin",
        ":x-pack:plugin:async-search",
        ":x-pack:plugin:async-search:qa",
        ":x-pack:plugin:async-search:qa:security",
        ":x-pack:plugin:autoscaling:qa:rest",
        ":x-pack:plugin:ccr",
        ":x-pack:plugin:ccr:qa",
        ":x-pack:plugin:ccr:qa:downgrade-to-basic-license",
        ":x-pack:plugin:ccr:qa:multi-cluster",
        ":x-pack:plugin:ccr:qa:non-compliant-license",
        ":x-pack:plugin:ccr:qa:rest",
        ":x-pack:plugin:ccr:qa:restart",
        ":x-pack:plugin:ccr:qa:security",
        ":x-pack:plugin:core",
        ":x-pack:plugin:data-streams:qa:multi-node",
        ":x-pack:plugin:data-streams:qa:rest",
        ":x-pack:plugin:deprecation",
        ":x-pack:plugin:enrich:qa:common",
        ":x-pack:plugin:enrich:qa:rest",
        ":x-pack:plugin:enrich:qa:rest-with-advanced-security",
        ":x-pack:plugin:enrich:qa:rest-with-security",
        ":x-pack:plugin:eql",
        ":x-pack:plugin:eql:qa",
        ":x-pack:plugin:eql:qa:common",
        ":x-pack:plugin:eql:qa:mixed-node",
        ":x-pack:plugin:eql:qa:multi-cluster-with-security",
        ":x-pack:plugin:eql:qa:rest",
        ":x-pack:plugin:eql:qa:security",
        ":x-pack:plugin:fleet:qa:rest",
        ":x-pack:plugin:graph",
        ":x-pack:plugin:graph:qa:with-security",
        ":x-pack:plugin:identity-provider",
        ":x-pack:plugin:identity-provider:qa:idp-rest-tests",
        ":x-pack:plugin:ilm",
        ":x-pack:plugin:ilm:qa:multi-cluster",
        ":x-pack:plugin:ilm:qa:multi-node",
        ":x-pack:plugin:ilm:qa:rest",
        ":x-pack:plugin:ilm:qa:with-security",
        ":x-pack:plugin:mapper-constant-keyword",
        ":x-pack:plugin:ml",
        ":x-pack:plugin:ml:qa:basic-multi-node",
        ":x-pack:plugin:ml:qa:disabled",
        ":x-pack:plugin:ml:qa:ml-with-security",
        ":x-pack:plugin:ml:qa:native-multi-node-tests",
        ":x-pack:plugin:ml:qa:no-bootstrap-tests",
        ":x-pack:plugin:ml:qa:single-node-tests",
        ":x-pack:plugin:monitoring",
        ":x-pack:plugin:ql",
        ":x-pack:plugin:repositories-metering-api:qa:gcs",
        ":x-pack:plugin:repositories-metering-api:qa:s3",
        ":x-pack:plugin:repository-encrypted:qa:azure",
        ":x-pack:plugin:repository-encrypted:qa:gcs",
        ":x-pack:plugin:repository-encrypted:qa:s3",
        ":x-pack:plugin:rollup:qa:rest",
        ":x-pack:plugin:search-business-rules",
        ":x-pack:plugin:searchable-snapshots:qa:gcs",
        ":x-pack:plugin:searchable-snapshots:qa:rest",
        ":x-pack:plugin:security",
        ":x-pack:plugin:security:cli",
        ":x-pack:plugin:security:qa:basic-enable-security",
        ":x-pack:plugin:security:qa:security-basic",
        ":x-pack:plugin:security:qa:security-disabled",
        ":x-pack:plugin:security:qa:security-not-enabled",
        ":x-pack:plugin:security:qa:security-trial",
        ":x-pack:plugin:security:qa:service-account",
        ":x-pack:plugin:security:qa:smoke-test-all-realms",
        ":x-pack:plugin:security:qa:tls-basic",
        ":x-pack:plugin:shutdown:qa:multi-node",
        ":x-pack:plugin:snapshot-repo-test-kit:qa:rest",
        ":x-pack:plugin:spatial",
        ":x-pack:plugin:sql",
        ":x-pack:plugin:sql:jdbc",
        ":x-pack:plugin:sql:qa",
        ":x-pack:plugin:sql:qa:jdbc",
        ":x-pack:plugin:sql:qa:jdbc:security",
        ":x-pack:plugin:sql:qa:mixed-node",
        ":x-pack:plugin:sql:qa:security",
        ":x-pack:plugin:sql:qa:server:multi-node",
        ":x-pack:plugin:sql:qa:server:single-node",
        ":x-pack:plugin:sql:sql-action",
        ":x-pack:plugin:sql:sql-cli",
        ":x-pack:plugin:sql:sql-client",
        ":x-pack:plugin:sql:sql-proto",
        ":x-pack:plugin:stack:qa:rest",
        ":x-pack:plugin:text-structure:qa:text-structure-with-security",
        ":x-pack:plugin:transform",
        ":x-pack:plugin:transform:qa:multi-cluster-tests-with-security",
        ":x-pack:plugin:transform:qa:multi-node-tests",
        ":x-pack:plugin:transform:qa:single-node-tests",
        ":x-pack:plugin:vector-tile:qa:multi-cluster",
        ":x-pack:plugin:vectors",
        ":x-pack:plugin:watcher",
        ":x-pack:plugin:watcher:qa:rest",
        ":x-pack:plugin:watcher:qa:with-monitoring",
        ":x-pack:plugin:watcher:qa:with-security",
        ":x-pack:plugin:wildcard",
        ":x-pack:qa",
        ":x-pack:qa:core-rest-tests-with-security",
        ":x-pack:qa:evil-tests",
        ":x-pack:qa:full-cluster-restart",
        ":x-pack:qa:kerberos-tests",
        ":x-pack:qa:mixed-tier-cluster",
        ":x-pack:qa:multi-cluster-search-security",
        ":x-pack:qa:multi-node",
        ":x-pack:qa:oidc-op-tests",
        ":x-pack:qa:openldap-tests",
        ":x-pack:qa:password-protected-keystore",
        ":x-pack:qa:reindex-tests-with-security",
        ":x-pack:qa:rolling-upgrade",
        ":x-pack:qa:rolling-upgrade-multi-cluster",
        ":x-pack:qa:runtime-fields:core-with-mapped",
        ":x-pack:qa:runtime-fields:core-with-search",
        ":x-pack:qa:runtime-fields:with-security",
        ":x-pack:qa:saml-idp-tests",
        ":x-pack:qa:security-client-tests",
        ":x-pack:qa:security-example-spi-extension",
        ":x-pack:qa:security-migrate-tests",
        ":x-pack:qa:security-setup-password-tests",
        ":x-pack:qa:security-tools-tests",
        ":x-pack:qa:smoke-test-plugins",
        ":x-pack:qa:smoke-test-plugins-ssl",
        ":x-pack:qa:smoke-test-security-with-mustache",
        ":x-pack:qa:third-party:active-directory",
        ":x-pack:qa:third-party:jira",
        ":x-pack:qa:third-party:pagerduty",
        ":x-pack:qa:third-party:slack",
        ":x-pack:qa:transport-client-tests",
        ":x-pack:snapshot-tool",
        ":x-pack:snapshot-tool:qa:google-cloud-storage",
        ":x-pack:snapshot-tool:qa:s3",
        ":x-pack:test:feature-aware",
        ":x-pack:test:idp-fixture",
        ":x-pack:test:smb-fixture",
        ":x-pack:transport-client"
    );
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

import org.elasticsearch.gradle.internal.test.LegacyRestTestBasePlugin;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.services.BuildService;
import org.gradle.api.services.BuildServiceParameters;

public class DeprecationService implements BuildService<BuildServiceParameters.None> {

    private static ListMultimap<Class<?>, String> usageWhitelist = ArrayListMultimap.create();

    public DeprecationService() {
        addLegacyRestTestBasePluginUsageLimitations();
    }

    private static void addLegacyRestTestBasePluginUsageLimitations() {
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":docs");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":rest-api-spec");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":distribution:docker");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":modules:aggregations");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":modules:analysis-common");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":modules:data-streams");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":modules:ingest-attachment");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":modules:ingest-common");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":modules:ingest-geoip");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":modules:ingest-user-agent");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":modules:kibana");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":modules:lang-expression");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":modules:lang-mustache");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":modules:lang-painless");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":modules:mapper-extras");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":modules:parent-join");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":modules:percolator");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":modules:rank-eval");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":modules:reindex");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":modules:repository-s3");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":modules:repository-url");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":modules:rest-root");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":modules:runtime-fields-common");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":modules:transport-netty4");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":plugins:analysis-icu");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":plugins:analysis-kuromoji");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":plugins:analysis-nori");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":plugins:analysis-phonetic");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":plugins:analysis-smartcn");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":plugins:analysis-stempel");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":plugins:analysis-ukrainian");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":plugins:discovery-azure-classic");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":plugins:discovery-ec2");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":plugins:discovery-gce");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":plugins:mapper-annotated-text");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":plugins:mapper-murmur3");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":plugins:mapper-size");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":plugins:repository-hdfs");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":plugins:store-smb");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":qa:ccs-rolling-upgrade-remote-cluster");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":qa:ccs-unavailable-clusters");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":qa:logging-config");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":qa:mixed-cluster");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":qa:multi-cluster-search");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":qa:remote-clusters");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":qa:repository-multi-version");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":qa:rolling-upgrade");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":qa:smoke-test-http");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":qa:smoke-test-ingest-disabled");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":qa:smoke-test-ingest-with-all-dependencies");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":qa:smoke-test-plugins");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":qa:system-indices");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":qa:unconfigured-node-name");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":qa:verify-version-constants");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":test:external-modules:test-delayed-aggs");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":test:external-modules:test-die-with-dignity");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":test:external-modules:test-error-query");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":test:external-modules:test-latency-simulating-directory");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":test:external-modules:test-seek-tracking-directory");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":test:yaml-rest-runner");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":distribution:archives:integ-test-zip");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:core");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:ent-search");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:fleet");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:logstash");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:mapper-constant-keyword");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:mapper-unsigned-long");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:mapper-version");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:vector-tile");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:wildcard");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:qa:kerberos-tests");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:qa:mixed-tier-cluster");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:qa:password-protected-keystore");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:qa:reindex-tests-with-security");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:qa:repository-old-versions");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:qa:rolling-upgrade");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:qa:rolling-upgrade-basic");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:qa:rolling-upgrade-multi-cluster");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:qa:runtime-fields:core-with-mapped");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:qa:runtime-fields:core-with-search");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:qa:saml-idp-tests");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:qa:security-example-spi-extension");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:qa:security-setup-password-tests");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:qa:smoke-test-plugins");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:qa:smoke-test-plugins-ssl");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:qa:smoke-test-security-with-mustache");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:qa:xpack-prefix-rest-compat");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":modules:ingest-geoip:qa:file-based-update");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":plugins:discovery-ec2:qa:amazon-ec2");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":plugins:discovery-gce:qa:gce");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:qa:multi-cluster-search-security:legacy-with-basic-license");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:qa:multi-cluster-search-security:legacy-with-full-license");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:qa:multi-cluster-search-security:legacy-with-restricted-trust");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:qa:runtime-fields:with-security");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:qa:third-party:jira");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:qa:third-party:pagerduty");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:qa:third-party:slack");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:async-search:qa:rest");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:async-search:qa:security");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:autoscaling:qa:rest");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:ccr:qa:downgrade-to-basic-license");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:ccr:qa:multi-cluster");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:ccr:qa:non-compliant-license");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:ccr:qa:rest");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:ccr:qa:restart");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:ccr:qa:security");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:deprecation:qa:early-deprecation-rest");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:deprecation:qa:rest");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:downsample:qa:rest");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:downsample:qa:with-security");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:enrich:qa:rest");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:enrich:qa:rest-with-advanced-security");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:enrich:qa:rest-with-security");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:ent-search:qa:rest");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:eql:qa:ccs-rolling-upgrade");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:eql:qa:correctness");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:eql:qa:mixed-node");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:eql:qa:multi-cluster-with-security");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:eql:qa:rest");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:esql:qa:security");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:esql:qa:server:multi-node");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:esql:qa:server:single-node");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:fleet:qa:rest");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:graph:qa:with-security");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:identity-provider:qa:idp-rest-tests");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:ilm:qa:multi-cluster");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:ilm:qa:multi-node");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:ilm:qa:rest");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:ilm:qa:with-security");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:ml:qa:basic-multi-node");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:ml:qa:disabled");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:ml:qa:ml-with-security");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:ml:qa:multi-cluster-tests-with-security");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:ml:qa:native-multi-node-tests");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:ml:qa:single-node-tests");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:repositories-metering-api:qa:s3");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:searchable-snapshots:qa:hdfs");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:searchable-snapshots:qa:minio");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:searchable-snapshots:qa:rest");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:searchable-snapshots:qa:s3");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:searchable-snapshots:qa:url");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:security:qa:operator-privileges-tests");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:security:qa:profile");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:security:qa:security-disabled");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:security:qa:smoke-test-all-realms");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:security:qa:tls-basic");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:shutdown:qa:multi-node");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:shutdown:qa:rolling-upgrade");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:slm:qa:multi-node");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:slm:qa:rest");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:slm:qa:with-security");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:snapshot-based-recoveries:qa:fs");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:snapshot-based-recoveries:qa:license-enforcing");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:snapshot-based-recoveries:qa:s3");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:snapshot-repo-test-kit:qa:hdfs");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:snapshot-repo-test-kit:qa:minio");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:snapshot-repo-test-kit:qa:rest");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:snapshot-repo-test-kit:qa:s3");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:sql:qa:jdbc:multi-node");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:sql:qa:jdbc:no-sql");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:sql:qa:jdbc:single-node");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:sql:qa:jdbc:security:with-ssl");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:sql:qa:jdbc:security:without-ssl");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:sql:qa:mixed-node");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:sql:qa:server:multi-cluster-with-security");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:sql:qa:server:multi-node");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:sql:qa:server:single-node");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:sql:qa:server:security:with-ssl");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:sql:qa:server:security:without-ssl");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:stack:qa:rest");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:text-structure:qa:text-structure-with-security");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:transform:qa:multi-cluster-tests-with-security");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:transform:qa:multi-node-tests");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:transform:qa:single-node-tests");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:vector-tile:qa:multi-cluster");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:watcher:qa:rest");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:watcher:qa:with-monitoring");
        usageWhitelist.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:watcher:qa:with-security");
    }

    @Override
    public BuildServiceParameters.None getParameters() {
        return null;
    }

    public void failOnDeprecatedUsage(Class<?> aClass, Project project) {
        if (isSupported(aClass, project.getPath()) == false) {
            throw new GradleException("Deprecated usage of " + aClass.getName() + " in " + project.getPath());
        }

    }

    private boolean isSupported(Class<?> aClass, String path) {
        return usageWhitelist.get(aClass).contains(path);
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

import org.elasticsearch.gradle.internal.test.LegacyRestTestBasePlugin;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.provider.Property;
import org.gradle.api.services.BuildService;
import org.gradle.api.services.BuildServiceParameters;

public abstract class RestrictedBuildApiService implements BuildService<RestrictedBuildApiService.Params> {

    public static final String BUILD_API_RESTRICTIONS_SYS_PROPERTY = "org.elasticsearch.gradle.build-api-restriction.disabled";

    private static ListMultimap<Class<?>, String> usageWhitelist = createLegacyRestTestBasePluginUsage();

    private static ListMultimap<Class<?>, String> createLegacyRestTestBasePluginUsage() {
        ListMultimap<Class<?>, String> map = ArrayListMultimap.create(1, 200);
        map.put(LegacyRestTestBasePlugin.class, ":docs");
        map.put(LegacyRestTestBasePlugin.class, ":distribution:docker");
        map.put(LegacyRestTestBasePlugin.class, ":modules:reindex");
        map.put(LegacyRestTestBasePlugin.class, ":qa:ccs-rolling-upgrade-remote-cluster");
        map.put(LegacyRestTestBasePlugin.class, ":qa:mixed-cluster");
        map.put(LegacyRestTestBasePlugin.class, ":qa:multi-cluster-search");
        map.put(LegacyRestTestBasePlugin.class, ":qa:remote-clusters");
        map.put(LegacyRestTestBasePlugin.class, ":qa:repository-multi-version");
        map.put(LegacyRestTestBasePlugin.class, ":qa:rolling-upgrade-legacy");
        map.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:ent-search");
        map.put(LegacyRestTestBasePlugin.class, ":x-pack:qa:mixed-tier-cluster");
        map.put(LegacyRestTestBasePlugin.class, ":x-pack:qa:repository-old-versions");
        map.put(LegacyRestTestBasePlugin.class, ":x-pack:qa:rolling-upgrade");
        map.put(LegacyRestTestBasePlugin.class, ":x-pack:qa:rolling-upgrade-basic");
        map.put(LegacyRestTestBasePlugin.class, ":x-pack:qa:rolling-upgrade-multi-cluster");
        map.put(LegacyRestTestBasePlugin.class, ":x-pack:qa:runtime-fields:core-with-mapped");
        map.put(LegacyRestTestBasePlugin.class, ":x-pack:qa:runtime-fields:core-with-search");
        map.put(LegacyRestTestBasePlugin.class, ":x-pack:qa:security-example-spi-extension");
        map.put(LegacyRestTestBasePlugin.class, ":x-pack:qa:security-setup-password-tests");
        map.put(LegacyRestTestBasePlugin.class, ":x-pack:qa:smoke-test-plugins");
        map.put(LegacyRestTestBasePlugin.class, ":x-pack:qa:smoke-test-plugins-ssl");
        map.put(LegacyRestTestBasePlugin.class, ":x-pack:qa:smoke-test-security-with-mustache");
        map.put(LegacyRestTestBasePlugin.class, ":x-pack:qa:xpack-prefix-rest-compat");
        map.put(LegacyRestTestBasePlugin.class, ":x-pack:qa:multi-cluster-search-security:legacy-with-basic-license");
        map.put(LegacyRestTestBasePlugin.class, ":x-pack:qa:multi-cluster-search-security:legacy-with-full-license");
        map.put(LegacyRestTestBasePlugin.class, ":x-pack:qa:multi-cluster-search-security:legacy-with-restricted-trust");
        map.put(LegacyRestTestBasePlugin.class, ":x-pack:qa:third-party:jira");
        map.put(LegacyRestTestBasePlugin.class, ":x-pack:qa:third-party:pagerduty");
        map.put(LegacyRestTestBasePlugin.class, ":x-pack:qa:third-party:slack");
        map.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:async-search:qa:rest");
        map.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:autoscaling:qa:rest");
        map.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:deprecation:qa:early-deprecation-rest");
        map.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:deprecation:qa:rest");
        map.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:downsample:qa:with-security");
        map.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:enrich:qa:rest");
        map.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:enrich:qa:rest-with-advanced-security");
        map.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:enrich:qa:rest-with-security");
        map.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:ent-search:qa:rest");
        map.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:eql:qa:ccs-rolling-upgrade");
        map.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:eql:qa:correctness");
        map.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:eql:qa:mixed-node");
        map.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:identity-provider:qa:idp-rest-tests");
        map.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:ilm:qa:multi-cluster");
        map.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:ilm:qa:multi-node");
        map.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:ilm:qa:rest");
        map.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:ml:qa:basic-multi-node");
        map.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:ml:qa:disabled");
        map.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:ml:qa:ml-with-security");
        map.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:ml:qa:multi-cluster-tests-with-security");
        map.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:ml:qa:native-multi-node-tests");
        map.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:ml:qa:single-node-tests");
        map.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:searchable-snapshots:qa:hdfs");
        map.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:searchable-snapshots:qa:rest");
        map.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:searchable-snapshots:qa:url");
        map.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:security:qa:tls-basic");
        map.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:shutdown:qa:multi-node");
        map.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:shutdown:qa:rolling-upgrade");
        map.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:snapshot-based-recoveries:qa:fs");
        map.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:snapshot-based-recoveries:qa:license-enforcing");
        map.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:snapshot-repo-test-kit:qa:hdfs");
        map.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:snapshot-repo-test-kit:qa:rest");
        map.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:sql:qa:jdbc:multi-node");
        map.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:sql:qa:jdbc:no-sql");
        map.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:sql:qa:jdbc:single-node");
        map.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:sql:qa:jdbc:security:with-ssl");
        map.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:sql:qa:jdbc:security:without-ssl");
        map.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:sql:qa:mixed-node");
        map.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:sql:qa:server:security:with-ssl");
        map.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:sql:qa:server:security:without-ssl");
        map.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:text-structure:qa:text-structure-with-security");
        map.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:transform:qa:multi-cluster-tests-with-security");
        map.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:transform:qa:multi-node-tests");
        map.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:transform:qa:single-node-tests");
        map.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:watcher:qa:rest");
        map.put(LegacyRestTestBasePlugin.class, ":x-pack:plugin:watcher:qa:with-security");
        return map;
    }

    public void failOnUsageRestriction(Class<?> aClass, Project project) {
        if (getParameters().getDisabled().getOrElse(false)) {
            return;
        }
        if (isSupported(aClass, project.getPath()) == false) {
            throw new GradleException("Usage of deprecated " + aClass.getName() + " in " + project.getPath());
        }
    }

    private boolean isSupported(Class<?> aClass, String path) {
        return usageWhitelist.get(aClass).contains(path);
    }

    public abstract static class Params implements BuildServiceParameters {
        public abstract Property<Boolean> getDisabled();
    }
}

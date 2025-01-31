/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.qa;

import org.elasticsearch.common.Strings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.PluginInstallSpec;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.yaml.YamlXContent;
import org.junit.rules.ExternalResource;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

class EntitlementsTestRule implements TestRule {

    interface PolicyBuilder {
        void build(XContentBuilder builder, Path tempDir) throws IOException;
    }

    final TemporaryFolder testDir;
    final ElasticsearchCluster cluster;
    final TestRule ruleChain;

    @SuppressWarnings("this-escape")
    EntitlementsTestRule(boolean modular, PolicyBuilder policyBuilder) {
        testDir = new TemporaryFolder();
        var tempDirSetup = new ExternalResource() {
            @Override
            protected void before() throws Throwable {
                Path testPath = testDir.getRoot().toPath();
                Files.createDirectory(testPath.resolve("read_dir"));
                Files.createDirectory(testPath.resolve("read_write_dir"));
                Files.writeString(testPath.resolve("read_file"), "");
                Files.writeString(testPath.resolve("read_write_file"), "");
            }
        };
        cluster = ElasticsearchCluster.local()
            .module("entitled")
            .module("entitlement-test-plugin", spec -> setupEntitlements(spec, modular, policyBuilder))
            .systemProperty("es.entitlements.enabled", "true")
            .systemProperty("es.entitlements.testdir", () -> testDir.getRoot().getAbsolutePath())
            .setting("xpack.security.enabled", "false")
            .build();
        ruleChain = RuleChain.outerRule(testDir).around(tempDirSetup).around(cluster);
    }

    @Override
    public Statement apply(Statement statement, Description description) {
        return ruleChain.apply(statement, description);
    }

    private void setupEntitlements(PluginInstallSpec spec, boolean modular, PolicyBuilder policyBuilder) {
        String moduleName = modular ? "org.elasticsearch.entitlement.qa.test" : "ALL-UNNAMED";
        if (policyBuilder != null) {
            spec.withEntitlementsOverride(old -> {
                try {
                    try (var builder = YamlXContent.contentBuilder()) {
                        builder.startObject();
                        builder.field(moduleName);
                        builder.startArray();

                        policyBuilder.build(builder, testDir.getRoot().toPath());
                        builder.endArray();
                        builder.endObject();

                        String policy = Strings.toString(builder);
                        System.out.println("Using entitlement policy:\n" + policy);
                        return Resource.fromString(policy);
                    }

                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        }

        if (modular == false) {
            spec.withPropertiesOverride(old -> {
                String props = old.replace("modulename=org.elasticsearch.entitlement.qa.test", "");
                System.out.println("Using plugin properties:\n" + props);
                return Resource.fromString(props);
            });
        }
    }
}

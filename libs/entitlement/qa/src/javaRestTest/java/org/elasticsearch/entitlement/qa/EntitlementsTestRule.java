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
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.PluginInstallSpec;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.yaml.YamlXContent;
import org.junit.rules.ExternalResource;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

class EntitlementsTestRule implements TestRule {

    private static final Logger logger = LogManager.getLogger(EntitlementsTestRule.class);

    // entitlements that test methods may use, see EntitledActions
    private static final PolicyBuilder ENTITLED_POLICY = (builder, tempDir) -> {
        builder.value("manage_threads");
        builder.value("outbound_network");
        builder.value(
            Map.of(
                "files",
                List.of(
                    Map.of("path", tempDir.resolve("read_dir"), "mode", "read_write"),
                    Map.of("path", tempDir.resolve("read_dir").resolve("k8s").resolve("..data"), "mode", "read", "exclusive", true),
                    Map.of("path", tempDir.resolve("read_write_dir"), "mode", "read_write"),
                    Map.of("path", tempDir.resolve("read_file"), "mode", "read"),
                    Map.of("path", tempDir.resolve("read_write_file"), "mode", "read_write")
                )
            )
        );
    };
    public static final String ENTITLEMENT_QA_TEST_MODULE_NAME = "org.elasticsearch.entitlement.qa.test";
    public static final String ENTITLEMENT_TEST_PLUGIN_NAME = "entitlement-test-plugin";

    interface PolicyBuilder {
        void build(XContentBuilder builder, Path tempDir) throws IOException;
    }

    interface TempDirSystemPropertyProvider {
        Map<String, String> get(Path tempDir);
    }

    Path testDir;
    final ElasticsearchCluster cluster;
    final TestRule ruleChain;
    private final String overrideDescriptorName;

    EntitlementsTestRule(boolean modular, PolicyBuilder policyBuilder) {
        this(modular, policyBuilder, tempDir -> Map.of(), null);
    }

    EntitlementsTestRule(boolean modular, PolicyBuilder policyBuilder, TempDirSystemPropertyProvider tempDirSystemPropertyProvider) {
        this(modular, policyBuilder, tempDirSystemPropertyProvider, null);
    }

    /**
     * @param overrideDescriptorName if non-null, rewrites the test plugin's descriptor {@code name=}
     *                               to this value (the install directory stays {@link #ENTITLEMENT_TEST_PLUGIN_NAME}).
     */
    @SuppressWarnings("this-escape")
    EntitlementsTestRule(
        boolean modular,
        PolicyBuilder policyBuilder,
        TempDirSystemPropertyProvider tempDirSystemPropertyProvider,
        String overrideDescriptorName
    ) {
        this.overrideDescriptorName = overrideDescriptorName;
        var tempDirSetup = new ExternalResource() {
            @Override
            protected void before() throws Throwable {
                testDir = Files.createTempDirectory(PathUtils.get(System.getProperty("java.io.tmpdir")), "entitlements-test-rule");
                Files.createDirectory(testDir.resolve("read_dir"));
                Files.createDirectory(testDir.resolve("read_write_dir"));
                Files.writeString(testDir.resolve("read_file"), "");
                Files.writeString(testDir.resolve("read_write_file"), "");
            }

            @Override
            protected void after() {
                if (testDir == null) {
                    return;
                }
                try (var paths = Files.walk(testDir)) {
                    paths.sorted(Comparator.reverseOrder()).forEach(path -> {
                        try {
                            Files.deleteIfExists(path);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    });
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        };
        cluster = ElasticsearchCluster.local()
            .module("entitled", spec -> buildEntitlements(spec, "org.elasticsearch.entitlement.qa.entitled", ENTITLED_POLICY))
            .module(ENTITLEMENT_TEST_PLUGIN_NAME, spec -> setupEntitlements(spec, modular, policyBuilder))
            .systemProperty("es.entitlements.verify_bytecode", "true")
            .systemProperty("es.entitlements.testdir", () -> testDir.toAbsolutePath().toString())
            .systemProperties(spec -> tempDirSystemPropertyProvider.get(testDir))
            .setting("xpack.security.enabled", "false")
            // Logs in libs/entitlement/qa/build/test-results/javaRestTest/TEST-org.elasticsearch.entitlement.qa.EntitlementsXXX.xml
            // .setting("logger.org.elasticsearch.entitlement", "DEBUG")
            .build();
        ruleChain = RuleChain.outerRule(tempDirSetup).around(cluster);
    }

    @Override
    public Statement apply(Statement statement, Description description) {
        return ruleChain.apply(statement, description);
    }

    private void buildEntitlements(PluginInstallSpec spec, String moduleName, PolicyBuilder policyBuilder) {
        spec.withEntitlementsOverride(old -> {
            try (var builder = YamlXContent.contentBuilder()) {
                builder.startObject();
                builder.field(moduleName);
                builder.startArray();

                policyBuilder.build(builder, testDir);
                builder.endArray();
                builder.endObject();

                String policy = Strings.toString(builder);
                logger.info("Using entitlement policy for module [{}]:\n{}", moduleName, policy);
                return Resource.fromString(policy);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    private void setupEntitlements(PluginInstallSpec spec, boolean modular, PolicyBuilder policyBuilder) {
        String moduleName = modular ? ENTITLEMENT_QA_TEST_MODULE_NAME : "ALL-UNNAMED";
        if (policyBuilder != null) {
            buildEntitlements(spec, moduleName, policyBuilder);
        }

        boolean rewriteModulename = (modular == false);
        boolean rewriteName = overrideDescriptorName != null;
        if (rewriteModulename || rewriteName) {
            spec.withPropertiesOverride(old -> {
                String props = old;
                if (rewriteModulename) {
                    props = props.replace("modulename=" + ENTITLEMENT_QA_TEST_MODULE_NAME, "");
                }
                if (rewriteName) {
                    props = props.replaceAll("(?m)^name=.*$", "name=" + overrideDescriptorName);
                }
                logger.info("Using plugin properties:\n{}", props);
                return Resource.fromString(props);
            });
        }
    }
}

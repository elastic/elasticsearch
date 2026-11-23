/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.ClasspathUtils;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ClientYamlTestClient;
import org.elasticsearch.test.rest.yaml.ClientYamlTestExecutionContext;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.elasticsearch.test.rest.yaml.restspec.ClientYamlSuiteRestSpec;
import org.elasticsearch.test.rest.yaml.section.ClientYamlTestSuite;
import org.elasticsearch.test.rest.yaml.section.ExecutableSection;
import org.junit.AssumptionViolatedException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.rest.yaml.section.ExecutableSection.XCONTENT_REGISTRY;

/**
 * Runs {@code remote_cluster/10_basic.yml} against the CCS remote once per JVM so Gradle no longer needs a
 * separate {@code #remote-cluster} / {@code #rev-remote-setup} task chain.
 */
public final class MultiClusterRemoteYamlSeed {

    private static final Logger logger = LogManager.getLogger(MultiClusterRemoteYamlSeed.class);
    private static final Object LOCK = new Object();
    private static boolean seeded;

    private MultiClusterRemoteYamlSeed() {}

    /** Idempotent: safe from every test class {@code @BeforeClass} in this module. */
    public static void ensureSeeded() throws Exception {
        if ("multi_cluster".equals(System.getProperty(ESClientYamlSuiteTestCase.REST_TESTS_SUITE)) == false) {
            return;
        }
        synchronized (LOCK) {
            if (seeded) {
                return;
            }
            run();
            seeded = true;
        }
    }

    private static void run() throws Exception {
        List<HttpHost> hosts = MultiClusterSearchClusters.remoteClusterHosts();
        ClientYamlSuiteRestSpec restSpec = ClientYamlSuiteRestSpec.load("rest-api-spec/api");
        Path yamlFile = findYamlFile("remote_cluster/10_basic.yml");
        ClientYamlTestSuite suite = ClientYamlTestSuite.parse(XCONTENT_REGISTRY, "remote_cluster", yamlFile, Map.of());
        suite.validate();

        try (
            RestClient remoteClient = RestClient.builder(hosts.toArray(HttpHost[]::new)).build();
            ClientYamlTestClient yamlClient = new ClientYamlTestClient(
                restSpec,
                remoteClient,
                hosts,
                (CheckedSupplier<RestClientBuilder, IOException>) () -> RestClient.builder(hosts.toArray(HttpHost[]::new))
            )
        ) {
            Set<String> versions = ESRestTestCase.readVersionsFromNodesInfo(remoteClient);
            var testFeatureService = ESRestTestCase.newYamlTestFeatureServiceForCluster(remoteClient, versions);
            String os = ESClientYamlSuiteTestCase.readOsFromNodesInfo(remoteClient);
            Set<String> osSet = Set.of(os);

            for (var testSection : suite.getTestSections()) {
                ClientYamlTestCandidate candidate = new ClientYamlTestCandidate(suite, testSection);
                ClientYamlTestExecutionContext ctx = new ClientYamlTestExecutionContext(
                    candidate,
                    yamlClient,
                    false,
                    versions,
                    testFeatureService,
                    osSet
                );
                try {
                    testSection.getPrerequisiteSection().evaluate(ctx, candidate.getTestPath());
                } catch (AssumptionViolatedException e) {
                    logger.debug("skipping seeding section [{}]: {}", testSection.getName(), e.getMessage());
                    continue;
                }
                for (ExecutableSection executableSection : testSection.getExecutableSections()) {
                    executableSection.execute(ctx);
                }
                for (ExecutableSection teardownDo : suite.getTeardownSection().getDoSections()) {
                    teardownDo.execute(ctx);
                }
                ctx.clear();
            }
        }
    }

    private static Path findYamlFile(String relativeUnderRestApiSpecTest) throws Exception {
        Path[] roots = ClasspathUtils.findFilePaths(MultiClusterRemoteYamlSeed.class.getClassLoader(), "rest-api-spec/test");
        for (Path root : roots) {
            Path candidate = root;
            for (String part : relativeUnderRestApiSpecTest.split("/")) {
                candidate = candidate.resolve(part);
            }
            if (Files.isRegularFile(candidate)) {
                return candidate;
            }
        }
        throw new IllegalStateException("Could not locate YAML at rest-api-spec/test/" + relativeUnderRestApiSpecTest);
    }

}

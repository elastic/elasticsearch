/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.upgrades;

import org.elasticsearch.Build;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.Version;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public abstract class AbstractXpackRollingUpgradeTestCase extends ParameterizedRollingUpgradeTestCase {

    private static final TemporaryFolder repoDirectory = new TemporaryFolder();

    private static final ElasticsearchCluster cluster = buildCluster();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(repoDirectory).around(cluster);

    private static ElasticsearchCluster buildCluster() {
        var cluster = ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .version(getOldClusterVersion(), isOldClusterDetachedVersion())
            .nodes(NODE_NUM)
            .setting("xpack.license.self_generated.type", "trial")
            .setting("xpack.security.enabled", "false")
            .systemProperty("ingest.geoip.downloader.enabled.default", "true")
            .systemProperty("ingest.geoip.downloader.endpoint.default", "http://invalid.endpoint")
            .setting("ingest.geoip.downloader.endpoint", "http://invalid.endpoint")
            .setting("path.repo", new Supplier<>() {
                @Override
                @SuppressForbidden(reason = "TemporaryFolder only has io.File methods, not nio.File")
                public String get() {
                    return repoDirectory.getRoot().getPath();
                }
            })
            .setting("xpack.searchable.snapshot.shared_cache.size", "16MB")
            .setting("xpack.searchable.snapshot.shared_cache.region_size", "256KB");

        // Avoid triggering bogus assertion when serialized parsed mappings don't match with original mappings, because _source key is
        // inconsistent. As usual, we operate under the premise that "versionless" clusters (serverless) are on the latest code and
        // do not need this.
        if (Version.tryParse(getOldClusterVersion()).map(v -> v.before(Version.fromString("8.18.0"))).orElse(false)) {
            cluster.jvmArg("-da:org.elasticsearch.index.mapper.DocumentMapper");
            cluster.jvmArg("-da:org.elasticsearch.index.mapper.MapperService");
        }
        return cluster.build();
    }

    public AbstractXpackRollingUpgradeTestCase(int upgradedNodes) {
        super(upgradedNodes);
    }

    @Override
    protected ElasticsearchCluster getUpgradeCluster() {
        return cluster;
    }

    protected static boolean isOriginalCluster(String clusterVersion) {
        return getOldClusterVersion().equals(clusterVersion);
    }

    /**
     * Upgrade tests by design are also executed with the same version. We might want to skip some checks if that's the case, see
     * for example gh#39102.
     * @return true if the cluster version is the current version.
     */
    protected static boolean isOriginalClusterCurrent() {
        return getOldClusterVersion().equals(Build.current().version());
    }

    /**
     * ML tests may need to be skipped on hosts where the old cluster version and the installed glibc are incompatible,
     * see {@code BwcVersions#isMlCompatible} in the build logic. The build wires this into the {@code tests.ml.skip}
     * system property per BWC version.
     */
    protected static boolean skipMlTests() {
        return Booleans.parseBoolean(System.getProperty("tests.ml.skip", "false"));
    }

    protected Collection<String> templatesToWaitFor() {
        return Collections.emptyList();
    }

    @Before
    public void setupForTests() throws Exception {
        final Collection<String> expectedTemplates = templatesToWaitFor();

        if (expectedTemplates.isEmpty()) {
            return;
        }
        assertBusy(() -> {
            final Request catRequest = new Request("GET", "_cat/templates?h=n&s=n");
            final Response catResponse = adminClient().performRequest(catRequest);

            final List<String> templates = Streams.readAllLines(catResponse.getEntity().getContent());

            final List<String> missingTemplates = expectedTemplates.stream()
                .filter(each -> templates.contains(each) == false)
                .collect(Collectors.toList());

            // While it's possible to use a Hamcrest matcher for this, the failure is much less legible.
            if (missingTemplates.isEmpty() == false) {
                fail("Some expected templates are missing: " + missingTemplates + ". The templates that exist are: " + templates + "");
            }
        });
    }
}

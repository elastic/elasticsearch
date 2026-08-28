/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.upgrades;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.Version;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.notNullValue;

public abstract class AbstractXpackRollingUpgradeWithSecurityTestCase extends ParameterizedRollingUpgradeTestCase {

    protected static final String USER = "test_user";
    protected static final String PASS = "x-pack-test-password";

    private static final ElasticsearchCluster cluster = buildCluster();

    @ClassRule
    public static ElasticsearchCluster clusterRule = cluster;

    private static ElasticsearchCluster buildCluster() {
        var builder = ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .version(getOldClusterVersion())
            .nodes(NODE_NUM)
            .user(USER, PASS)
            .setting("xpack.license.self_generated.type", "trial")
            .setting("xpack.security.enabled", "true")
            .setting("xpack.security.autoconfiguration.enabled", "false")
            .setting("xpack.security.transport.ssl.enabled", "true")
            .setting("xpack.security.transport.ssl.key", "testnode.pem")
            .setting("xpack.security.transport.ssl.certificate", "testnode.crt")
            .setting("xpack.security.authc.token.enabled", "true")
            .setting("xpack.security.authc.token.timeout", "60m")
            .setting("xpack.security.authc.api_key.enabled", "true")
            .setting("xpack.security.audit.enabled", "true")
            .setting("xpack.security.authc.realms.file.file1.order", "0")
            .setting("xpack.security.authc.realms.native.native1.order", "1")
            .setting("xpack.watcher.encrypt_sensitive_data", "true")
            .setting("logger.org.elasticsearch.xpack.watcher", "DEBUG")
            .configFile("testnode.pem", Resource.fromClasspath("org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.pem"))
            .configFile("testnode.crt", Resource.fromClasspath("org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"))
            .keystore("xpack.security.transport.ssl.secure_key_passphrase", "testnode")
            .keystore("xpack.watcher.encryption_key", Resource.fromClasspath("system_key"));

        if (getOldClusterTestVersion().before(Version.fromString("8.18.0"))) {
            builder.jvmArg("-da:org.elasticsearch.index.mapper.DocumentMapper");
            builder.jvmArg("-da:org.elasticsearch.index.mapper.MapperService");
        }

        return builder.build();
    }

    protected AbstractXpackRollingUpgradeWithSecurityTestCase(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    @Override
    protected ElasticsearchCluster getUpgradeCluster() {
        return cluster;
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(USER, new SecureString(PASS.toCharArray()));
        return Settings.builder().put(super.restClientSettings()).put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    protected RestClient oldVersionClient = null;
    protected RestClient newVersionClient = null;

    protected void closeClientsByVersion() throws IOException {
        if (oldVersionClient != null) {
            oldVersionClient.close();
            oldVersionClient = null;
        }
        if (newVersionClient != null) {
            newVersionClient.close();
            newVersionClient = null;
        }
    }

    protected void createClientsByCapability(Predicate<NodeInfo> capabilityChecker) throws IOException {
        var testNodesByCapability = NodeInfo.getAll(adminClient()).stream().collect(Collectors.partitioningBy(capabilityChecker));
        oldVersionClient = buildClient(
            restClientSettings(),
            new HttpHost[] { HttpHost.create(testNodesByCapability.get(false).get(0).restEndpoint()) }
        );
        newVersionClient = buildClient(
            restClientSettings(),
            new HttpHost[] { HttpHost.create(testNodesByCapability.get(true).get(0).restEndpoint()) }
        );
        assertThat(oldVersionClient, notNullValue());
        assertThat(newVersionClient, notNullValue());
    }

    protected static void waitForSecurityMigrationCompletion(RestClient adminClient, int version) throws Exception {
        final Request request = new Request("GET", "_cluster/state/metadata/.security-7");
        assertBusy(() -> {
            Map<String, Object> indices = new XContentTestUtils.JsonMapView(entityAsMap(adminClient.performRequest(request))).get(
                "metadata.indices"
            );
            assertNotNull(indices);
            assertTrue(indices.containsKey(".security-7"));
            @SuppressWarnings("unchecked")
            String responseVersion = new XContentTestUtils.JsonMapView((Map<String, Object>) indices.get(".security-7")).get(
                "migration_version.version"
            );
            assertNotNull(responseVersion);
            assertTrue(Integer.parseInt(responseVersion) >= version);
        });
    }

}

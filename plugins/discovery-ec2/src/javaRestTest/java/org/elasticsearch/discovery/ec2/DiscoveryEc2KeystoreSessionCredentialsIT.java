/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.discovery.ec2;

import fixture.aws.DynamicRegionSupplier;
import fixture.aws.ec2.AwsEc2HttpFixture;

import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.util.List;
import java.util.function.Supplier;

import static fixture.aws.AwsCredentialsUtils.fixedAccessKeyAndToken;

public class DiscoveryEc2KeystoreSessionCredentialsIT extends DiscoveryEc2ClusterFormationTestCase {

    private static final String PREFIX = getIdentifierPrefix("DiscoveryEc2KeystoreSessionCredentialsIT");
    private static final String ACCESS_KEY = PREFIX + "-access-key";
    private static final String SESSION_TOKEN = PREFIX + "-session-token";

    private static final Supplier<String> regionSupplier = new DynamicRegionSupplier();
    private static final AwsEc2HttpFixture ec2ApiFixture = new AwsEc2HttpFixture(
        fixedAccessKeyAndToken(ACCESS_KEY, SESSION_TOKEN, regionSupplier, "ec2"),
        DiscoveryEc2KeystoreSessionCredentialsIT::getAvailableTransportEndpoints
    );

    private static final ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .nodes(2)
        .plugin("discovery-ec2")
        .setting(DiscoveryModule.DISCOVERY_SEED_PROVIDERS_SETTING.getKey(), Ec2DiscoveryPlugin.EC2_SEED_HOSTS_PROVIDER_NAME)
        .setting("logger." + AwsEc2SeedHostsProvider.class.getCanonicalName(), "DEBUG")
        .setting(Ec2ClientSettings.ENDPOINT_SETTING.getKey(), ec2ApiFixture::getAddress)
        .environment("AWS_REGION", regionSupplier)
        .keystore("discovery.ec2.access_key", ACCESS_KEY)
        .keystore("discovery.ec2.secret_key", ESTestCase::randomSecretKey)
        .keystore("discovery.ec2.session_token", SESSION_TOKEN)
        .build();

    private static List<String> getAvailableTransportEndpoints() {
        return cluster.getAvailableTransportEndpoints();
    }

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(ec2ApiFixture).around(cluster);

    @Override
    protected ElasticsearchCluster getCluster() {
        return cluster;
    }
}

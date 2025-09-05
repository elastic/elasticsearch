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

import static fixture.aws.AwsCredentialsUtils.fixedAccessKey;

public class DiscoveryEc2EnvironmentVariableCredentialsIT extends DiscoveryEc2ClusterFormationTestCase {

    private static final String PREFIX = getIdentifierPrefix("DiscoveryEc2EnvironmentVariableCredentialsIT");
    private static final String ACCESS_KEY = PREFIX + "-access-key";

    private static final Supplier<String> regionSupplier = new DynamicRegionSupplier();
    private static final AwsEc2HttpFixture ec2ApiFixture = new AwsEc2HttpFixture(
        fixedAccessKey(ACCESS_KEY, regionSupplier, "ec2"),
        DiscoveryEc2EnvironmentVariableCredentialsIT::getAvailableTransportEndpoints
    );

    private static final ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .nodes(2)
        .plugin("discovery-ec2")
        .setting(DiscoveryModule.DISCOVERY_SEED_PROVIDERS_SETTING.getKey(), Ec2DiscoveryPlugin.EC2_SEED_HOSTS_PROVIDER_NAME)
        .setting("logger." + AwsEc2SeedHostsProvider.class.getCanonicalName(), "DEBUG")
        .setting(Ec2ClientSettings.ENDPOINT_SETTING.getKey(), ec2ApiFixture::getAddress)
        .environment("AWS_REGION", regionSupplier)
        .environment("AWS_ACCESS_KEY_ID", ACCESS_KEY)
        .environment("AWS_SECRET_ACCESS_KEY", ESTestCase::randomSecretKey)
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

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.discovery.ec2;

import fixture.aws.imds.Ec2ImdsHttpFixture;
import fixture.aws.imds.Ec2ImdsServiceBuilder;
import fixture.aws.imds.Ec2ImdsVersion;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

public class DiscoveryEc2AvailabilityZoneAttributeImdsV2IT extends DiscoveryEc2AvailabilityZoneAttributeTestCase {
    private static final Ec2ImdsHttpFixture ec2ImdsHttpFixture = new Ec2ImdsHttpFixture(
        new Ec2ImdsServiceBuilder(Ec2ImdsVersion.V2).availabilityZoneSupplier(
            DiscoveryEc2AvailabilityZoneAttributeTestCase::getAvailabilityZone
        )
    );

    public static ElasticsearchCluster cluster = buildCluster(ec2ImdsHttpFixture::getAddress);

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(ec2ImdsHttpFixture).around(cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }
}

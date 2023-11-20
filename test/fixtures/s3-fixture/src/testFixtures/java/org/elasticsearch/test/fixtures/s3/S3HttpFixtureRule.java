/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.fixtures.s3;

import fixture.s3.S3HttpFixture;

import fixture.s3.S3HttpFixtureWithEC2;

import fixture.s3.S3HttpFixtureWithSessionToken;

import org.jetbrains.annotations.NotNull;
import org.junit.rules.ExternalResource;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class S3HttpFixtureRule extends ExternalResource {
    private final boolean enabled;
    private final List<S3FixtureType> enabledFixtures;

    private Map<S3FixtureType, S3HttpFixture> configuredFixtures = new HashMap<>();

    private void configureFixture(S3FixtureType fixtureType) throws Exception {

        InetSocketAddress inetSocketAddress = resolveLoopBackAddress();
        int port = inetSocketAddress.getPort();

        if (fixtureType == S3FixtureType.S3Fixture) {
            S3HttpFixture fixture = new S3HttpFixture(
                inetSocketAddress,
                new String[] { "localhost", String.valueOf(port), "bucket", "base_path_integration_tests", "s3_test_access_key" }
            );
            configuredFixtures.put(S3FixtureType.S3Fixture, fixture);
        } else if (fixtureType == S3FixtureType.S3FixtureEc2) {
            S3HttpFixture fixture = new S3HttpFixtureWithEC2(
                inetSocketAddress,
                new String[] { "localhost", String.valueOf(port), "ec2_bucket", "ec2_base_path", "ec2_access_key", "ec2_session_token" }
            );
            configuredFixtures.put(S3FixtureType.S3FixtureEc2, fixture);
        } else if (fixtureType == S3FixtureType.S3FixtureEcs) {
            S3HttpFixture fixture = new S3HttpFixtureWithEC2(
                inetSocketAddress,
                new String[] { "localhost", String.valueOf(port), "ecs_bucket", "ecs_base_path", "ecs_access_key", "ecs_session_token" }
            );
            configuredFixtures.put(S3FixtureType.S3FixtureEcs, fixture);
        } else if (fixtureType == S3FixtureType.S3FixtureWithToken) {
            S3HttpFixture fixture = new S3HttpFixtureWithSessionToken(
                inetSocketAddress,
                new String[] {
                    "localhost",
                    String.valueOf(port),
                    "session_token_bucket",
                    "session_token_base_path_integration_tests",
                    "session_token_access_key",
                    "session_token" }
            );
            configuredFixtures.put(S3FixtureType.S3FixtureWithToken, fixture);
        }
    }

    public enum S3FixtureType {
        S3Fixture,
        S3FixtureEcs,
        S3FixtureEc2,
        S3FixtureWithToken
    }

    public S3HttpFixtureRule(boolean enabled, List<S3FixtureType> enabledFixtures) {
        this.enabled = enabled;
        this.enabledFixtures = enabledFixtures;

    }

    public String getAddress(S3FixtureType fixtureRuleType) {
        return configuredFixtures.get(fixtureRuleType).getAddress();
    }

    protected void before() throws Throwable {
        if (enabled) {
            startFixtures();
        }
    }

    private void startFixtures() throws Exception {
        for (S3FixtureType enabledFixture : enabledFixtures) {
            configureFixture(enabledFixture);
        }
        configuredFixtures.values().forEach(s3HttpFixture -> {
            try {
                s3HttpFixture.start();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @NotNull
    private static InetSocketAddress resolveLoopBackAddress() {
        return new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
    }

    @Override
    protected void after() {
        if (enabled) {
            configuredFixtures.values().forEach(f -> f.stop(0));
        }
    }

}

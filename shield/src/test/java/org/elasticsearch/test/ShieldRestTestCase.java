/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.test;

import com.carrotsearch.randomizedtesting.annotations.Name;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.elasticsearch.client.support.Headers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.shield.authc.support.UsernamePasswordToken;
import org.elasticsearch.test.rest.ElasticsearchRestTestCase;
import org.elasticsearch.test.rest.RestTestCandidate;
import org.elasticsearch.test.rest.client.RestException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static org.elasticsearch.shield.authc.support.UsernamePasswordToken.basicAuthHeaderValue;

/**
 * Allows to run Elasticsearch REST tests against a cluster with shield installed.
 * Subclasses {@link org.elasticsearch.test.ShieldIntegrationTest} that contains all the needed code to override the global
 * cluster settings and make sure shield is properly installed and configured.
 * Delegates all of the tests to {@link org.elasticsearch.test.rest.ElasticsearchRestTestCase}.
 */
@ElasticsearchRestTestCase.Rest
@ElasticsearchIntegrationTest.ClusterScope(randomDynamicTemplates = false)
@LuceneTestCase.SuppressFsync // we aren't trying to test this here, and it can make the test slow
@LuceneTestCase.SuppressCodecs("*") // requires custom completion postings format
@Slow
public abstract class ShieldRestTestCase extends ShieldIntegrationTest {

    private final DelegatedRestTestCase delegate;

    public ShieldRestTestCase(@Name("yaml") RestTestCandidate testCandidate) {
        delegate = new DelegatedRestTestCase(testCandidate);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(delegate.nodeSettings(nodeOrdinal))
                .put(super.nodeSettings(nodeOrdinal))
                .put(Node.HTTP_ENABLED, true)
                .build();
    }

    @BeforeClass
    public static void initExecutionContext() throws IOException, RestException {
        ElasticsearchRestTestCase.initExecutionContext();
    }

    @AfterClass
    public static void close() {
        ElasticsearchRestTestCase.close();
    }

    @Test
    public void test() throws IOException {
        delegate.test();
    }

    @Before
    public void reset() throws IOException, RestException {
        delegate.reset();
    }

    class DelegatedRestTestCase extends ElasticsearchRestTestCase {

        DelegatedRestTestCase(RestTestCandidate candidate) {
            super(candidate);
        }

        @Override
        protected Settings restClientSettings() {
            return Settings.builder()
                    .put(Headers.PREFIX + "." + UsernamePasswordToken.BASIC_AUTH_HEADER, basicAuthHeaderValue(ShieldSettingsSource.DEFAULT_USER_NAME,
                            new SecuredString(ShieldSettingsSource.DEFAULT_PASSWORD.toCharArray()))).build();
        }

        @Override
        public Settings nodeSettings(int ordinal) {
            return super.nodeSettings(ordinal);
        }

        @Override
        protected Settings transportClientSettings() {
            return ShieldRestTestCase.this.transportClientSettings();
        }
    }
}

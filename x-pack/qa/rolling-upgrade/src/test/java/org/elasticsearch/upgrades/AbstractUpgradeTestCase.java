/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.upgrades;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.test.SecuritySettingsSourceField;
import org.junit.Before;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.test.SecuritySettingsSourceField.basicAuthHeaderValue;

public abstract class AbstractUpgradeTestCase extends ESRestTestCase {

    private static final String BASIC_AUTH_VALUE =
            basicAuthHeaderValue("test_user", SecuritySettingsSourceField.TEST_PASSWORD);

    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveReposUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveTemplatesUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveRollupJobsUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveILMPoliciesUponCompletion() {
        return true;
    }

    enum ClusterType {
        OLD,
        MIXED,
        UPGRADED;

        public static ClusterType parse(String value) {
            switch (value) {
                case "old_cluster":
                    return OLD;
                case "mixed_cluster":
                    return MIXED;
                case "upgraded_cluster":
                    return UPGRADED;
                default:
                    throw new AssertionError("unknown cluster type: " + value);
            }
        }
    }

    protected static final ClusterType CLUSTER_TYPE = ClusterType.parse(System.getProperty("tests.rest.suite"));

    @Override
    protected Settings restClientSettings() {
        return Settings.builder()
                .put(ThreadContext.PREFIX + ".Authorization", BASIC_AUTH_VALUE)
                .build();
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

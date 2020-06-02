/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.multi_cluster;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.lucene.util.TimeUnits;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;

import static org.elasticsearch.xpack.test.SecuritySettingsSourceField.basicAuthHeaderValue;

@TimeoutSuite(millis = 5 * TimeUnits.MINUTE) // to account for slow as hell VMs
public class MultiClusterYamlTestSuiteIT extends ESClientYamlSuiteTestCase {

    private static final String USER = "test_user";
    private static final String PASS = "x-pack-test-password";

    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    public MultiClusterYamlTestSuiteIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return createParameters();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(USER, new SecureString(PASS.toCharArray()));
        return Settings.builder()
                .put(super.restClientSettings())
                .put(ThreadContext.PREFIX + ".Authorization", token)
                .build();
    }
}

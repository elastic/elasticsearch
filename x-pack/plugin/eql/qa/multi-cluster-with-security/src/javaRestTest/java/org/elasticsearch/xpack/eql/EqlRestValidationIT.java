/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.eql.EqlRestValidationTestCase;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;

import static org.elasticsearch.xpack.eql.RemoteClusterTestUtils.LOCAL_CLUSTER;
import static org.elasticsearch.xpack.eql.RemoteClusterTestUtils.REMOTE_CLUSTER;
import static org.elasticsearch.xpack.eql.RemoteClusterTestUtils.remoteClusterPattern;

@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class EqlRestValidationIT extends EqlRestValidationTestCase {
    @ClassRule
    public static TestRule clusterRule = RuleChain.outerRule(REMOTE_CLUSTER).around(LOCAL_CLUSTER);

    @Override
    protected String getTestRestCluster() {
        return LOCAL_CLUSTER.getHttpAddresses();
    }

    @Override
    protected String getRemoteCluster() {
        return REMOTE_CLUSTER.getHttpAddresses();
    }

    @Override
    protected String getInexistentIndexErrorMessage() {
        return "\"root_cause\":[{\"type\":\"index_not_found_exception\",\"reason\":\"no such index ";
    }

    protected void assertErrorMessageWhenAllowNoIndicesIsFalse(String reqParameter) throws IOException {
        assertErrorMessage("inexistent1*", reqParameter, getInexistentIndexErrorMessage() + "[" + indexPattern("inexistent1*") + "]\"");
        assertErrorMessage(
            "inexistent1*,inexistent2*",
            reqParameter,
            getInexistentIndexErrorMessage() + "[" + indexPattern("inexistent1*") + "]\""
        );
        assertErrorMessage(
            "test_eql,inexistent*",
            reqParameter,
            getInexistentIndexErrorMessage() + "[" + indexPattern("inexistent*") + "]\""
        );
        // TODO: revisit the next two tests when https://github.com/elastic/elasticsearch/issues/64190 is closed
        assertErrorMessage("inexistent", reqParameter, getInexistentIndexErrorMessage() + "[" + indexPattern("inexistent") + "]\"");
        assertErrorMessage(
            "inexistent1,inexistent2",
            reqParameter,
            getInexistentIndexErrorMessage()
                + "[null]\",\"resource.type\":\"index_expression\",\"resource.id\":[\"inexistent1\",\"inexistent2\"]}]"
        );
    }

    @Override
    protected String indexPattern(String pattern) {
        return remoteClusterPattern(pattern);
    }
}

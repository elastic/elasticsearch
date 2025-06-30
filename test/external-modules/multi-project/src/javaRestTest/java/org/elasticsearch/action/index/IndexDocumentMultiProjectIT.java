/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.index;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.core.Strings;
import org.elasticsearch.multiproject.MultiProjectRestTestCase;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.LocalClusterSpecBuilder;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TestName;

import java.util.List;
import java.util.Locale;

import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;

public class IndexDocumentMultiProjectIT extends MultiProjectRestTestCase {

    protected static final int NODE_NUM = 3;

    @ClassRule
    public static ElasticsearchCluster cluster = createCluster();

    @Rule
    public final TestName testNameRule = new TestName();

    @FixForMultiProject
    private static ElasticsearchCluster createCluster() {
        LocalClusterSpecBuilder<ElasticsearchCluster> clusterBuilder = ElasticsearchCluster.local()
            .nodes(NODE_NUM)
            .distribution(DistributionType.INTEG_TEST)
            .module("test-multi-project")
            .setting("test.multi_project.enabled", "true")
            .setting("xpack.security.enabled", "false") // TODO multi-project: make this test suite work with Security enabled
            .setting("xpack.ml.enabled", "false"); // TODO multi-project: make this test suite work with ML enabled
        return clusterBuilder.build();
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testIndexDocumentMultiProject() throws Exception {
        List<String> projects = List.of(
            "projectid1" + testNameRule.getMethodName().toLowerCase(Locale.ROOT),
            "projectid2" + testNameRule.getMethodName().toLowerCase(Locale.ROOT)
        );

        for (String p : projects) {
            createProject(p);
        }

        String indexName = testNameRule.getMethodName().toLowerCase(Locale.ROOT);

        for (String p : projects) {
            Request putIndexRequest = new Request("PUT", "/" + indexName + "?wait_for_active_shards=all&master_timeout=999s&timeout=999s");
            putIndexRequest.setJsonEntity(Strings.format("""
                {
                    "settings": {
                      "number_of_shards": %d,
                      "number_of_replicas": %d
                    }
                }
                """, randomIntBetween(1, 3), randomIntBetween(0, NODE_NUM - 1)));
            setRequestProjectId(putIndexRequest, p);
            Response putIndexResponse = client().performRequest(putIndexRequest);
            assertOK(putIndexResponse);
            var putIndexResponseBodyMap = entityAsMap(putIndexResponse);
            assertTrue((boolean) XContentMapValues.extractValue("acknowledged", putIndexResponseBodyMap));
            assertTrue((boolean) XContentMapValues.extractValue("shards_acknowledged", putIndexResponseBodyMap));
            assertThat((String) XContentMapValues.extractValue("index", putIndexResponseBodyMap), is(indexName));
        }

        for (String p : projects) {
            Request indexDocumentRequest = new Request("PUT", "/" + indexName + "/_doc/0");
            setRequestProjectId(indexDocumentRequest, p);
            indexDocumentRequest.setJsonEntity(Strings.format("""
                {
                    "index-field": "%s-doc"
                }
                """, p));
            Response indexDocumentResponse = client().performRequest(indexDocumentRequest);
            assertOK(indexDocumentResponse);
        }

        for (String p : projects) {
            Request indexDocumentRequest = new Request("GET", "/" + indexName + "/_source/0");
            setRequestProjectId(indexDocumentRequest, p);
            Response indexDocumentResponse = client().performRequest(indexDocumentRequest);
            assertOK(indexDocumentResponse);
            var response = responseAsMap(indexDocumentResponse);
            assertThat(response, hasEntry("index-field", p + "-doc"));
        }
    }

}

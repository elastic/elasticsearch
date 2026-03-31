/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import static org.hamcrest.Matchers.containsString;

/**
 * Tests that it is possible to <em>define</em> a pipeline with the
 * {@link org.elasticsearch.xpack.security.ingest.SetSecurityUserProcessor} on a cluster with security disabled, but it is not possible
 * to use that pipeline for ingestion.
 */
public class SetSecurityUserProcessorWithSecurityDisabledIT extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .nodes(2)
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.ml.enabled", "false")
        // We run with a trial license, but explicitly disable security.
        // This means the security plugin is loaded and all feature are permitted, but they are not enabled
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.security.enabled", "false")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testDefineAndUseProcessor() throws Exception {
        final String pipeline = "pipeline-" + getTestName();
        final String index = "index-" + getTestName();
        {
            final Request putPipeline = new Request("PUT", "/_ingest/pipeline/" + pipeline);
            putPipeline.setJsonEntity(Strings.format("""
                {
                  "description": "Test pipeline (%s)",
                  "processors": [ { "set_security_user": { "field": "user" } } ]
                }""", getTestName()));
            final Response response = client().performRequest(putPipeline);
            assertOK(response);
        }

        {
            final Request ingest = new Request("PUT", "/" + index + "/_doc/1?pipeline=" + pipeline);
            ingest.setJsonEntity("{\"field\":\"value\"}");
            final ResponseException ex = expectThrows(ResponseException.class, () -> client().performRequest(ingest));
            final Response response = ex.getResponse();
            assertThat(
                EntityUtils.toString(response.getEntity()),
                containsString("Security (authentication) is not enabled on this cluster")
            );
        }
    }

}

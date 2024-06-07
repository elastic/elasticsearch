/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch;

import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.junit.Test;

import java.io.IOException;

public class ILMTransitionOnSizeTests {

    @Test
    public void testILMTransitionOnSize() throws IOException, InterruptedException {
        RestClient client = RestClient.builder(new HttpHost("localhost", 9200, "http")).build();

        String policy = "{\n" +
            "  \"policy\": {\n" +
            "    \"phases\": {\n" +
            "      \"hot\": {\n" +
            "        \"actions\": {\n" +
            "          \"rollover\": {\n" +
            "            \"max_size\": \"50mb\"\n" +
            "          }\n" +
            "        }\n" +
            "      },\n" +
            "      \"warm\": {\n" +
            "        \"actions\": {\n" +
            "          \"forcemerge\": {\n" +
            "            \"max_num_segments\": 1\n" +
            "          }\n" +
            "        }\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}";

        Request putPolicyRequest = new Request("PUT", "/_ilm/policy/test_policy");
        putPolicyRequest.setJsonEntity(policy);
        client.performRequest(putPolicyRequest);

        String template = "{\n" +
            "  \"index_patterns\": [\"test-index-*\"],\n" +
            "  \"settings\": {\n" +
            "    \"index.lifecycle.name\": \"test_policy\",\n" +
            "    \"index.lifecycle.rollover_alias\": \"test-alias\"\n" +
            "  }\n" +
            "}";

        Request putTemplateRequest = new Request("PUT", "/_index_template/test_template");
        putTemplateRequest.setJsonEntity(template);
        client.performRequest(putTemplateRequest);

        String index = "{\n" +
            "  \"aliases\": {\n" +
            "    \"test-alias\": {\n" +
            "      \"is_write_index\": true\n" +
            "    }\n" +
            "  }\n" +
            "}";

        Request createIndexRequest = new Request("PUT", "/test-index-000001");
        createIndexRequest.setJsonEntity(index);
        client.performRequest(createIndexRequest);

        String document = "{\"field\": \"" + "value".repeat(1000) + "\"}";
        for (int i = 0; i < 100000; i++) {
            Request indexRequest = new Request("POST", "/test-alias/_doc");
            indexRequest.setJsonEntity(document);
            client.performRequest(indexRequest);
        }

        Thread.sleep(60000);

        Request explainRequest = new Request("GET", "/test-index-000001/_ilm/explain");
        Response explainResponse = client.performRequest(explainRequest);
        String responseBody = EntityUtils.toString(explainResponse.getEntity());

        assert responseBody.contains("\"phase\":\"warm\"");

        Request deleteIndexRequest = new Request("DELETE", "/test-index-*");
        client.performRequest(deleteIndexRequest);
        Request deletePolicyRequest = new Request("DELETE", "/_ilm/policy/test_policy");
        client.performRequest(deletePolicyRequest);
        Request deleteTemplateRequest = new Request("DELETE", "/_index_template/test_template");
        client.performRequest(deleteTemplateRequest);

        client.close();
    }
}

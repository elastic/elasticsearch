/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich;

import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class EnrichIT extends ESRestTestCase {

    // TODO: update this test when policy runner is ready
    public void testBasicFlow() throws Exception {
        // Create the policy:
        Request putPolicyRequest = new Request("PUT", "/_enrich/policy/my_policy");
        putPolicyRequest.setJsonEntity("{\"type\": \"exact_match\",\"index_pattern\": \"my-index*\", \"enrich_key\": \"host\", " +
            "\"enrich_values\": [\"globalRank\", \"tldRank\", \"tld\"], \"schedule\": \"0 5 * * *\"}");
        assertOK(client().performRequest(putPolicyRequest));

        // Add a single enrich document for now and then refresh:
        Request indexRequest = new Request("PUT", "/.enrich-my_policy/_doc/elastic.co");
        XContentBuilder document = XContentBuilder.builder(XContentType.SMILE.xContent());
        document.startObject();
        document.field("host", "elastic.co");
        document.field("globalRank", 25);
        document.field("tldRank", 7);
        document.field("tld", "co");
        document.endObject();
        document.close();
        ByteArrayOutputStream out  = (ByteArrayOutputStream) document.getOutputStream();
        indexRequest.setEntity(new ByteArrayEntity(out.toByteArray(), ContentType.create("application/smile")));
        assertOK(client().performRequest(indexRequest));
        Request refreshRequest = new Request("POST", "/.enrich-my_policy/_refresh");
        assertOK(client().performRequest(refreshRequest));

        // Create pipeline
        Request putPipelineRequest = new Request("PUT", "/_ingest/pipeline/my_pipeline");
        putPipelineRequest.setJsonEntity("{\"processors\":[" +
            "{\"enrich\":{\"policy_name\":\"my_policy\",\"enrich_key\":\"host\",\"enrich_values\":[" +
                "{\"source\":\"globalRank\",\"target\":\"global_rank\"}," +
                "{\"source\":\"tldRank\",\"target\":\"tld_rank\"}" +
                "]}}" +
            "]}");
        assertOK(client().performRequest(putPipelineRequest));

        // Index document using pipeline with enrich processor:
        indexRequest = new Request("PUT", "/my-index/_doc/1");
        indexRequest.addParameter("pipeline", "my_pipeline");
        indexRequest.setJsonEntity("{\"host\": \"elastic.co\"}");
        assertOK(client().performRequest(indexRequest));

        // Check if document has been enriched
        Request getRequest = new Request("GET", "/my-index/_doc/1");
        Map<String, Object> response = toMap(client().performRequest(getRequest));
        Map<?, ?> _source = (Map<?, ?>) response.get("_source");
        assertThat(_source.size(), equalTo(3));
        assertThat(_source.get("host"), equalTo("elastic.co"));
        assertThat(_source.get("global_rank"), equalTo(25));
        assertThat(_source.get("tld_rank"), equalTo(7));
    }

    private static Map<String, Object> toMap(Response response) throws IOException {
        return toMap(EntityUtils.toString(response.getEntity()));
    }

    private static Map<String, Object> toMap(String response) {
        return XContentHelper.convertToMap(JsonXContent.jsonXContent, response, false);
    }

}

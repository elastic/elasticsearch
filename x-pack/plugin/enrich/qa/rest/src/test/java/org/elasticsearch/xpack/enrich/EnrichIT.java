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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.yaml.ObjectPath;
import org.junit.After;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class EnrichIT extends ESRestTestCase {

    @After
    private void deletePolicies() throws Exception {
        Map<String, Object> responseMap = toMap(client().performRequest(new Request("GET", "/_enrich/policy")));
        @SuppressWarnings("unchecked")
        List<Map<?,?>> policies = (List<Map<?,?>>) responseMap.get("policies");

        for (Map<?, ?> entry: policies) {
            client().performRequest(new Request("DELETE", "/_enrich/policy/" + entry.get("name")));
        }
    }

    public void testBasicFlow() throws Exception {
        // Create the policy:
        Request putPolicyRequest = new Request("PUT", "/_enrich/policy/my_policy");
        putPolicyRequest.setJsonEntity("{\"type\": \"exact_match\",\"indices\": [\"my-source-index\"], \"enrich_key\": \"host\", " +
            "\"enrich_values\": [\"globalRank\", \"tldRank\", \"tld\"]}");
        assertOK(client().performRequest(putPolicyRequest));

        // Add entry to source index and then refresh:
        Request indexRequest = new Request("PUT", "/my-source-index/_doc/elastic.co");
        indexRequest.setJsonEntity("{\"host\": \"elastic.co\",\"globalRank\": 25,\"tldRank\": 7,\"tld\": \"co\"}");
        assertOK(client().performRequest(indexRequest));
        Request refreshRequest = new Request("POST", "/my-source-index/_refresh");
        assertOK(client().performRequest(refreshRequest));

        // Execute the policy:
        Request executePolicyRequest = new Request("POST", "/_enrich/policy/my_policy/_execute");
        assertOK(client().performRequest(executePolicyRequest));

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

    public void testEnrichSourceMapping() throws Exception {
        String mapping = "\"dynamic\": false,\"_source\": {\"enabled\": false},\"_enrich_source\": {\"enabled\": true}";
        createIndex("enrich", Settings.EMPTY, mapping);

        Request indexRequest = new Request("PUT", "/enrich/_doc/elastic.co");
        XContentBuilder document = XContentBuilder.builder(XContentType.SMILE.xContent());
        document.startObject();
        document.field("globalRank", 25);
        document.field("tldRank", 7);
        document.field("tld", "co");
        document.endObject();
        document.close();
        ByteArrayOutputStream out  = (ByteArrayOutputStream) document.getOutputStream();
        indexRequest.setEntity(new ByteArrayEntity(out.toByteArray(), ContentType.create("application/smile")));
        assertOK(client().performRequest(indexRequest));

        Request refreshRequest = new Request("POST", "/enrich/_refresh");
        assertOK(client().performRequest(refreshRequest));

        Request searchRequest = new Request("GET", "/enrich/_search");
        searchRequest.setJsonEntity("{\"docvalue_fields\": [{\"field\": \"_enrich_source\"}]}");
        Map<String, ?> response = toMap(client().performRequest(searchRequest));
        logger.info("RSP={}", response);
        String enrichSource = ObjectPath.evaluate(response, "hits.hits.0.fields._enrich_source.0");
        assertThat(enrichSource, notNullValue());

        try (InputStream in = new ByteArrayInputStream(Base64.getDecoder().decode(enrichSource))) {
            Map<String, Object> map = XContentHelper.convertToMap(XContentType.SMILE.xContent(), in, false);
            assertThat(map.size(), equalTo(3));
            assertThat(map.get("globalRank"), equalTo(25));
            assertThat(map.get("tldRank"), equalTo(7));
            assertThat(map.get("tld"), equalTo("co"));
        }
    }

    private static Map<String, Object> toMap(Response response) throws IOException {
        return toMap(EntityUtils.toString(response.getEntity()));
    }

    private static Map<String, Object> toMap(String response) {
        return XContentHelper.convertToMap(JsonXContent.jsonXContent, response, false);
    }

}

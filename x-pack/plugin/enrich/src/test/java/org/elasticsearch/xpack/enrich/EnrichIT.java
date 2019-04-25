/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
*/
package org.elasticsearch.xpack.enrich;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.yaml.ObjectPath;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Base64;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class EnrichIT extends ESRestTestCase {

    // TODO: replace this with a more complete test that uses enrich apis to create a policy and with the enrich processor
    public void testEnrichSourceMapping() throws Exception {
//        String mapping = "\"dynamic\": false,\"_source\": {\"enabled\": false},\"_enrich_source\": {\"enabled\": false}";
        String mapping = "\"_source\": {\"enabled\": false},\"_enrich_source\": {\"enabled\": true}";
        createIndex("enrich", Settings.EMPTY, mapping);

        Request indexRequest = new Request("PUT", "/enrich/_doc/elastic.co");
        indexRequest.setJsonEntity("{\"globalRank\": 25, \"tldRank\": 7, \"tld\": \"co\"}");
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

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    private static Map<String, Object> toMap(Response response) throws IOException {
        return toMap(EntityUtils.toString(response.getEntity()));
    }

    private static Map<String, Object> toMap(String response) {
        return XContentHelper.convertToMap(JsonXContent.jsonXContent, response, false);
    }

}

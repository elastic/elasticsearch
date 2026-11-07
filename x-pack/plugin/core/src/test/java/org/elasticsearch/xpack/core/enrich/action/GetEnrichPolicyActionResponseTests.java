/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.enrich.action;

import org.elasticsearch.common.Strings;
import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class GetEnrichPolicyActionResponseTests extends ESTestCase {

    public void testChunkCountIsOnePerPolicyPlusWrapping() {
        int count = randomIntBetween(0, 5);
        Map<String, EnrichPolicy> policies = new HashMap<>();
        for (int i = 0; i < count; i++) {
            policies.put("policy-" + i, samplePolicy());
        }
        GetEnrichPolicyAction.Response response = new GetEnrichPolicyAction.Response(policies);
        // one chunk per policy, plus the surrounding startObject/startArray/endArray/endObject chunks
        AbstractChunkedSerializingTestCase.assertChunkCount(response, ignored -> count + 4);
    }

    public void testRendersWellFormedJson() throws IOException {
        GetEnrichPolicyAction.Response response = new GetEnrichPolicyAction.Response(Map.of("p1", samplePolicy()));

        XContentBuilder builder = JsonXContent.contentBuilder();
        Iterator<? extends ToXContent> iterator = response.toXContentChunked(ToXContent.EMPTY_PARAMS);
        while (iterator.hasNext()) {
            iterator.next().toXContent(builder, ToXContent.EMPTY_PARAMS);
        }
        String json = Strings.toString(builder);
        assertThat(json, containsString("\"policies\""));
        assertThat(json, containsString("\"config\""));
        assertThat(json, containsString("\"match\""));

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            Map<String, Object> map = parser.map();
            assertThat(map.keySet(), equalTo(Map.of("policies", List.of()).keySet()));
            assertThat((List<?>) map.get("policies"), hasSize(1));
        }
    }

    private static EnrichPolicy samplePolicy() {
        return new EnrichPolicy(EnrichPolicy.MATCH_TYPE, null, List.of("source-index"), "match_field", List.of("a", "b"));
    }
}

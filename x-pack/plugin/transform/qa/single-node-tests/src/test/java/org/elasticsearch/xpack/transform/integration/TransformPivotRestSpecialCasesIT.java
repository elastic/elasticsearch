/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.integration;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class TransformPivotRestSpecialCasesIT extends TransformRestTestCase {
    private static boolean indicesCreated = false;

    // preserve indices in order to reuse source indices in several test cases
    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    @Before
    public void createIndexes() throws IOException {

        // it's not possible to run it as @BeforeClass as clients aren't initialized then, so we need this little hack
        if (indicesCreated) {
            return;
        }

        createReviewsIndex();
        indicesCreated = true;
    }

    public void testIndexTemplateMappingClash() throws Exception {
        String transformId = "special_pivot_template_mappings_clash";
        String transformIndex = "special_pivot_template_mappings_clash";

        // create a template that defines a field "rating" with a type "float" which will clash later with
        // output field "rating.avg" in the pivot config
        final Request createIndexTemplateRequest = new Request("PUT", "_template/special_pivot_template");

        String template = "{"
            + "\"index_patterns\" : [\"special_pivot_template*\"],"
            + "  \"mappings\" : {"
            + "    \"properties\": {"
            + "      \"rating\":{"
            + "        \"type\": \"float\"\n"
            + "      }"
            + "    }"
            + "  }"
            + "}";

        createIndexTemplateRequest.setJsonEntity(template);
        Map<String, Object> createIndexTemplateResponse = entityAsMap(client().performRequest(createIndexTemplateRequest));
        assertThat(createIndexTemplateResponse.get("acknowledged"), equalTo(Boolean.TRUE));

        final Request createTransformRequest = new Request("PUT", getTransformEndpoint() + transformId);

        String config = "{"
            + " \"source\": {\"index\":\""
            + REVIEWS_INDEX_NAME
            + "\"},"
            + " \"dest\": {\"index\":\""
            + transformIndex
            + "\"},";

        config += " \"pivot\": {"
            + "   \"group_by\": {"
            + "     \"reviewer\": {"
            + "       \"terms\": {"
            + "         \"field\": \"user_id\""
            + " } } },"
            + "   \"aggregations\": {"
            + "     \"rating.avg\": {"
            + "       \"avg\": {"
            + "         \"field\": \"stars\""
            + " } }"
            + " } }"
            + "}";

        createTransformRequest.setJsonEntity(config);
        Map<String, Object> createTransformResponse = entityAsMap(client().performRequest(createTransformRequest));
        assertThat(createTransformResponse.get("acknowledged"), equalTo(Boolean.TRUE));

        startAndWaitForTransform(transformId, transformIndex);
        assertTrue(indexExists(transformIndex));

        // we expect 27 documents as there shall be 27 user_id's
        Map<String, Object> indexStats = getAsMap(transformIndex + "/_stats");
        assertEquals(27, XContentMapValues.extractValue("_all.total.docs.count", indexStats));

        // get and check some users
        Map<String, Object> searchResult = getAsMap(transformIndex + "/_search?q=reviewer:user_4");

        assertEquals(1, XContentMapValues.extractValue("hits.total.value", searchResult));
        Number actual = (Number) ((List<?>) XContentMapValues.extractValue("hits.hits._source.rating.avg", searchResult)).get(0);
        assertEquals(3.878048780, actual.doubleValue(), 0.000001);
    }
}

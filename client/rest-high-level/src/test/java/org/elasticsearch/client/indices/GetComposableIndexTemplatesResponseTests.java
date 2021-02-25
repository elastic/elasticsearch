/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.indices;

import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.client.indices.GetComponentTemplatesResponseTests.randomMeta;
import static org.elasticsearch.client.indices.GetComponentTemplatesResponseTests.randomTemplate;
import static org.elasticsearch.test.AbstractXContentTestCase.xContentTester;

public class GetComposableIndexTemplatesResponseTests extends ESTestCase {

    public void testFromXContent() throws Exception {
        xContentTester(
            this::createParser,
            GetComposableIndexTemplatesResponseTests::createTestInstance,
            GetComposableIndexTemplatesResponseTests::toXContent,
            GetComposableIndexTemplatesResponse::fromXContent)
            .supportsUnknownFields(true)
            .randomFieldsExcludeFilter(a -> true)
            .test();
    }

    private static GetComposableIndexTemplatesResponse createTestInstance() {
        Map<String, ComposableIndexTemplate> templates = new HashMap<>();
        if (randomBoolean()) {
            int count = randomInt(10);
            for (int i = 0; i < count; i++) {
                templates.put(randomAlphaOfLength(10), randomIndexTemplate());
            }
        }
        return new GetComposableIndexTemplatesResponse(templates);
    }

    private static void toXContent(GetComposableIndexTemplatesResponse response, XContentBuilder builder) throws IOException {
        builder.startObject();
        builder.startArray("index_templates");
        for (Map.Entry<String, ComposableIndexTemplate> e : response.getIndexTemplates().entrySet()) {
            builder.startObject();
            builder.field("name", e.getKey());
            builder.field("index_template");
            e.getValue().toXContent(builder, null);
            builder.endObject();
        }
        builder.endArray();
        builder.endObject();
    }

    private static ComposableIndexTemplate randomIndexTemplate() {
        List<String> patterns = Arrays.asList(generateRandomStringArray(10, 10, false, false));
        List<String> composedOf = null;
        Map<String, Object> meta = null;
        ComposableIndexTemplate.DataStreamTemplate dataStreamTemplate = null;
        if (randomBoolean()) {
            composedOf = Arrays.asList(generateRandomStringArray(10, 10, false, false));
        }
        if (randomBoolean()) {
            meta = randomMeta();
        }

        Long priority = randomBoolean() ? null : randomNonNegativeLong();
        Long version = randomBoolean() ? null : randomNonNegativeLong();
        if (randomBoolean()) {
            dataStreamTemplate = new ComposableIndexTemplate.DataStreamTemplate();
        }
        return new ComposableIndexTemplate(patterns, randomTemplate(), composedOf, priority, version, meta, dataStreamTemplate, null);
    }
}

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.indices;

import org.elasticsearch.cluster.metadata.IndexTemplateV2;
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

public class GetIndexTemplatesV2ResponseTests extends ESTestCase {

    public void testFromXContent() throws Exception {
        xContentTester(
            this::createParser,
            GetIndexTemplatesV2ResponseTests::createTestInstance,
            GetIndexTemplatesV2ResponseTests::toXContent,
            GetIndexTemplatesV2Response::fromXContent)
            .supportsUnknownFields(true)
            .randomFieldsExcludeFilter(a -> true)
            .test();
    }

    private static GetIndexTemplatesV2Response createTestInstance() {
        Map<String, IndexTemplateV2> templates = new HashMap<>();
        if (randomBoolean()) {
            int count = randomInt(10);
            for (int i = 0; i < count; i++) {
                templates.put(randomAlphaOfLength(10), randomIndexTemplate());
            }
        }
        return new GetIndexTemplatesV2Response(templates);
    }

    private static void toXContent(GetIndexTemplatesV2Response response, XContentBuilder builder) throws IOException {
        builder.startObject();
        builder.startArray("index_templates");
        for (Map.Entry<String, IndexTemplateV2> e : response.getIndexTemplates().entrySet()) {
            builder.startObject();
            builder.field("name", e.getKey());
            builder.field("index_template");
            e.getValue().toXContent(builder, null);
            builder.endObject();
        }
        builder.endArray();
        builder.endObject();
    }

    private static IndexTemplateV2 randomIndexTemplate() {
        List<String> patterns = Arrays.asList(generateRandomStringArray(10, 10, false, false));
        List<String> composedOf = null;
        Map<String, Object> meta = null;
        IndexTemplateV2.DataStreamTemplate dataStreamTemplate = null;
        if (randomBoolean()) {
            composedOf = Arrays.asList(generateRandomStringArray(10, 10, false, false));
        }
        if (randomBoolean()) {
            meta = randomMeta();
        }

        Long priority = randomBoolean() ? null : randomNonNegativeLong();
        Long version = randomBoolean() ? null : randomNonNegativeLong();
        if (randomBoolean()) {
            dataStreamTemplate = new IndexTemplateV2.DataStreamTemplate(randomAlphaOfLength(8));
        }
        return new IndexTemplateV2(patterns, randomTemplate(), composedOf, priority, version, meta, dataStreamTemplate);
    }
}

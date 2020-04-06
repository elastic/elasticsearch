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

import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateV2;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
                templates.put(randomAlphaOfLength(10), randomTemplate());
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

    private static IndexTemplateV2 randomTemplate() {
        Settings settings = null;
        CompressedXContent mappings = null;
        Map<String, AliasMetadata> aliases = null;
        List<String> patterns = Arrays.asList(generateRandomStringArray(10, 10, false, false));
        List<String> composedOf = null;
        if (randomBoolean()) {
            settings = randomSettings();
        }
        if (randomBoolean()) {
            mappings = randomMappings();
        }
        if (randomBoolean()) {
            aliases = randomAliases();
        }
        if (randomBoolean()) {
            composedOf = Arrays.asList(generateRandomStringArray(10, 10, false, false));
        }
        Template template = new Template(settings, mappings, aliases);

        Map<String, Object> meta = null;
        if (randomBoolean()) {
            meta = randomMeta();
        }

        Long priority = randomBoolean() ? null : randomNonNegativeLong();
        Long version = randomBoolean() ? null : randomNonNegativeLong();
        return new IndexTemplateV2(patterns, template, composedOf, priority, version, meta);
    }

    private static Map<String, AliasMetadata> randomAliases() {
        String aliasName = randomAlphaOfLength(5);
        AliasMetadata aliasMeta = AliasMetadata.builder(aliasName)
            .filter(Collections.singletonMap(randomAlphaOfLength(2), randomAlphaOfLength(2)))
            .routing(randomBoolean() ? null : randomAlphaOfLength(3))
            .isHidden(randomBoolean() ? null : randomBoolean())
            .writeIndex(randomBoolean() ? null : randomBoolean())
            .build();
        return Collections.singletonMap(aliasName, aliasMeta);
    }

    private static CompressedXContent randomMappings() {
        try {
            return new CompressedXContent("{\"" + randomAlphaOfLength(3) + "\":\"" + randomAlphaOfLength(7) + "\"}");
        } catch (IOException e) {
            fail("got an IO exception creating fake mappings: " + e);
            return null;
        }
    }

    private static Settings randomSettings() {
        return Settings.builder()
            .put(randomAlphaOfLength(4), randomAlphaOfLength(10))
            .build();
    }

    private static Map<String, Object> randomMeta() {
        if (randomBoolean()) {
            return Collections.singletonMap(randomAlphaOfLength(4), randomAlphaOfLength(4));
        } else {
            return Collections.singletonMap(randomAlphaOfLength(5),
                Collections.singletonMap(randomAlphaOfLength(4), randomAlphaOfLength(4)));
        }
    }
}

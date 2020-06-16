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
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.test.AbstractXContentTestCase.xContentTester;

public class GetComponentTemplatesResponseTests extends ESTestCase {

    public void testFromXContent() throws Exception {
        xContentTester(
            this::createParser,
            GetComponentTemplatesResponseTests::createTestInstance,
            GetComponentTemplatesResponseTests::toXContent,
            GetComponentTemplatesResponse::fromXContent)
            .supportsUnknownFields(true)
            .randomFieldsExcludeFilter(a -> true)
            .test();
    }

    public static Template randomTemplate() {
        Settings settings = null;
        CompressedXContent mappings = null;
        Map<String, AliasMetadata> aliases = null;
        if (randomBoolean()) {
            settings = randomSettings();
        }
        if (randomBoolean()) {
            mappings = randomMappings();
        }
        if (randomBoolean()) {
            aliases = randomAliases();
        }
        return new Template(settings, mappings, aliases);
    }

    public static Map<String, Object> randomMeta() {
        if (randomBoolean()) {
            return Collections.singletonMap(randomAlphaOfLength(4), randomAlphaOfLength(4));
        } else {
            return Collections.singletonMap(randomAlphaOfLength(5),
                Collections.singletonMap(randomAlphaOfLength(4), randomAlphaOfLength(4)));
        }
    }

    private static GetComponentTemplatesResponse createTestInstance() {
        Map<String, ComponentTemplate> templates = new HashMap<>();
        if (randomBoolean()) {
            int count = randomInt(10);
            for (int i = 0; i < count; i++) {
                templates.put(randomAlphaOfLength(10), randomComponentTemplate());
            }
        }
        return new GetComponentTemplatesResponse(templates);
    }

    private static void toXContent(GetComponentTemplatesResponse response, XContentBuilder builder) throws IOException {
        builder.startObject();
        builder.startArray("component_templates");
        for (Map.Entry<String, ComponentTemplate> e : response.getComponentTemplates().entrySet()) {
            builder.startObject();
            builder.field("name", e.getKey());
            builder.field("component_template");
            e.getValue().toXContent(builder, null);
            builder.endObject();
        }
        builder.endArray();
        builder.endObject();
    }

    private static ComponentTemplate randomComponentTemplate() {
        Template template = randomTemplate();

        Map<String, Object> meta = null;
        if (randomBoolean()) {
            meta = randomMeta();
        }
        return new ComponentTemplate(template, randomBoolean() ? null : randomNonNegativeLong(), meta);
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
}

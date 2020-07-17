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

package org.elasticsearch.client.transform;

import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.AbstractXContentTestCase.xContentTester;
import static org.hamcrest.Matchers.equalTo;

public class PreviewTransformResponseTests extends ESTestCase {

    public void testFromXContent() throws IOException {
        xContentTester(this::createParser, this::createTestInstance, this::toXContent, PreviewTransformResponse::fromXContent)
            .supportsUnknownFields(true)
            .randomFieldsExcludeFilter(path -> path.isEmpty() == false)
            .test();
    }

    public void testCreateIndexRequest() throws IOException {
        PreviewTransformResponse previewResponse = randomPreviewResponse();

        CreateIndexRequest createIndexRequest = previewResponse.getCreateIndexRequest("dest_index");
        assertEquals("dest_index", createIndexRequest.index());
        assertThat(createIndexRequest.aliases(), equalTo(previewResponse.getAliases()));
        assertThat(createIndexRequest.settings(), equalTo(previewResponse.getSettings()));

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        builder.map(previewResponse.getMappings());

        assertThat(BytesReference.bytes(builder), equalTo(createIndexRequest.mappings()));
    }

    public void testBWCPre77XContent() throws IOException {
        PreviewTransformResponse response = randomPreviewResponse();

        XContentBuilder builder = XContentFactory.jsonBuilder();

        builder.startObject();
        builder.startArray("preview");
        for (Map<String, Object> doc : response.getDocs()) {
            builder.map(doc);
        }
        builder.endArray();
        builder.field("mappings", response.getGeneratedDestIndexSettings().getMappings());
        builder.endObject();
        XContentParser parser = createParser(builder);
        PreviewTransformResponse oldResponse = PreviewTransformResponse.fromXContent(parser);

        assertThat(response.getDocs(), equalTo(oldResponse.getDocs()));
        assertThat(response.getMappings(), equalTo(oldResponse.getMappings()));
        assertTrue(oldResponse.getAliases().isEmpty());
        assertThat(oldResponse.getSettings(), equalTo(Settings.EMPTY));
    }

    private PreviewTransformResponse createTestInstance() {
        return randomPreviewResponse();
    }

    private void toXContent(PreviewTransformResponse response, XContentBuilder builder) throws IOException {
        builder.startObject();
        builder.startArray("preview");
        for (Map<String, Object> doc : response.getDocs()) {
            builder.map(doc);
        }
        builder.endArray();
        builder.startObject("generated_dest_index");
        builder.field("mappings", response.getGeneratedDestIndexSettings().getMappings());

        builder.startObject("settings");
        response.getGeneratedDestIndexSettings().getSettings().toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        builder.startObject("aliases");
        for (Alias alias : response.getGeneratedDestIndexSettings().getAliases()) {
            alias.toXContent(builder, ToXContent.EMPTY_PARAMS);
        }
        builder.endObject();
        builder.endObject();
        builder.endObject();
    }

    private static PreviewTransformResponse randomPreviewResponse() {
        int size = randomIntBetween(0, 10);
        List<Map<String, Object>> data = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            data.add(Map.of(randomAlphaOfLength(10), Map.of("value1", randomIntBetween(1, 100))));
        }

        return new PreviewTransformResponse(data, randomGeneratedDestIndexSettings());
    }

    private static PreviewTransformResponse.GeneratedDestIndexSettings randomGeneratedDestIndexSettings() {
        int size = randomIntBetween(0, 10);

        Map<String, Object> mappings = null;
        if (randomBoolean()) {
            mappings = new HashMap<>(size);

            for (int i = 0; i < size; i++) {
                mappings.put(randomAlphaOfLength(10), Map.of("type", randomAlphaOfLength(10)));
            }
        }

        Settings settings = null;
        if (randomBoolean()) {
            Settings.Builder settingsBuilder = Settings.builder();
            size = randomIntBetween(0, 10);
            for (int i = 0; i < size; i++) {
                settingsBuilder.put(randomAlphaOfLength(10), randomBoolean());
            }
            settings = settingsBuilder.build();
        }

        Set<Alias> aliases = null;
        if (randomBoolean()) {
            aliases = new HashSet<>();
            size = randomIntBetween(0, 10);
            for (int i = 0; i < size; i++) {
                aliases.add(new Alias(randomAlphaOfLength(10)));
            }
        }

        return new PreviewTransformResponse.GeneratedDestIndexSettings(mappings, settings, aliases);
    }
}

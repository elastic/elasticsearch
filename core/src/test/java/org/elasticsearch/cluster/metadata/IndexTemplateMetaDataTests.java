/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.cluster.metadata;

import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.cluster.metadata.AliasMetaData.newAliasMetaDataBuilder;
import static org.hamcrest.CoreMatchers.equalTo;

public class IndexTemplateMetaDataTests extends ESTestCase {

    // bwc for #21009
    public void testIndexTemplateMetaData510() throws IOException {
        IndexTemplateMetaData metaData = IndexTemplateMetaData.builder("foo")
            .patterns(Collections.singletonList("bar"))
            .order(1)
            .settings(Settings.builder()
                .put("setting1", "value1")
                .put("setting2", "value2"))
            .putAlias(newAliasMetaDataBuilder("alias-bar1")).build();

        IndexTemplateMetaData multiMetaData = IndexTemplateMetaData.builder("foo")
            .patterns(Arrays.asList("bar", "foo"))
            .order(1)
            .settings(Settings.builder()
                .put("setting1", "value1")
                .put("setting2", "value2"))
            .putAlias(newAliasMetaDataBuilder("alias-bar1")).build();

        // These bytes were retrieved by Base64 encoding the result of the above with 5_0_0 code
        String templateBytes = "A2ZvbwAAAAEDYmFyAghzZXR0aW5nMQEGdmFsdWUxCHNldHRpbmcyAQZ2YWx1ZTIAAQphbGlhcy1iYXIxAAAAAAA=";
        BytesArray bytes = new BytesArray(Base64.getDecoder().decode(templateBytes));

        try (StreamInput in = bytes.streamInput()) {
            in.setVersion(Version.V_5_0_0);
            IndexTemplateMetaData readMetaData = IndexTemplateMetaData.readFrom(in);
            assertEquals(0, in.available());
            assertEquals(metaData.getName(), readMetaData.getName());
            assertEquals(metaData.getPatterns(), readMetaData.getPatterns());
            assertTrue(metaData.aliases().containsKey("alias-bar1"));
            assertEquals(1, metaData.aliases().size());

            BytesStreamOutput output = new BytesStreamOutput();
            output.setVersion(Version.V_5_0_0);
            readMetaData.writeTo(output);
            assertEquals(bytes.toBytesRef(), output.bytes().toBytesRef());

            // test that multi templates are reverse-compatible.
            // for the bwc case, if multiple patterns, use only the first pattern seen.
            output.reset();
            multiMetaData.writeTo(output);
            assertEquals(bytes.toBytesRef(), output.bytes().toBytesRef());
        }
    }

    public void testIndexTemplateMetaDataXContentRoundTrip() throws Exception {
        ToXContent.Params params = new ToXContent.MapParams(singletonMap("reduce_mappings", "true"));

        String template = "{\"index_patterns\" : [ \".test-*\" ],\"order\" : 1000," +
            "\"settings\" : {\"number_of_shards\" : 1,\"number_of_replicas\" : 0}," +
            "\"mappings\" : {\"doc\" :" +
            "{\"properties\":{\"" +
            randomAlphaOfLength(10) + "\":{\"type\":\"text\"},\"" +
            randomAlphaOfLength(10) + "\":{\"type\":\"keyword\"}}" +
            "}}}";

        BytesReference templateBytes = new BytesArray(template);
        final IndexTemplateMetaData indexTemplateMetaData;
        try (XContentParser parser = XContentHelper.createParser(NamedXContentRegistry.EMPTY, templateBytes, XContentType.JSON)) {
            indexTemplateMetaData = IndexTemplateMetaData.Builder.fromXContent(parser, "test");
        }

        final BytesReference templateBytesRoundTrip;
        try (XContentBuilder builder = XContentBuilder.builder(JsonXContent.jsonXContent)) {
            builder.startObject();
            IndexTemplateMetaData.Builder.toXContent(indexTemplateMetaData, builder, params);
            builder.endObject();
            templateBytesRoundTrip = builder.bytes();
        }

        final IndexTemplateMetaData indexTemplateMetaDataRoundTrip;
        try (XContentParser parser = XContentHelper.createParser(NamedXContentRegistry.EMPTY, templateBytesRoundTrip, XContentType.JSON)) {
            indexTemplateMetaDataRoundTrip = IndexTemplateMetaData.Builder.fromXContent(parser, "test");
        }
        assertThat(indexTemplateMetaData, equalTo(indexTemplateMetaDataRoundTrip));
    }

    public void testValidateInvalidIndexPatterns() throws Exception {
        final IllegalArgumentException emptyPatternError = expectThrows(IllegalArgumentException.class, () -> {
            new IndexTemplateMetaData(randomRealisticUnicodeOfLengthBetween(5, 10), randomInt(), randomInt(),
                Collections.emptyList(), Settings.EMPTY, ImmutableOpenMap.of(), ImmutableOpenMap.of(), ImmutableOpenMap.of());
        });
        assertThat(emptyPatternError.getMessage(), equalTo("Index patterns must not be null or empty; got []"));

        final IllegalArgumentException nullPatternError = expectThrows(IllegalArgumentException.class, () -> {
            new IndexTemplateMetaData(randomRealisticUnicodeOfLengthBetween(5, 10), randomInt(), randomInt(),
                null, Settings.EMPTY, ImmutableOpenMap.of(), ImmutableOpenMap.of(), ImmutableOpenMap.of());
        });
        assertThat(nullPatternError.getMessage(), equalTo("Index patterns must not be null or empty; got null"));

        final String templateWithEmptyPattern = "{\"index_patterns\" : [],\"order\" : 1000," +
            "\"settings\" : {\"number_of_shards\" : 10,\"number_of_replicas\" : 1}," +
            "\"mappings\" : {\"doc\" :" +
            "{\"properties\":{\"" +
            randomAlphaOfLength(10) + "\":{\"type\":\"text\"},\"" +
            randomAlphaOfLength(10) + "\":{\"type\":\"keyword\"}}" +
            "}}}";
        try (XContentParser parser =
                 XContentHelper.createParser(NamedXContentRegistry.EMPTY, new BytesArray(templateWithEmptyPattern), XContentType.JSON)) {
            final IllegalArgumentException ex = expectThrows(IllegalArgumentException.class,
                () -> IndexTemplateMetaData.Builder.fromXContent(parser, randomAlphaOfLengthBetween(1, 100)));
            assertThat(ex.getMessage(), equalTo("Index patterns must not be null or empty; got []"));
        }

        final String templateWithoutPattern = "{\"order\" : 1000," +
            "\"settings\" : {\"number_of_shards\" : 10,\"number_of_replicas\" : 1}," +
            "\"mappings\" : {\"doc\" :" +
            "{\"properties\":{\"" +
            randomAlphaOfLength(10) + "\":{\"type\":\"text\"},\"" +
            randomAlphaOfLength(10) + "\":{\"type\":\"keyword\"}}" +
            "}}}";
        try (XContentParser parser =
                 XContentHelper.createParser(NamedXContentRegistry.EMPTY, new BytesArray(templateWithoutPattern), XContentType.JSON)) {
            final IllegalArgumentException ex = expectThrows(IllegalArgumentException.class,
                () -> IndexTemplateMetaData.Builder.fromXContent(parser, randomAlphaOfLengthBetween(1, 100)));
            assertThat(ex.getMessage(), equalTo("Index patterns must not be null or empty; got null"));
        }
    }
}

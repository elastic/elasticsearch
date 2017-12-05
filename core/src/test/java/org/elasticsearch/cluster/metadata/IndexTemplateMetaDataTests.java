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

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;

import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.equalTo;

public class IndexTemplateMetaDataTests extends ESTestCase {

    public void testIndexTemplateMetaDataXContentRoundTrip() throws Exception {
        ToXContent.Params params = new ToXContent.MapParams(singletonMap("reduce_mappings", "true"));

        String template = "{\"template\" : [ \".test-*\" ],\"order\" : 1000," +
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

    public void testValidateInvalidTemplate() throws Exception {
        final IllegalArgumentException nullPatternError = expectThrows(IllegalArgumentException.class, () -> {
            new IndexTemplateMetaData(randomRealisticUnicodeOfLengthBetween(5, 10), randomInt(), randomInt(),
                null, Settings.EMPTY, ImmutableOpenMap.of(), ImmutableOpenMap.of(), ImmutableOpenMap.of());
        });
        assertThat(nullPatternError.getMessage(), equalTo("Template must not be null"));

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
            assertThat(ex.getMessage(), equalTo("Template must not be null"));
        }
    }
}

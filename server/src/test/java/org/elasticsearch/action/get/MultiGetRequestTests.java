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

package org.elasticsearch.action.get;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;

public class MultiGetRequestTests extends ESTestCase {

    public void testAddWithInvalidKey() throws IOException {
        final XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startArray("doc");
            {
                builder.startObject();
                {
                    builder.field("_type", "type");
                    builder.field("_id", "1");
                }
                builder.endObject();
            }
            builder.endArray();
        }
        builder.endObject();
        final XContentParser parser = createParser(builder);
        final MultiGetRequest mgr = new MultiGetRequest();
        final ParsingException e = expectThrows(
                ParsingException.class,
                () -> {
                    final String defaultIndex = randomAlphaOfLength(5);
                    final String defaultType = randomAlphaOfLength(3);
                    final FetchSourceContext fetchSource = FetchSourceContext.FETCH_SOURCE;
                    mgr.add(defaultIndex, defaultType, null, fetchSource, null, parser, true);
                });
        assertThat(
                e.toString(),
                containsString("unknown key [doc] for a START_ARRAY, expected [docs] or [ids]"));
    }

    public void testUnexpectedField() throws IOException {
        final XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject("docs");
            {
                builder.field("_type", "type");
                builder.field("_id", "1");
            }
            builder.endObject();
        }
        builder.endObject();
        final XContentParser parser = createParser(builder);
        final MultiGetRequest mgr = new MultiGetRequest();
        final ParsingException e = expectThrows(
                ParsingException.class,
                () -> {
                    final String defaultIndex = randomAlphaOfLength(5);
                    final String defaultType = randomAlphaOfLength(3);
                    final FetchSourceContext fetchSource = FetchSourceContext.FETCH_SOURCE;
                    mgr.add(defaultIndex, defaultType, null, fetchSource, null, parser, true);
                });
        assertThat(
                e.toString(),
                containsString(
                        "unexpected token [START_OBJECT], expected [FIELD_NAME] or [START_ARRAY]"));
    }

    public void testAddWithInvalidSourceValueIsRejected() throws Exception {
        String sourceValue = randomFrom("on", "off", "0", "1");
        XContentParser parser = createParser(XContentFactory.jsonBuilder()
            .startObject()
                .startArray("docs")
                    .startObject()
                        .field("_source", sourceValue)
                    .endObject()
                .endArray()
            .endObject()
        );

        MultiGetRequest multiGetRequest = new MultiGetRequest();
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> multiGetRequest.add
            (randomAlphaOfLength(5), randomAlphaOfLength(3), null, FetchSourceContext.FETCH_SOURCE, null, parser, true));
        assertEquals("Failed to parse value [" + sourceValue + "] as only [true] or [false] are allowed.", ex.getMessage());
    }

    public void testAddWithValidSourceValueIsAccepted() throws Exception {
        XContentParser parser = createParser(XContentFactory.jsonBuilder()
            .startObject()
                .startArray("docs")
                    .startObject()
                        .field("_source", randomFrom("false", "true"))
                    .endObject()
                    .startObject()
                        .field("_source", randomBoolean())
                    .endObject()
                .endArray()
            .endObject()
        );

        MultiGetRequest multiGetRequest = new MultiGetRequest();
        multiGetRequest.add(
            randomAlphaOfLength(5), randomAlphaOfLength(3), null, FetchSourceContext.FETCH_SOURCE, null, parser, true);

        assertEquals(2, multiGetRequest.getItems().size());
    }
}

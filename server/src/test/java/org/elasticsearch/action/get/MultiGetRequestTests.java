/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.get;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

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
        try (XContentParser parser = createParser(builder)) {
            final MultiGetRequest mgr = new MultiGetRequest();
            final ParsingException e = expectThrows(ParsingException.class, () -> {
                final String defaultIndex = randomAlphaOfLength(5);
                final FetchSourceContext fetchSource = FetchSourceContext.FETCH_SOURCE;
                mgr.add(defaultIndex, null, fetchSource, null, parser, true);
            });
            assertThat(e.toString(), containsString("unknown key [doc] for a START_ARRAY, expected [docs] or [ids]"));
        }
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
        final ParsingException e = expectThrows(ParsingException.class, () -> {
            final String defaultIndex = randomAlphaOfLength(5);
            final FetchSourceContext fetchSource = FetchSourceContext.FETCH_SOURCE;
            mgr.add(defaultIndex, null, fetchSource, null, parser, true);
        });
        assertThat(e.toString(), containsString("unexpected token [START_OBJECT], expected [FIELD_NAME] or [START_ARRAY]"));
    }

    public void testAddWithValidSourceValueIsAccepted() throws Exception {
        XContentParser parser = createParser(
            XContentFactory.jsonBuilder()
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
        multiGetRequest.add(randomAlphaOfLength(5), null, FetchSourceContext.FETCH_SOURCE, null, parser, true);

        assertEquals(2, multiGetRequest.getItems().size());
    }

    public void testXContentSerialization() throws IOException {
        for (int runs = 0; runs < 20; runs++) {
            MultiGetRequest expected = createTestInstance();
            XContentType xContentType = randomFrom(XContentType.values());
            BytesReference shuffled = toShuffledXContent(expected, xContentType, ToXContent.EMPTY_PARAMS, false);
            try (XContentParser parser = createParser(XContentFactory.xContent(xContentType), shuffled)) {
                MultiGetRequest actual = new MultiGetRequest();
                actual.add(null, null, null, null, parser, true);
                assertThat(parser.nextToken(), nullValue());

                assertThat(actual.items.size(), equalTo(expected.items.size()));
                for (int i = 0; i < expected.items.size(); i++) {
                    MultiGetRequest.Item expectedItem = expected.items.get(i);
                    MultiGetRequest.Item actualItem = actual.items.get(i);
                    assertThat(actualItem, equalTo(expectedItem));
                }
            }
        }
    }

    private MultiGetRequest createTestInstance() {
        int numItems = randomIntBetween(0, 128);
        MultiGetRequest request = new MultiGetRequest();
        for (int i = 0; i < numItems; i++) {
            MultiGetRequest.Item item = new MultiGetRequest.Item(randomAlphaOfLength(4), randomAlphaOfLength(4));
            if (randomBoolean()) {
                item.version(randomNonNegativeLong());
            }
            if (randomBoolean()) {
                item.versionType(randomFrom(VersionType.values()));
            }
            if (randomBoolean()) {
                FetchSourceContext fetchSourceContext;
                if (randomBoolean()) {
                    fetchSourceContext = FetchSourceContext.of(
                        true,
                        generateRandomStringArray(16, 8, false),
                        generateRandomStringArray(5, 4, false)
                    );
                } else {
                    fetchSourceContext = FetchSourceContext.DO_NOT_FETCH_SOURCE;
                }
                item.fetchSourceContext(fetchSourceContext);
            }
            if (randomBoolean()) {
                item.storedFields(generateRandomStringArray(16, 8, false));
            }
            if (randomBoolean()) {
                item.routing(randomAlphaOfLength(4));
            }
            request.add(item);
        }
        return request;
    }

}

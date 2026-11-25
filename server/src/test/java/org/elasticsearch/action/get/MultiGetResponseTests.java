/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.get;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.get.GetResultTests;
import org.elasticsearch.rest.ChunkedRestResponseBodyPart;
import org.elasticsearch.rest.action.RestChunkedToXContentListener;
import org.elasticsearch.rest.action.document.RestMultiGetAction;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class MultiGetResponseTests extends ESTestCase {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(MultiGetResponse.class);

    private static final ParseField TYPE = new ParseField("_type");

    private static final ParseField ERROR = new ParseField("error");

    public void testFromXContent() throws IOException {
        for (int runs = 0; runs < 20; runs++) {
            MultiGetResponse expected = createTestInstance();
            XContentType xContentType = randomFrom(XContentType.values());
            BytesReference shuffled = toShuffledXContent(
                ChunkedToXContent.wrapAsToXContent(expected),
                xContentType,
                ToXContent.EMPTY_PARAMS,
                false
            );
            MultiGetResponse parsed;
            try (XContentParser parser = createParser(XContentFactory.xContent(xContentType), shuffled)) {
                parsed = parseInstance(parser);
                assertNull(parser.nextToken());
            }
            assertNotSame(expected, parsed);

            assertThat(parsed.getResponses().length, equalTo(expected.getResponses().length));
            for (int i = 0; i < expected.getResponses().length; i++) {
                MultiGetItemResponse expectedItem = expected.getResponses()[i];
                MultiGetItemResponse actualItem = parsed.getResponses()[i];
                assertThat(actualItem.getIndex(), equalTo(expectedItem.getIndex()));
                assertThat(actualItem.getId(), equalTo(expectedItem.getId()));
                if (expectedItem.isFailed()) {
                    assertThat(actualItem.isFailed(), is(true));
                    assertThat(actualItem.getFailure().getMessage(), containsString(expectedItem.getFailure().getMessage()));
                } else {
                    assertThat(actualItem.isFailed(), is(false));
                    assertThat(actualItem.getResponse(), equalTo(expectedItem.getResponse()));
                }
            }

        }
    }

    /** Items the serialization has handed out are not kept reachable by it. */
    public void testSerializationDropsSerializedItems() throws Exception {
        int items = randomIntBetween(2, 10);
        List<WeakReference<GetResponse>> itemRefs = new ArrayList<>();
        // the response is not kept in a local, which would keep every item reachable
        Iterator<? extends ToXContent> serialization = unreferencedResponse(items, new BytesArray("{}"), itemRefs).toXContentChunked(
            ToXContent.EMPTY_PARAMS
        );
        int serialized = randomIntBetween(1, items - 1);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        // the first two chunks open the object and the docs array
        for (int i = 0; i < serialized + 2; i++) {
            serialization.next().toXContent(builder, ToXContent.EMPTY_PARAMS);
        }

        // System.gc() is only a hint, so retry until the serialized items have been collected
        assertBusy(() -> {
            System.gc();
            for (int i = 0; i < serialized; i++) {
                assertNull("item [" + i + "] has been serialized", itemRefs.get(i).get());
            }
        });
        for (int i = serialized; i < items; i++) {
            assertNotNull("item [" + i + "] has not been serialized", itemRefs.get(i).get());
        }
        Reference.reachabilityFence(serialization);
    }

    public void testRestResponseDropsSerializedItems() throws Exception {
        int items = randomIntBetween(2, 10);
        List<WeakReference<GetResponse>> itemRefs = new ArrayList<>();
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest.Builder(xContentRegistry()).build(), randomBoolean());
        // each source is larger than the buffer of the JSON generator, so with a size hint of one byte each call encodes one item
        BytesArray source = new BytesArray("{\"payload\":\"" + randomAlphaOfLength(64 * 1024) + "\"}");
        // the response is not kept in a local, which would keep every item reachable
        new RestChunkedToXContentListener<MultiGetResponse>(channel).onResponse(unreferencedResponse(items, source, itemRefs));
        ChunkedRestResponseBodyPart body = channel.capturedResponse().chunkedContent();
        int serialized = randomIntBetween(1, items - 1);
        for (int i = 0; i < serialized; i++) {
            body.encodeChunk(1, BytesRefRecycler.NON_RECYCLING_INSTANCE).close();
        }

        assertBusy(() -> {
            System.gc();
            for (int i = 0; i < serialized; i++) {
                assertNull("item [" + i + "] has been encoded", itemRefs.get(i).get());
            }
        });
        for (int i = serialized; i < items; i++) {
            assertNotNull("item [" + i + "] has not been encoded", itemRefs.get(i).get());
        }
        Reference.reachabilityFence(channel);
    }

    public void testSerializationLeavesResponseUnchanged() {
        MultiGetResponse response = createTestInstance();
        MultiGetItemResponse[] items = response.getResponses().clone();
        String serialized = Strings.toString(ChunkedToXContent.wrapAsToXContent(response));
        assertArrayEquals(items, response.getResponses());
        assertThat(Strings.toString(ChunkedToXContent.wrapAsToXContent(response)), equalTo(serialized));
    }

    private static MultiGetResponse unreferencedResponse(int items, BytesReference source, List<WeakReference<GetResponse>> itemRefs) {
        MultiGetItemResponse[] responses = new MultiGetItemResponse[items];
        for (int i = 0; i < items; i++) {
            GetResponse response = new GetResponse(new GetResult("index", Integer.toString(i), 0, 1, 1, true, source, null, null));
            itemRefs.add(new WeakReference<>(response));
            responses[i] = new MultiGetItemResponse(response, null);
        }
        return new MultiGetResponse(responses);
    }

    private static MultiGetResponse createTestInstance() {
        MultiGetItemResponse[] items = new MultiGetItemResponse[randomIntBetween(0, 128)];
        for (int i = 0; i < items.length; i++) {
            if (randomBoolean()) {
                items[i] = new MultiGetItemResponse(
                    new GetResponse(
                        new GetResult(randomAlphaOfLength(4), randomAlphaOfLength(4), 0, 1, randomNonNegativeLong(), true, null, null, null)
                    ),
                    null
                );
            } else {
                items[i] = new MultiGetItemResponse(
                    null,
                    new MultiGetResponse.Failure(
                        randomAlphaOfLength(4),
                        randomAlphaOfLength(4),
                        new RuntimeException(randomAlphaOfLength(4))
                    )
                );
            }
        }
        return new MultiGetResponse(items);
    }

    public static MultiGetResponse parseInstance(XContentParser parser) throws IOException {
        String currentFieldName = null;
        List<MultiGetItemResponse> items = new ArrayList<>();
        for (XContentParser.Token token = parser.nextToken(); token != XContentParser.Token.END_OBJECT; token = parser.nextToken()) {
            switch (token) {
                case FIELD_NAME:
                    currentFieldName = parser.currentName();
                    break;
                case START_ARRAY:
                    if (MultiGetResponse.DOCS.getPreferredName().equals(currentFieldName)) {
                        for (token = parser.nextToken(); token != XContentParser.Token.END_ARRAY; token = parser.nextToken()) {
                            if (token == XContentParser.Token.START_OBJECT) {
                                items.add(parseItem(parser));
                            }
                        }
                    }
                    break;
                default:
                    // If unknown tokens are encounter then these should be ignored, because
                    // this is parsing logic on the client side.
                    break;
            }
        }
        return new MultiGetResponse(items.toArray(new MultiGetItemResponse[0]));
    }

    private static MultiGetItemResponse parseItem(XContentParser parser) throws IOException {
        String currentFieldName = null;
        String index = null;
        String id = null;
        ElasticsearchException exception = null;
        GetResult getResult = null;
        for (XContentParser.Token token = parser.nextToken(); token != XContentParser.Token.END_OBJECT; token = parser.nextToken()) {
            switch (token) {
                case FIELD_NAME:
                    currentFieldName = parser.currentName();
                    if (MultiGetResponse.INDEX.match(currentFieldName, parser.getDeprecationHandler()) == false
                        && MultiGetResponse.ID.match(currentFieldName, parser.getDeprecationHandler()) == false
                        && ERROR.match(currentFieldName, parser.getDeprecationHandler()) == false) {
                        getResult = GetResultTests.parseInstanceFromEmbedded(parser, index, id);
                    }
                    break;
                case VALUE_STRING:
                    if (MultiGetResponse.INDEX.match(currentFieldName, parser.getDeprecationHandler())) {
                        index = parser.text();
                    } else if (TYPE.match(currentFieldName, parser.getDeprecationHandler())) {
                        deprecationLogger.compatibleCritical("mget_with_types", RestMultiGetAction.TYPES_DEPRECATION_MESSAGE);
                    } else if (MultiGetResponse.ID.match(currentFieldName, parser.getDeprecationHandler())) {
                        id = parser.text();
                    }
                    break;
                case START_OBJECT:
                    if (ERROR.match(currentFieldName, parser.getDeprecationHandler())) {
                        exception = ElasticsearchException.fromXContent(parser);
                    }
                    break;
                default:
                    // If unknown tokens are encounter then these should be ignored, because
                    // this is parsing logic on the client side.
                    break;
            }
            if (getResult != null) {
                break;
            }
        }

        if (exception != null) {
            return new MultiGetItemResponse(null, new MultiGetResponse.Failure(index, id, exception));
        } else {
            GetResponse getResponse = new GetResponse(getResult);
            return new MultiGetItemResponse(getResponse, null);
        }
    }

}

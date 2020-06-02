/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.search;

import org.elasticsearch.Version;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentElasticsearchExtension;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.TransformNamedXContentProvider;
import org.elasticsearch.xpack.core.transform.transforms.SyncConfig;
import org.elasticsearch.xpack.core.transform.transforms.TimeSyncConfig;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.search.GetAsyncSearchRequestTests.randomSearchId;

public class AsyncSearchResponseTests extends ESTestCase {
    private SearchResponse searchResponse = randomSearchResponse();
    private NamedWriteableRegistry namedWriteableRegistry;

    @Before
    public void registerNamedObjects() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, emptyList());

        List<NamedWriteableRegistry.Entry> namedWriteables = searchModule.getNamedWriteables();
        namedWriteables.add(new NamedWriteableRegistry.Entry(SyncConfig.class, TransformField.TIME_BASED_SYNC.getPreferredName(),
            TimeSyncConfig::new));

        List<NamedXContentRegistry.Entry> namedXContents = searchModule.getNamedXContents();
        namedXContents.addAll(new TransformNamedXContentProvider().getNamedXContentParsers());

        namedWriteableRegistry = new NamedWriteableRegistry(namedWriteables);
    }


    protected Writeable.Reader<AsyncSearchResponse> instanceReader() {
        return AsyncSearchResponse::new;
    }

    protected AsyncSearchResponse createTestInstance() {
        return randomAsyncSearchResponse(randomSearchId(), searchResponse);
    }

    protected void assertEqualInstances(AsyncSearchResponse expectedInstance, AsyncSearchResponse newInstance) {
        assertNotSame(newInstance, expectedInstance);
        assertEqualResponses(expectedInstance, newInstance);
    }

    public final void testSerialization() throws IOException {
        for (int runs = 0; runs < 10; runs++) {
            AsyncSearchResponse testInstance = createTestInstance();
            assertSerialization(testInstance);
        }
    }

    protected final AsyncSearchResponse assertSerialization(AsyncSearchResponse testInstance) throws IOException {
        return assertSerialization(testInstance, Version.CURRENT);
    }

    protected final AsyncSearchResponse assertSerialization(AsyncSearchResponse testInstance, Version version) throws IOException {
        AsyncSearchResponse deserializedInstance = copyInstance(testInstance, version);
        assertEqualInstances(testInstance, deserializedInstance);
        return deserializedInstance;
    }

    protected final AsyncSearchResponse copyInstance(AsyncSearchResponse instance) throws IOException {
        return copyInstance(instance, Version.CURRENT);
    }

    protected AsyncSearchResponse copyInstance(AsyncSearchResponse instance, Version version) throws IOException {
        return copyWriteable(instance, namedWriteableRegistry, instanceReader(), version);
    }

    static AsyncSearchResponse randomAsyncSearchResponse(String searchId, SearchResponse searchResponse) {
        int rand = randomIntBetween(0, 2);
        switch (rand) {
            case 0:
                return new AsyncSearchResponse(searchId, randomBoolean(),
                    randomBoolean(), randomNonNegativeLong(), randomNonNegativeLong());

            case 1:
                return new AsyncSearchResponse(searchId, searchResponse, null,
                    randomBoolean(), randomBoolean(), randomNonNegativeLong(), randomNonNegativeLong());

            case 2:
                return new AsyncSearchResponse(searchId, searchResponse,
                    new ScriptException("messageData", new Exception("causeData"), Arrays.asList("stack1", "stack2"),
                        "sourceData", "langData"), randomBoolean(), randomBoolean(),
                    randomNonNegativeLong(), randomNonNegativeLong());

            default:
                throw new AssertionError();
        }
    }

    static SearchResponse randomSearchResponse() {
        long tookInMillis = randomNonNegativeLong();
        int totalShards = randomIntBetween(1, Integer.MAX_VALUE);
        int successfulShards = randomIntBetween(0, totalShards);
        int skippedShards = randomIntBetween(0, successfulShards);
        InternalSearchResponse internalSearchResponse = InternalSearchResponse.empty();
        return new SearchResponse(internalSearchResponse, null, totalShards,
            successfulShards, skippedShards, tookInMillis, ShardSearchFailure.EMPTY_ARRAY, SearchResponse.Clusters.EMPTY);
    }

    static void assertEqualResponses(AsyncSearchResponse expected, AsyncSearchResponse actual) {
        assertEquals(expected.getId(), actual.getId());
        assertEquals(expected.status(), actual.status());
        assertEquals(expected.getFailure() == null, actual.getFailure() == null);
        assertEquals(expected.isRunning(), actual.isRunning());
        assertEquals(expected.isPartial(), actual.isPartial());
        assertEquals(expected.getStartTime(), actual.getStartTime());
        assertEquals(expected.getExpirationTime(), actual.getExpirationTime());
    }

    public void testToXContent() throws IOException {
        Date date = new Date();
        AsyncSearchResponse asyncSearchResponse = new AsyncSearchResponse("id", true, true, date.getTime(), date.getTime());

        try ( XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            builder.prettyPrint();
            asyncSearchResponse.toXContent(builder, ToXContent.EMPTY_PARAMS);
            assertEquals("{\n" +
                "  \"id\" : \"id\",\n" +
                "  \"is_partial\" : true,\n" +
                "  \"is_running\" : true,\n" +
                "  \"start_time_in_millis\" : " + date.getTime() + ",\n" +
                "  \"expiration_time_in_millis\" : " + date.getTime() + "\n" +
                "}", Strings.toString(builder));
        }

        try ( XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            builder.prettyPrint();
            builder.humanReadable(true);
            asyncSearchResponse.toXContent(builder, new ToXContent.MapParams(Collections.singletonMap("human", "true")));
            assertEquals("{\n" +
                "  \"id\" : \"id\",\n" +
                "  \"is_partial\" : true,\n" +
                "  \"is_running\" : true,\n" +
                "  \"start_time\" : \"" + XContentElasticsearchExtension.DEFAULT_DATE_PRINTER.print(date.getTime()) + "\",\n" +
                "  \"start_time_in_millis\" : " + date.getTime() + ",\n" +
                "  \"expiration_time\" : \"" + XContentElasticsearchExtension.DEFAULT_DATE_PRINTER.print(date.getTime()) + "\",\n" +
                "  \"expiration_time_in_millis\" : " + date.getTime() + "\n" +
                "}", Strings.toString(builder));
        }
    }
}

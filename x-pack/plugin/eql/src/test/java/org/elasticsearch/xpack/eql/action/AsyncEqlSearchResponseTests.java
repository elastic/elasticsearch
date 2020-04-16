/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentElasticsearchExtension;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.eql.action.AsyncEqlSearchResponse;
import org.elasticsearch.xpack.core.eql.action.EqlSearchResponse;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.TransformNamedXContentProvider;
import org.elasticsearch.xpack.core.transform.transforms.SyncConfig;
import org.elasticsearch.xpack.core.transform.transforms.TimeSyncConfig;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.eql.action.EqlSearchResponseTests.randomEqlSearchResponse;
import static org.elasticsearch.xpack.eql.action.GetAsyncEqlSearchRequestTests.randomSearchId;

public class AsyncEqlSearchResponseTests extends ESTestCase {
    private final EqlSearchResponse searchResponse = randomEqlSearchResponse();
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


    protected Writeable.Reader<AsyncEqlSearchResponse> instanceReader() {
        return AsyncEqlSearchResponse::new;
    }

    protected AsyncEqlSearchResponse createTestInstance() {
        return randomAsyncEqlSearchResponse(randomSearchId(), searchResponse);
    }

    protected void assertEqualInstances(AsyncEqlSearchResponse expectedInstance, AsyncEqlSearchResponse newInstance) {
        assertNotSame(newInstance, expectedInstance);
        assertEqualResponses(expectedInstance, newInstance);
    }

    public final void testSerialization() throws IOException {
        for (int runs = 0; runs < 10; runs++) {
            AsyncEqlSearchResponse testInstance = createTestInstance();
            assertSerialization(testInstance);
        }
    }

    protected final AsyncEqlSearchResponse assertSerialization(AsyncEqlSearchResponse testInstance) throws IOException {
        return assertSerialization(testInstance, Version.CURRENT);
    }

    protected final AsyncEqlSearchResponse assertSerialization(AsyncEqlSearchResponse testInstance, Version version) throws IOException {
        AsyncEqlSearchResponse deserializedInstance = copyInstance(testInstance, version);
        assertEqualInstances(testInstance, deserializedInstance);
        return deserializedInstance;
    }

    protected final AsyncEqlSearchResponse copyInstance(AsyncEqlSearchResponse instance) throws IOException {
        return copyInstance(instance, Version.CURRENT);
    }

    protected AsyncEqlSearchResponse copyInstance(AsyncEqlSearchResponse instance, Version version) throws IOException {
        return copyWriteable(instance, namedWriteableRegistry, instanceReader(), version);
    }

    static AsyncEqlSearchResponse randomAsyncEqlSearchResponse(String searchId, EqlSearchResponse searchResponse) {
        int rand = randomIntBetween(0, 2);
        switch (rand) {
            case 0:
                return new AsyncEqlSearchResponse(searchId, randomBoolean(),
                    randomBoolean(), randomNonNegativeLong(), randomNonNegativeLong());

            case 1:
                return new AsyncEqlSearchResponse(searchId, searchResponse, null,
                    randomBoolean(), randomBoolean(), randomNonNegativeLong(), randomNonNegativeLong());

            case 2:
                return new AsyncEqlSearchResponse(searchId, searchResponse,
                    new ElasticsearchException(new IOException("boum")), randomBoolean(), randomBoolean(),
                    randomNonNegativeLong(), randomNonNegativeLong());

            default:
                throw new AssertionError();
        }
    }

    static void assertEqualResponses(AsyncEqlSearchResponse expected, AsyncEqlSearchResponse actual) {
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
        AsyncEqlSearchResponse asyncSearchResponse = new AsyncEqlSearchResponse("id", true, true, date.getTime(), date.getTime());

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

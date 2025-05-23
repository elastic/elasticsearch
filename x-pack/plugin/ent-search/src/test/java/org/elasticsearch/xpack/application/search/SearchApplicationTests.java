/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.search;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.application.EnterpriseSearchModuleTestUtils;
import org.junit.Before;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.emptyList;
import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.hamcrest.CoreMatchers.equalTo;

public class SearchApplicationTests extends ESTestCase {
    private NamedWriteableRegistry namedWriteableRegistry;

    @Before
    public void registerNamedObjects() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, emptyList());

        List<NamedWriteableRegistry.Entry> namedWriteables = searchModule.getNamedWriteables();
        namedWriteableRegistry = new NamedWriteableRegistry(namedWriteables);
    }

    public final void testRandomSerialization() throws IOException {
        for (int runs = 0; runs < 10; runs++) {
            SearchApplication testInstance = EnterpriseSearchModuleTestUtils.randomSearchApplication();
            assertTransportSerialization(testInstance);
            assertXContent(testInstance, randomBoolean());
            assertIndexSerialization(testInstance);
        }
    }

    public void testToXContent() throws IOException {
        String content = XContentHelper.stripWhitespace("""
            {
              "name": "my_search_app",
              "indices": ["my_index"],
              "analytics_collection_name": "my_search_app_analytics",
              "template": {
                "script": {
                  "source": {
                    "query": {
                      "query_string": {
                        "query": "{{query_string}}"
                      }
                    }
                  }
                }
              },
              "updated_at_millis": 12345
            }""");
        SearchApplication app = SearchApplication.fromXContentBytes("my_search_app", new BytesArray(content), XContentType.JSON);
        boolean humanReadable = true;
        BytesReference originalBytes = toShuffledXContent(app, XContentType.JSON, ToXContent.EMPTY_PARAMS, humanReadable);
        SearchApplication parsed;
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), originalBytes)) {
            parsed = SearchApplication.fromXContent(app.name(), parser);
        }
        assertToXContentEquivalent(originalBytes, toXContent(parsed, XContentType.JSON, humanReadable), XContentType.JSON);
    }

    public void testToXContentInvalidSearchApplicationName() throws IOException {
        String content = XContentHelper.stripWhitespace("""
            {
              "name": "different_search_app_name",
              "indices": ["my_index"],
              "analytics_collection_name": "my_search_app_analytics",
              "updated_at_millis": 0
            }""");
        expectThrows(
            IllegalArgumentException.class,
            () -> SearchApplication.fromXContentBytes("my_search_app", new BytesArray(content), XContentType.JSON)
        );
    }

    public void testMerge() throws IOException {
        String content = """
            {
              "indices": ["my_index", "my_index_2"],
              "updated_at_millis": 0
            }""";

        String update = """
            {
              "indices": ["my_index_2", "my_index"],
              "analytics_collection_name": "my_search_app_analytics",
              "updated_at_millis": 12345
            }""";
        SearchApplication app = SearchApplication.fromXContentBytes("my_search_app", new BytesArray(content), XContentType.JSON);
        SearchApplication updatedApp = app.merge(new BytesArray(update), XContentType.JSON);
        assertNotSame(app, updatedApp);
        assertThat(updatedApp.indices(), equalTo(new String[] { "my_index", "my_index_2" }));
        assertThat(updatedApp.analyticsCollectionName(), equalTo("my_search_app_analytics"));
        assertThat(updatedApp.updatedAtMillis(), equalTo(12345L));
    }

    private SearchApplication assertXContent(SearchApplication app, boolean humanReadable) throws IOException {
        BytesReference originalBytes = toShuffledXContent(app, XContentType.JSON, ToXContent.EMPTY_PARAMS, humanReadable);
        SearchApplication parsed;
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), originalBytes)) {
            parsed = SearchApplication.fromXContent(app.name(), parser);
        }
        assertToXContentEquivalent(originalBytes, toXContent(parsed, XContentType.JSON, humanReadable), XContentType.JSON);
        return parsed;
    }

    private SearchApplication assertTransportSerialization(SearchApplication testInstance) throws IOException {
        SearchApplication deserializedInstance = copyInstance(testInstance);
        assertNotSame(testInstance, deserializedInstance);
        assertThat(testInstance, equalTo(deserializedInstance));
        return deserializedInstance;
    }

    private SearchApplication assertIndexSerialization(SearchApplication testInstance) throws IOException {
        final SearchApplication deserializedInstance;
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            SearchApplicationIndexService.writeSearchApplicationBinaryWithVersion(
                testInstance,
                output,
                TransportVersions.MINIMUM_COMPATIBLE
            );
            try (
                StreamInput in = new NamedWriteableAwareStreamInput(
                    new InputStreamStreamInput(output.bytes().streamInput()),
                    namedWriteableRegistry
                )
            ) {
                deserializedInstance = SearchApplicationIndexService.parseSearchApplicationBinaryWithVersion(in, testInstance.indices());
            }
        }
        assertNotSame(testInstance, deserializedInstance);
        assertThat(testInstance, equalTo(deserializedInstance));
        return deserializedInstance;
    }

    private SearchApplication copyInstance(SearchApplication instance) throws IOException {
        return copyWriteable(instance, namedWriteableRegistry, SearchApplication::new);
    }
}

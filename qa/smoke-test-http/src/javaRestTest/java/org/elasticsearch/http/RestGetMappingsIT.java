/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.http;

import org.apache.http.client.methods.HttpGet;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Request;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.hamcrest.Matchers;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.aMapWithSize;

@LuceneTestCase.SuppressFileSystems(value = "HandleLimitFS") // we sometimes have >2048 open files
public class RestGetMappingsIT extends HttpSmokeTestCase {

    public void testGetLargeMappingsResponse() throws Exception {
        final XContentBuilder builder = JsonXContent.contentBuilder()
            .startObject()
            .startObject(MapperService.SINGLE_MAPPING_NAME)
            .startObject("properties");
        final int fields = randomIntBetween(500, 1000);
        for (int i = 0; i < fields; i++) {
            builder.startObject("loooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooong-named-field-" + i)
                .field("type", "text")
                .field("store", true)
                .endObject();
        }
        builder.endObject().endObject().endObject();
        assertAcked(admin().indices().preparePutTemplate("large-mapping").setPatterns(List.of("test-idx-*")).setMapping(builder).get());
        final int indexCount = randomIntBetween(50, 150);
        final PlainActionFuture<Void> f = PlainActionFuture.newFuture();
        final ActionListener<CreateIndexResponse> listener = new GroupedActionListener<>(f.map(l -> null), indexCount);
        for (int i = 0; i < indexCount; i++) {
            admin().indices()
                .prepareCreate("test-idx-" + i)
                .setSettings(AbstractSnapshotIntegTestCase.SINGLE_SHARD_NO_REPLICA)
                .execute(listener);
        }
        f.get();
        final var response = getRestClient().performRequest(new Request(HttpGet.METHOD_NAME, "/_mappings"));
        try (
            InputStream input = response.getEntity().getContent();
            XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, input)
        ) {
            final Map<String, Object> mappings = parser.map();
            assertThat(mappings, Matchers.aMapWithSize(indexCount));
            final Object idx0 = mappings.get("test-idx-0");
            @SuppressWarnings("unchecked")
            var properties = ((Map<String, Map<String, Object>>) idx0).get("mappings").get("properties");
            assertThat((Map<?, ?>) properties, aMapWithSize(fields));
            mappings.forEach((key, value) -> assertEquals(key, idx0, value));
        }
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.entsearch.engine;

import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.emptyList;
import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.hamcrest.CoreMatchers.equalTo;

public class EngineTests extends ESTestCase {
    private NamedWriteableRegistry namedWriteableRegistry;

    @Before
    public void registerNamedObjects() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, emptyList());

        List<NamedWriteableRegistry.Entry> namedWriteables = searchModule.getNamedWriteables();
        namedWriteableRegistry = new NamedWriteableRegistry(namedWriteables);
    }

    public final void testRandomSerialization() throws IOException {
        for (int runs = 0; runs < 10; runs++) {
            Engine testInstance = randomEngine();
            assertTransportSerialization(testInstance);
            assertXContent(testInstance, randomBoolean());
            assertIndexSerialization(testInstance, Version.CURRENT);
        }
    }

    public void testToXContent() throws IOException {
        String content = XContentHelper.stripWhitespace("""
            {
              "indices": ["my_index"],
              "analytics_collection_name": "my_engine_analytics",
              "updated_at_millis": 0
            }""");
        Engine engine = Engine.fromXContentBytes("my_engine", new BytesArray(content), XContentType.JSON);
        boolean humanReadable = true;
        BytesReference originalBytes = toShuffledXContent(engine, XContentType.JSON, ToXContent.EMPTY_PARAMS, humanReadable);
        Engine parsed;
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), originalBytes)) {
            parsed = Engine.fromXContent(engine.name(), parser);
        }
        assertToXContentEquivalent(originalBytes, toXContent(parsed, XContentType.JSON, humanReadable), XContentType.JSON);
    }

    public void testMerge() {
        String content = """
            {
              "indices": ["my_index", "my_index_2"],
              "updated_at_millis": 0
            }""";

        String update = """
            {
              "indices": ["my_index_2", "my_index"],
              "analytics_collection_name": "my_engine_analytics"
            }""";
        Engine engine = Engine.fromXContentBytes("my_engine", new BytesArray(content), XContentType.JSON);
        Engine updatedEngine = engine.merge(new BytesArray(update), XContentType.JSON, BigArrays.NON_RECYCLING_INSTANCE);
        assertNotSame(engine, updatedEngine);
        assertThat(updatedEngine.indices(), equalTo(new String[] { "my_index", "my_index_2" }));
        assertThat(updatedEngine.analyticsCollectionName(), equalTo("my_engine_analytics"));
    }

    private Engine assertXContent(Engine engine, boolean humanReadable) throws IOException {
        BytesReference originalBytes = toShuffledXContent(engine, XContentType.JSON, ToXContent.EMPTY_PARAMS, humanReadable);
        Engine parsed;
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), originalBytes)) {
            parsed = Engine.fromXContent(engine.name(), parser);
        }
        assertToXContentEquivalent(originalBytes, toXContent(parsed, XContentType.JSON, humanReadable), XContentType.JSON);
        return parsed;
    }

    private Engine assertTransportSerialization(Engine testInstance) throws IOException {
        return assertTransportSerialization(testInstance, Version.CURRENT);
    }

    private Engine assertTransportSerialization(Engine testInstance, Version version) throws IOException {
        Engine deserializedInstance = copyInstance(testInstance, version);
        assertNotSame(testInstance, deserializedInstance);
        assertThat(testInstance, equalTo(deserializedInstance));
        return deserializedInstance;
    }

    private Engine assertIndexSerialization(Engine testInstance, Version version) throws IOException {
        final Engine deserializedInstance;
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.setTransportVersion(version.transportVersion);
            EngineIndexService.writeEngineBinaryWithVersion(testInstance, output, version.minimumCompatibilityVersion());
            try (
                StreamInput in = new NamedWriteableAwareStreamInput(
                    new InputStreamStreamInput(output.bytes().streamInput()),
                    namedWriteableRegistry
                )
            ) {
                deserializedInstance = EngineIndexService.parseEngineBinaryWithVersion(in);
            }
        }
        assertNotSame(testInstance, deserializedInstance);
        assertThat(testInstance, equalTo(deserializedInstance));
        return deserializedInstance;
    }

    private Engine copyInstance(Engine instance, Version version) throws IOException {
        return copyWriteable(instance, namedWriteableRegistry, Engine::new, version.transportVersion);
    }

    static Engine randomEngine() {
        String name = randomAlphaOfLengthBetween(5, 10);
        String[] indices = new String[randomIntBetween(1, 3)];
        for (int i = 0; i < indices.length; i++) {
            indices[i] = randomAlphaOfLengthBetween(10, 20);
        }
        String analyticsCollectionName = randomBoolean() ? randomAlphaOfLengthBetween(10, 15) : null;
        return new Engine(name, indices, analyticsCollectionName);
    }
}

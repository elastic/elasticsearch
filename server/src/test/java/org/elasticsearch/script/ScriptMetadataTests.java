/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.script;

import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.UncheckedIOException;

import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasKey;

public class ScriptMetadataTests extends AbstractChunkedSerializingTestCase<ScriptMetadata> {

    public void testGetScript() throws Exception {
        ScriptMetadata.Builder builder = new ScriptMetadata.Builder(null);

        XContentBuilder sourceBuilder = XContentFactory.jsonBuilder();
        sourceBuilder.startObject()
            .startObject("script")
            .field("lang", "_lang")
            .startObject("source")
            .field("field", "value")
            .endObject()
            .endObject()
            .endObject();
        builder.storeScript("source_template", StoredScriptSource.parse(BytesReference.bytes(sourceBuilder), sourceBuilder.contentType()));

        sourceBuilder = XContentFactory.jsonBuilder();
        sourceBuilder.startObject().startObject("script").field("lang", "_lang").field("source", "_source").endObject().endObject();
        builder.storeScript("script", StoredScriptSource.parse(BytesReference.bytes(sourceBuilder), sourceBuilder.contentType()));

        ScriptMetadata scriptMetadata = builder.build();
        assertEquals("_source", scriptMetadata.getStoredScript("script").getSource());
        assertEquals("{\"field\":\"value\"}", scriptMetadata.getStoredScript("source_template").getSource());
    }

    public void testDiff() {
        ScriptMetadata.Builder builder = new ScriptMetadata.Builder(null);
        builder.storeScript("1", StoredScriptSource.parse(new BytesArray("""
            {"script":{"lang":"mustache","source":{"foo":"abc"}}}"""), XContentType.JSON));
        builder.storeScript("2", StoredScriptSource.parse(new BytesArray("""
            {"script":{"lang":"mustache","source":{"foo":"def"}}}"""), XContentType.JSON));
        builder.storeScript("3", StoredScriptSource.parse(new BytesArray("""
            {"script":{"lang":"mustache","source":{"foo":"ghi"}}}"""), XContentType.JSON));
        ScriptMetadata scriptMetadata1 = builder.build();

        builder = new ScriptMetadata.Builder(scriptMetadata1);
        builder.storeScript("2", StoredScriptSource.parse(new BytesArray("""
            {"script":{"lang":"mustache","source":{"foo":"changed"}}}"""), XContentType.JSON));
        builder.deleteScript("3");
        builder.storeScript("4", StoredScriptSource.parse(new BytesArray("""
            {"script":{"lang":"mustache","source":{"foo":"jkl"}}}"""), XContentType.JSON));
        ScriptMetadata scriptMetadata2 = builder.build();

        ScriptMetadata.ScriptMetadataDiff diff = (ScriptMetadata.ScriptMetadataDiff) scriptMetadata2.diff(scriptMetadata1);
        DiffableUtils.MapDiff<?, ?, ?> pipelinesDiff = (DiffableUtils.MapDiff) diff.pipelines;
        assertThat(pipelinesDiff.getDeletes(), contains("3"));
        assertThat(Maps.ofEntries(pipelinesDiff.getDiffs()), allOf(aMapWithSize(1), hasKey("2")));
        assertThat(Maps.ofEntries(pipelinesDiff.getUpserts()), allOf(aMapWithSize(1), hasKey("4")));

        ScriptMetadata result = (ScriptMetadata) diff.apply(scriptMetadata1);
        assertEquals("{\"foo\":\"abc\"}", result.getStoredScript("1").getSource());
        assertEquals("{\"foo\":\"changed\"}", result.getStoredScript("2").getSource());
        assertEquals("{\"foo\":\"jkl\"}", result.getStoredScript("4").getSource());
    }

    public void testBuilder() {
        ScriptMetadata.Builder builder = new ScriptMetadata.Builder(null);
        builder.storeScript("_id", StoredScriptSource.parse(new BytesArray("""
            {"script": {"lang": "painless", "source": "1 + 1"} }"""), XContentType.JSON));

        ScriptMetadata result = builder.build();
        assertEquals("1 + 1", result.getStoredScript("_id").getSource());
    }

    public void testLoadEmptyScripts() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject().startObject("script").field("lang", "lang").field("source", "").endObject().endObject();
        XContentParser parser = XContentType.JSON.xContent()
            .createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                BytesReference.bytes(builder).streamInput()
            );
        assertTrue(ScriptMetadata.fromXContent(parser).getStoredScripts().isEmpty());

        builder = XContentFactory.jsonBuilder();
        builder.startObject().startObject("script").field("lang", "mustache").field("source", "").endObject().endObject();
        parser = XContentType.JSON.xContent()
            .createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                BytesReference.bytes(builder).streamInput()
            );
        assertTrue(ScriptMetadata.fromXContent(parser).getStoredScripts().isEmpty());
    }

    private ScriptMetadata randomScriptMetadata(XContentType sourceContentType, int minNumberScripts) throws IOException {
        ScriptMetadata.Builder builder = new ScriptMetadata.Builder(null);
        int numScripts = scaledRandomIntBetween(minNumberScripts, 32);
        for (int i = 0; i < numScripts; i++) {
            XContentBuilder sourceBuilder = XContentBuilder.builder(sourceContentType.xContent());
            sourceBuilder.startObject()
                .field("script")
                .startObject()
                .field("lang", randomAlphaOfLength(4))
                .field("source", randomAlphaOfLength(10))
                .endObject()
                .endObject();
            builder.storeScript(
                randomAlphaOfLength(i + 1),
                StoredScriptSource.parse(BytesReference.bytes(sourceBuilder), sourceBuilder.contentType())
            );
        }
        return builder.build();
    }

    @Override
    protected ScriptMetadata createTestInstance() {
        try {
            return randomScriptMetadata(randomFrom(XContentType.values()), 0);
        } catch (IOException ioe) {
            throw new UncheckedIOException(ioe);
        }
    }

    @Override
    protected Writeable.Reader<ScriptMetadata> instanceReader() {
        return ScriptMetadata::new;
    }

    @Override
    protected ScriptMetadata mutateInstance(ScriptMetadata instance) throws IOException {
        ScriptMetadata.Builder builder = new ScriptMetadata.Builder(instance);
        return switch (between(0, 2)) {
            case 0 -> {
                XContentType contentType = randomFrom(XContentType.values());
                XContentBuilder sourceBuilder = XContentBuilder.builder(contentType.xContent());
                sourceBuilder.startObject()
                    .field("script")
                    .startObject()
                    .field("lang", randomAlphaOfLength(4))
                    .field("source", randomAlphaOfLength(10))
                    .endObject()
                    .endObject();
                builder.storeScript(
                    randomValueOtherThanMany(id -> instance.getStoredScripts().containsKey(id), () -> randomAlphaOfLength(8)),
                    StoredScriptSource.parse(BytesReference.bytes(sourceBuilder), sourceBuilder.contentType())
                );
                yield builder.build();
            }
            case 1 -> {
                if (instance.getStoredScripts().isEmpty()) {
                    // Empty map: delete/replace are impossible — fall back to adding one script via the builder.
                    XContentType contentType = randomFrom(XContentType.values());
                    XContentBuilder sourceBuilder = XContentBuilder.builder(contentType.xContent());
                    sourceBuilder.startObject()
                        .field("script")
                        .startObject()
                        .field("lang", randomAlphaOfLength(4))
                        .field("source", randomAlphaOfLength(10))
                        .endObject()
                        .endObject();
                    builder.storeScript(
                        randomAlphaOfLength(8),
                        StoredScriptSource.parse(BytesReference.bytes(sourceBuilder), sourceBuilder.contentType())
                    );
                    yield builder.build();
                }
                builder.deleteScript(randomFrom(instance.getStoredScripts().keySet().toArray(String[]::new)));
                yield builder.build();
            }
            default -> {
                if (instance.getStoredScripts().isEmpty()) {
                    XContentType contentType = randomFrom(XContentType.values());
                    XContentBuilder sourceBuilder = XContentBuilder.builder(contentType.xContent());
                    sourceBuilder.startObject()
                        .field("script")
                        .startObject()
                        .field("lang", randomAlphaOfLength(4))
                        .field("source", randomAlphaOfLength(10))
                        .endObject()
                        .endObject();
                    builder.storeScript(
                        randomAlphaOfLength(8),
                        StoredScriptSource.parse(BytesReference.bytes(sourceBuilder), sourceBuilder.contentType())
                    );
                    yield builder.build();
                }
                String id = randomFrom(instance.getStoredScripts().keySet().toArray(String[]::new));
                XContentType contentType = randomFrom(XContentType.values());
                XContentBuilder sourceBuilder = XContentBuilder.builder(contentType.xContent());
                sourceBuilder.startObject()
                    .field("script")
                    .startObject()
                    .field("lang", randomAlphaOfLength(4))
                    .field("source", randomAlphaOfLength(10))
                    .endObject()
                    .endObject();
                builder.storeScript(
                    id,
                    randomValueOtherThan(
                        instance.getStoredScript(id),
                        () -> StoredScriptSource.parse(BytesReference.bytes(sourceBuilder), sourceBuilder.contentType())
                    )
                );
                yield builder.build();
            }
        };
    }

    @Override
    protected ScriptMetadata doParseInstance(XContentParser parser) {
        try {
            return ScriptMetadata.fromXContent(parser);
        } catch (IOException ioe) {
            throw new UncheckedIOException(ioe);
        }
    }
}

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
package org.elasticsearch.script;

import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;

public class ScriptMetaDataTests extends AbstractSerializingTestCase<ScriptMetaData> {

    public void testFromXContentLoading() throws Exception {
        // failure to load to old namespace scripts with the same id but different langs
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject().field("lang0#id0", "script0").field("lang1#id0", "script1").endObject();
        XContentParser parser0 = XContentType.JSON.xContent()
            .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                    BytesReference.bytes(builder).streamInput());
        expectThrows(IllegalArgumentException.class, () -> ScriptMetaData.fromXContent(parser0));

        // failure to load a new namespace script and old namespace script with the same id but different langs
        builder = XContentFactory.jsonBuilder();
        builder.startObject().field("lang0#id0", "script0")
            .startObject("id0").field("lang", "lang1").field("source", "script1").endObject().endObject();
        XContentParser parser1 = XContentType.JSON.xContent()
            .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                    BytesReference.bytes(builder).streamInput());
        expectThrows(IllegalArgumentException.class, () -> ScriptMetaData.fromXContent(parser1));

        // failure to load a new namespace script and old namespace script with the same id but different langs with additional scripts
        builder = XContentFactory.jsonBuilder();
        builder.startObject().field("lang0#id0", "script0").field("lang0#id1", "script1")
            .startObject("id1").field("lang", "lang0").field("source", "script0").endObject()
            .startObject("id0").field("lang", "lang1").field("source", "script1").endObject().endObject();
        XContentParser parser2 = XContentType.JSON.xContent()
            .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                    BytesReference.bytes(builder).streamInput());
        expectThrows(IllegalArgumentException.class, () -> ScriptMetaData.fromXContent(parser2));

        // okay to load the same script from the new and old namespace if the lang is the same
        builder = XContentFactory.jsonBuilder();
        builder.startObject().field("lang0#id0", "script0")
            .startObject("id0").field("lang", "lang0").field("source", "script1").endObject().endObject();
        XContentParser parser3 = XContentType.JSON.xContent()
            .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                    BytesReference.bytes(builder).streamInput());
        ScriptMetaData.fromXContent(parser3);
    }

    public void testGetScript() throws Exception {
        ScriptMetaData.Builder builder = new ScriptMetaData.Builder(null);

        XContentBuilder sourceBuilder = XContentFactory.jsonBuilder();
        sourceBuilder.startObject().startObject("script")
            .field("lang", "_lang")
            .startObject("source").field("field", "value").endObject()
            .endObject().endObject();
        builder.storeScript("source_template", StoredScriptSource.parse(BytesReference.bytes(sourceBuilder),
            sourceBuilder.contentType()));

        sourceBuilder = XContentFactory.jsonBuilder();
        sourceBuilder.startObject().startObject("script").field("lang", "_lang").field("source", "_source").endObject().endObject();
        builder.storeScript("script", StoredScriptSource.parse(BytesReference.bytes(sourceBuilder), sourceBuilder.contentType()));

        ScriptMetaData scriptMetaData = builder.build();
        assertEquals("_source", scriptMetaData.getStoredScript("script").getSource());
        assertEquals("{\"field\":\"value\"}", scriptMetaData.getStoredScript("source_template").getSource());
    }

    public void testDiff() throws Exception {
        ScriptMetaData.Builder builder = new ScriptMetaData.Builder(null);
        builder.storeScript("1", StoredScriptSource.parse(
            new BytesArray("{\"script\":{\"lang\":\"mustache\",\"source\":{\"foo\":\"abc\"}}}"), XContentType.JSON));
        builder.storeScript("2", StoredScriptSource.parse(
            new BytesArray("{\"script\":{\"lang\":\"mustache\",\"source\":{\"foo\":\"def\"}}}"), XContentType.JSON));
        builder.storeScript("3", StoredScriptSource.parse(
            new BytesArray("{\"script\":{\"lang\":\"mustache\",\"source\":{\"foo\":\"ghi\"}}}"), XContentType.JSON));
        ScriptMetaData scriptMetaData1 = builder.build();

        builder = new ScriptMetaData.Builder(scriptMetaData1);
        builder.storeScript("2", StoredScriptSource.parse(
            new BytesArray("{\"script\":{\"lang\":\"mustache\",\"source\":{\"foo\":\"changed\"}}}"), XContentType.JSON));
        builder.deleteScript("3");
        builder.storeScript("4", StoredScriptSource.parse(
            new BytesArray("{\"script\":{\"lang\":\"mustache\",\"source\":{\"foo\":\"jkl\"}}}"), XContentType.JSON));
        ScriptMetaData scriptMetaData2 = builder.build();

        ScriptMetaData.ScriptMetadataDiff diff = (ScriptMetaData.ScriptMetadataDiff) scriptMetaData2.diff(scriptMetaData1);
        assertEquals(1, ((DiffableUtils.MapDiff) diff.pipelines).getDeletes().size());
        assertEquals("3", ((DiffableUtils.MapDiff) diff.pipelines).getDeletes().get(0));
        assertEquals(1, ((DiffableUtils.MapDiff) diff.pipelines).getDiffs().size());
        assertNotNull(((DiffableUtils.MapDiff) diff.pipelines).getDiffs().get("2"));
        assertEquals(1, ((DiffableUtils.MapDiff) diff.pipelines).getUpserts().size());
        assertNotNull(((DiffableUtils.MapDiff) diff.pipelines).getUpserts().get("4"));

        ScriptMetaData result = (ScriptMetaData) diff.apply(scriptMetaData1);
        assertEquals("{\"foo\":\"abc\"}", result.getStoredScript("1").getSource());
        assertEquals("{\"foo\":\"changed\"}", result.getStoredScript("2").getSource());
        assertEquals("{\"foo\":\"jkl\"}", result.getStoredScript("4").getSource());
    }

    public void testBuilder() {
        ScriptMetaData.Builder builder = new ScriptMetaData.Builder(null);
        builder.storeScript("_id", StoredScriptSource.parse(
            new BytesArray("{\"script\": {\"lang\": \"painless\", \"source\": \"1 + 1\"} }"), XContentType.JSON));

        ScriptMetaData result = builder.build();
        assertEquals("1 + 1", result.getStoredScript("_id").getSource());
    }

    public void testLoadEmptyScripts() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject().field("mustache#empty", "").endObject();
        XContentParser parser = XContentType.JSON.xContent()
            .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                BytesReference.bytes(builder).streamInput());
        ScriptMetaData.fromXContent(parser);
        assertWarnings("empty templates should no longer be used");

        builder = XContentFactory.jsonBuilder();
        builder.startObject().field("lang#empty", "").endObject();
        parser = XContentType.JSON.xContent()
            .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                BytesReference.bytes(builder).streamInput());
        ScriptMetaData.fromXContent(parser);
        assertWarnings("empty scripts should no longer be used");

        builder = XContentFactory.jsonBuilder();
        builder.startObject().startObject("script").field("lang", "lang").field("source", "").endObject().endObject();
        parser = XContentType.JSON.xContent()
            .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                BytesReference.bytes(builder).streamInput());
        ScriptMetaData.fromXContent(parser);
        assertWarnings("empty scripts should no longer be used");

        builder = XContentFactory.jsonBuilder();
        builder.startObject().startObject("script").field("lang", "mustache").field("source", "").endObject().endObject();
        parser = XContentType.JSON.xContent()
            .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                BytesReference.bytes(builder).streamInput());
        ScriptMetaData.fromXContent(parser);
        assertWarnings("empty templates should no longer be used");
    }

    public void testOldStyleDropped() throws IOException {
        XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent());

        builder.startObject();
        {
            builder.startObject("painless#test");
            {
                builder.field("lang", "painless");
                builder.field("source", "code");
            }
            builder.endObject();
            builder.startObject("lang#test");
            {
                builder.field("lang", "test");
                builder.field("source", "code");
            }
            builder.endObject();
            builder.startObject("test");
            {
                builder.field("lang", "painless");
                builder.field("source", "code");
            }
            builder.endObject();
        }
        builder.endObject();

        XContentParser parser = XContentType.JSON.xContent()
                .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                        BytesReference.bytes(builder).streamInput());
        ScriptMetaData smd = ScriptMetaData.fromXContent(parser);
        assertNull(smd.getStoredScript("painless#test"));
        assertNull(smd.getStoredScript("lang#test"));
        assertEquals(new StoredScriptSource("painless", "code", Collections.emptyMap()), smd.getStoredScript("test"));
        assertEquals(1, smd.getStoredScripts().size());
    }

    @Override
    protected boolean enableWarningsCheck() {
        return true;
    }

    private ScriptMetaData randomScriptMetaData(XContentType sourceContentType, int minNumberScripts) throws IOException {
        ScriptMetaData.Builder builder = new ScriptMetaData.Builder(null);
        int numScripts = scaledRandomIntBetween(minNumberScripts, 32);
        for (int i = 0; i < numScripts; i++) {
            XContentBuilder sourceBuilder = XContentBuilder.builder(sourceContentType.xContent());
            sourceBuilder.startObject().field("script").startObject()
                .field("lang", randomAlphaOfLength(4)).field("source", randomAlphaOfLength(10))
                .endObject().endObject();
            builder.storeScript(randomAlphaOfLength(i + 1),
                StoredScriptSource.parse(BytesReference.bytes(sourceBuilder), sourceBuilder.contentType()));
        }
        return builder.build();
    }

    @Override
    protected ScriptMetaData createTestInstance() {
        try {
            return randomScriptMetaData(randomFrom(XContentType.values()), 0);
        } catch (IOException ioe) {
            throw new UncheckedIOException(ioe);
        }
    }

    @Override
    protected Writeable.Reader<ScriptMetaData> instanceReader() {
        return ScriptMetaData::new;
    }

    @Override
    protected ScriptMetaData mutateInstance(ScriptMetaData instance) throws IOException {
        // ScriptMetaData doesn't allow us to see the scripts inside it so
        // the best we can do here is create a new random instance and rely
        // on the fact that the new instance is very unlikely to be equal to
        // the old one
        return randomScriptMetaData(randomFrom(XContentType.values()), 1);
    }

    @Override
    protected ScriptMetaData doParseInstance(XContentParser parser) {
        try {
            return ScriptMetaData.fromXContent(parser);
        } catch (IOException ioe) {
            throw new UncheckedIOException(ioe);
        }
    }
}

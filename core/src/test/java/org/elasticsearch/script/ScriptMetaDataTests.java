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
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.io.UncheckedIOException;

public class ScriptMetaDataTests extends AbstractSerializingTestCase<ScriptMetaData> {

    public void testGetScript() throws Exception {
        ScriptMetaData.Builder builder = new ScriptMetaData.Builder(null);

        XContentBuilder sourceBuilder = XContentFactory.jsonBuilder();
        sourceBuilder.startObject().startObject("template").field("field", "value").endObject().endObject();
        builder.storeScript("template", StoredScriptSource.parse(sourceBuilder.bytes(), sourceBuilder.contentType()));

        sourceBuilder = XContentFactory.jsonBuilder();
        sourceBuilder.startObject().field("template", "value").endObject();
        builder.storeScript("template_field", StoredScriptSource.parse(sourceBuilder.bytes(), sourceBuilder.contentType()));

        sourceBuilder = XContentFactory.jsonBuilder();
        sourceBuilder.startObject().startObject("script").field("field", "value").endObject().endObject();
        builder.storeScript("script", StoredScriptSource.parse(sourceBuilder.bytes(), sourceBuilder.contentType()));

        sourceBuilder = XContentFactory.jsonBuilder();
        sourceBuilder.startObject().field("script", "value").endObject();
        builder.storeScript("script_field", StoredScriptSource.parse(sourceBuilder.bytes(), sourceBuilder.contentType()));

        sourceBuilder = XContentFactory.jsonBuilder();
        sourceBuilder.startObject().field("field", "value").endObject();
        builder.storeScript("any", StoredScriptSource.parse(sourceBuilder.bytes(), sourceBuilder.contentType()));

        ScriptMetaData scriptMetaData = builder.build();
        assertEquals("{\"field\":\"value\"}", scriptMetaData.getStoredScript("lang").getSource());
        assertEquals("value", scriptMetaData.getStoredScript("lang").getSource());
        assertEquals("{\"field\":\"value\"}", scriptMetaData.getStoredScript("lang").getSource());
        assertEquals("value", scriptMetaData.getStoredScript("lang").getSource());
        assertEquals("{\"field\":\"value\"}", scriptMetaData.getStoredScript("lang").getSource());
    }

    public void testDiff() throws Exception {
        ScriptMetaData.Builder builder = new ScriptMetaData.Builder(null);
        builder.storeScript("1", StoredScriptSource.parse(new BytesArray("{\"foo\":\"abc\"}"), XContentType.JSON));
        builder.storeScript("2", StoredScriptSource.parse(new BytesArray("{\"foo\":\"def\"}"), XContentType.JSON));
        builder.storeScript("3", StoredScriptSource.parse(new BytesArray("{\"foo\":\"ghi\"}"), XContentType.JSON));
        ScriptMetaData scriptMetaData1 = builder.build();

        builder = new ScriptMetaData.Builder(scriptMetaData1);
        builder.storeScript("2", StoredScriptSource.parse(new BytesArray("{\"foo\":\"changed\"}"), XContentType.JSON));
        builder.deleteScript("3");
        builder.storeScript("4", StoredScriptSource.parse(new BytesArray("{\"foo\":\"jkl\"}"), XContentType.JSON));
        ScriptMetaData scriptMetaData2 = builder.build();

        ScriptMetaData.ScriptMetadataDiff diff = (ScriptMetaData.ScriptMetadataDiff) scriptMetaData2.diff(scriptMetaData1);
        assertEquals(2, ((DiffableUtils.MapDiff) diff.pipelines).getDeletes().size());
        assertEquals("3", ((DiffableUtils.MapDiff) diff.pipelines).getDeletes().get(0));
        assertEquals(2, ((DiffableUtils.MapDiff) diff.pipelines).getDiffs().size());
        assertNotNull(((DiffableUtils.MapDiff) diff.pipelines).getDiffs().get("2"));
        assertEquals(2, ((DiffableUtils.MapDiff) diff.pipelines).getUpserts().size());
        assertNotNull(((DiffableUtils.MapDiff) diff.pipelines).getUpserts().get("4"));

        ScriptMetaData result = (ScriptMetaData) diff.apply(scriptMetaData1);
        assertEquals("{\"foo\":\"abc\"}", result.getStoredScript("1").getSource());
        assertEquals("{\"foo\":\"changed\"}", result.getStoredScript("2").getSource());
        assertEquals("{\"foo\":\"jkl\"}", result.getStoredScript("4").getSource());
    }

    public void testBuilder() {
        ScriptMetaData.Builder builder = new ScriptMetaData.Builder(null);
        builder.storeScript("_id", StoredScriptSource.parse(new BytesArray("{\"script\":\"1 + 1\"}"), XContentType.JSON));

        ScriptMetaData result = builder.build();
        assertEquals("1 + 1", result.getStoredScript("_id").getSource());
    }

    private ScriptMetaData randomScriptMetaData(XContentType sourceContentType) throws IOException {
        ScriptMetaData.Builder builder = new ScriptMetaData.Builder(null);
        int numScripts = scaledRandomIntBetween(0, 32);
        for (int i = 0; i < numScripts; i++) {
            XContentBuilder sourceBuilder = XContentBuilder.builder(sourceContentType.xContent());
            sourceBuilder.startObject().field("script", randomAlphaOfLength(4)).endObject();
            builder.storeScript(randomAlphaOfLength(i + 1),
                StoredScriptSource.parse(sourceBuilder.bytes(), sourceBuilder.contentType()));
        }
        return builder.build();
    }

    @Override
    protected ScriptMetaData createTestInstance() {
        try {
            return randomScriptMetaData(randomFrom(XContentType.values()));
        } catch (IOException ioe) {
            throw new UncheckedIOException(ioe);
        }
    }

    @Override
    protected Writeable.Reader<ScriptMetaData> instanceReader() {
        return ScriptMetaData::new;
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

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
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;


public class ScriptMetaDataTests extends ESTestCase {

    public void testGetScript() throws Exception {
        ScriptMetaData.Builder builder = new ScriptMetaData.Builder(null);

        XContentBuilder sourceBuilder = XContentFactory.jsonBuilder();
        sourceBuilder.startObject().startObject("template").field("field", "value").endObject().endObject();
        builder.storeScript("lang", "template", sourceBuilder.bytes());

        sourceBuilder = XContentFactory.jsonBuilder();
        sourceBuilder.startObject().field("template", "value").endObject();
        builder.storeScript("lang", "template_field", sourceBuilder.bytes());

        sourceBuilder = XContentFactory.jsonBuilder();
        sourceBuilder.startObject().startObject("script").field("field", "value").endObject().endObject();
        builder.storeScript("lang", "script", sourceBuilder.bytes());

        sourceBuilder = XContentFactory.jsonBuilder();
        sourceBuilder.startObject().field("script", "value").endObject();
        builder.storeScript("lang", "script_field", sourceBuilder.bytes());

        sourceBuilder = XContentFactory.jsonBuilder();
        sourceBuilder.startObject().field("field", "value").endObject();
        builder.storeScript("lang", "any", sourceBuilder.bytes());

        ScriptMetaData scriptMetaData = builder.build();
        assertEquals("{\"field\":\"value\"}", scriptMetaData.getScript("lang", "template"));
        assertEquals("value", scriptMetaData.getScript("lang", "template_field"));
        assertEquals("{\"field\":\"value\"}", scriptMetaData.getScript("lang", "script"));
        assertEquals("value", scriptMetaData.getScript("lang", "script_field"));
        assertEquals("{\"field\":\"value\"}", scriptMetaData.getScript("lang", "any"));
    }

    public void testToAndFromXContent() throws IOException {
        XContentType contentType = randomFrom(XContentType.values());
        XContentBuilder xContentBuilder = XContentBuilder.builder(contentType.xContent());
        ScriptMetaData expected = randomScriptMetaData(contentType);

        xContentBuilder.startObject();
        expected.toXContent(xContentBuilder, new ToXContent.MapParams(Collections.emptyMap()));
        xContentBuilder.endObject();
        xContentBuilder = shuffleXContent(xContentBuilder);

        XContentParser parser = XContentHelper.createParser(xContentBuilder.bytes());
        parser.nextToken();
        ScriptMetaData result = ScriptMetaData.PROTO.fromXContent(parser);
        assertEquals(expected, result);
        assertEquals(expected.hashCode(), result.hashCode());
    }

    public void testReadFromWriteTo() throws IOException {
        ScriptMetaData expected = randomScriptMetaData(randomFrom(XContentType.values()));
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        expected.writeTo(new OutputStreamStreamOutput(out));

        ScriptMetaData result = ScriptMetaData.PROTO.readFrom(new InputStreamStreamInput(new ByteArrayInputStream(out.toByteArray())));
        assertEquals(expected, result);
        assertEquals(expected.hashCode(), result.hashCode());
    }

    public void testDiff() throws Exception {
        ScriptMetaData.Builder builder = new ScriptMetaData.Builder(null);
        builder.storeScript("lang", "1", new BytesArray("abc"));
        builder.storeScript("lang", "2", new BytesArray("def"));
        builder.storeScript("lang", "3", new BytesArray("ghi"));
        ScriptMetaData scriptMetaData1 = builder.build();

        builder = new ScriptMetaData.Builder(scriptMetaData1);
        builder.storeScript("lang", "2", new BytesArray("changed"));
        builder.deleteScript("lang", "3");
        builder.storeScript("lang", "4", new BytesArray("jkl"));
        ScriptMetaData scriptMetaData2 = builder.build();

        ScriptMetaData.ScriptMetadataDiff diff = (ScriptMetaData.ScriptMetadataDiff) scriptMetaData2.diff(scriptMetaData1);
        assertEquals(1, ((DiffableUtils.MapDiff) diff.pipelines).getDeletes().size());
        assertEquals("lang#3", ((DiffableUtils.MapDiff) diff.pipelines).getDeletes().get(0));
        assertEquals(1, ((DiffableUtils.MapDiff) diff.pipelines).getDiffs().size());
        assertNotNull(((DiffableUtils.MapDiff) diff.pipelines).getDiffs().get("lang#2"));
        assertEquals(1, ((DiffableUtils.MapDiff) diff.pipelines).getUpserts().size());
        assertNotNull(((DiffableUtils.MapDiff) diff.pipelines).getUpserts().get("lang#4"));

        ScriptMetaData result = (ScriptMetaData) diff.apply(scriptMetaData1);
        assertEquals(new BytesArray("abc"), result.getScriptAsBytes("lang", "1"));
        assertEquals(new BytesArray("changed"), result.getScriptAsBytes("lang", "2"));
        assertEquals(new BytesArray("jkl"), result.getScriptAsBytes("lang", "4"));
    }

    public void testBuilder() {
        ScriptMetaData.Builder builder = new ScriptMetaData.Builder(null);
        builder.storeScript("_lang", "_id", new BytesArray("{\"script\":\"1 + 1\"}"));

        IllegalArgumentException e =
                expectThrows(IllegalArgumentException.class, () -> builder.storeScript("_lang#", "_id", new BytesArray("{}")));
        assertEquals("stored script language can't contain: '#'", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> builder.storeScript("_lang", "_id#", new BytesArray("{}")));
        assertEquals("stored script id can't contain: '#'", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> builder.deleteScript("_lang#", "_id"));
        assertEquals("stored script language can't contain: '#'", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> builder.deleteScript("_lang", "_id#"));
        assertEquals("stored script id can't contain: '#'", e.getMessage());

        ScriptMetaData result = builder.build();
        assertEquals("1 + 1", result.getScript("_lang", "_id"));
    }

    private ScriptMetaData randomScriptMetaData(XContentType sourceContentType) throws IOException {
        ScriptMetaData.Builder builder = new ScriptMetaData.Builder(null);
        int numScripts = scaledRandomIntBetween(0, 32);
        for (int i = 0; i < numScripts; i++) {
            String lang = randomAsciiOfLength(4);
            XContentBuilder sourceBuilder = XContentBuilder.builder(sourceContentType.xContent());
            sourceBuilder.startObject().field(randomAsciiOfLength(4), randomAsciiOfLength(4)).endObject();
            builder.storeScript(lang, randomAsciiOfLength(i + 1), sourceBuilder.bytes());
        }
        return builder.build();
    }

}

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

import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class ScriptTests extends ESTestCase {

    public void testScriptParsing() throws IOException {
        Script expectedScript = createScript();
        try (XContentBuilder builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()))) {
            expectedScript.toXContent(builder, ToXContent.EMPTY_PARAMS);
            try (XContentParser parser = createParser(builder)) {
                Script actualScript = Script.parse(parser);
                assertThat(actualScript, equalTo(expectedScript));
            }
        }
    }

    public void testScriptSerialization() throws IOException {
        Script expectedScript = createScript();
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            expectedScript.writeTo(new OutputStreamStreamOutput(out));
            try (ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray())) {
                Script actualScript = new Script(new InputStreamStreamInput(in));
                assertThat(actualScript, equalTo(expectedScript));
            }
        }
    }

    private Script createScript() throws IOException {
        final Map<String, Object> params = randomBoolean() ? Collections.emptyMap() : Collections.singletonMap("key", "value");
        ScriptType scriptType = randomFrom(ScriptType.values());
        String script;
        if (scriptType == ScriptType.INLINE) {
            try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
                builder.startObject();
                builder.field("field", randomAlphaOfLengthBetween(1, 5));
                builder.endObject();
                script = builder.string();
            }
        } else {
            script = randomAlphaOfLengthBetween(1, 5);
        }
        return new Script(
            scriptType,
            scriptType == ScriptType.STORED ? null : randomFrom("_lang1", "_lang2", "_lang3"),
            script,
            scriptType == ScriptType.INLINE ?
                    Collections.singletonMap(Script.CONTENT_TYPE_OPTION, XContentType.JSON.mediaType()) : null, params
        );
    }

    public void testParse() throws IOException {
        Script expectedScript = createScript();
        try (XContentBuilder builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()))) {
            expectedScript.toXContent(builder, ToXContent.EMPTY_PARAMS);
            Settings settings = Settings.fromXContent(createParser(builder));
            Script actualScript = Script.parse(settings);
            assertThat(actualScript, equalTo(expectedScript));
        }
    }
}

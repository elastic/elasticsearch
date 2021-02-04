/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class CalculatedFieldTests extends MapperServiceTestCase {

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return Set.of(new TestScriptPlugin());
    }

    public void testCalculatedFieldLength() throws IOException {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {
            b.startObject("message").field("type", "text").endObject();
            b.startObject("message_length");
            b.field("type", "long");
            b.field("script", "length");
            b.endObject();
        }));

        ParsedDocument doc = mapper.parse(source(b -> b.field("message", "this is some text")));
        IndexableField[] lengthFields = doc.rootDoc().getFields("message_length");
        assertEquals(2, lengthFields.length);
        assertEquals("LongPoint <message_length:17>", lengthFields[0].toString());
        assertEquals("docValuesType=SORTED_NUMERIC<message_length:17>", lengthFields[1].toString());
    }

    public void testSerialization() throws IOException {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {
            b.startObject("message").field("type", "text").endObject();
            b.startObject("message_length");
            b.field("type", "long");
            b.field("script", "length");
            b.endObject();
        }));
        assertEquals(
            "{\"_doc\":{\"properties\":{\"message\":{\"type\":\"text\"}," +
                "\"message_length\":{\"type\":\"long\",\"script\":{\"source\":\"length\",\"lang\":\"painless\"}}}}}",
            Strings.toString(mapper));
    }

    public static class TestScriptPlugin extends Plugin implements ScriptPlugin {
        @Override
        public List<ScriptContext<?>> getContexts() {
            return List.of(NumberFieldMapper.SCRIPT_CONTEXT);
        }

        @Override
        public ScriptEngine getScriptEngine(Settings settings, Collection<ScriptContext<?>> contexts) {
            return new ScriptEngine() {
                @Override
                public String getType() {
                    return "painless";
                }

                @Override
                public <FactoryType> FactoryType compile(String name, String code, ScriptContext<FactoryType> context, Map<String, String> params) {
                    return (FactoryType) factory(code);
                }

                @Override
                public Set<ScriptContext<?>> getSupportedContexts() {
                    return Set.of(NumberFieldMapper.SCRIPT_CONTEXT);
                }

                private NumberFieldMapper.ScriptFactory factory(String code) {
                    switch (code) {
                        case "length":
                            return () -> new NumberFieldMapper.NumberScript() {
                                @Override
                                public void execute() {
                                    SourceLookup source = (SourceLookup) getParams().get("_source");
                                    for (Object v : source.extractRawValues("message")) {
                                        emit(Objects.toString(v).length());
                                    }
                                }
                            };
                        default:
                            throw new IllegalArgumentException("Cannot compile script [" + code + "]");
                    }
                }
            };
        }
    }

}

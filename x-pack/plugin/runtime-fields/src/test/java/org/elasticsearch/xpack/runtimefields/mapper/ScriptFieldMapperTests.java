/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.mapper;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.xpack.runtimefields.LongScriptFieldScript;
import org.elasticsearch.xpack.runtimefields.RuntimeFields;
import org.elasticsearch.xpack.runtimefields.StringScriptFieldScript;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.instanceOf;

public class ScriptFieldMapperTests extends ESSingleNodeTestCase {

    private static final String[] SUPPORTED_RUNTIME_TYPES = new String[] { "keyword" };

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class, RuntimeFields.class, TestScriptPlugin.class);
    }

    public void testRuntimeTypeIsRequired() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("my_field")
            .field("type", "script")
            .field("script", "value('test')")
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        MapperParsingException exception = expectThrows(MapperParsingException.class, () -> createIndex("test", Settings.EMPTY, mapping));
        assertEquals("Failed to parse mapping: runtime_type must be specified for script field [my_field]", exception.getMessage());
    }

    public void testScriptIsRequired() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("my_field")
            .field("type", "script")
            .field("runtime_type", randomFrom(SUPPORTED_RUNTIME_TYPES))
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        MapperParsingException exception = expectThrows(MapperParsingException.class, () -> createIndex("test", Settings.EMPTY, mapping));
        assertEquals("Failed to parse mapping: script must be specified for script field [my_field]", exception.getMessage());
    }

    public void testStoredScriptsAreNotSupported() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("my_field")
            .field("type", "script")
            .field("runtime_type", randomFrom(SUPPORTED_RUNTIME_TYPES))
            .startObject("script")
            .field("id", "test")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        MapperParsingException exception = expectThrows(MapperParsingException.class, () -> createIndex("test", Settings.EMPTY, mapping));
        assertEquals(
            "Failed to parse mapping: stored scripts specified but not supported when defining script field [my_field]",
            exception.getMessage()
        );
    }

    public void testKeyword() throws IOException {
        MapperService mapperService = createIndex("test", Settings.EMPTY, mapping("keyword")).mapperService();
        FieldMapper mapper = (FieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(mapper, instanceOf(ScriptFieldMapper.class));
        assertEquals(Strings.toString(mapping("keyword")), Strings.toString(mapperService.documentMapper()));
    }

    public void testLong() throws IOException {
        MapperService mapperService = createIndex("test", Settings.EMPTY, mapping("long")).mapperService();
        FieldMapper mapper = (FieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(mapper, instanceOf(ScriptFieldMapper.class));
        assertEquals(Strings.toString(mapping("long")), Strings.toString(mapperService.documentMapper()));
    }

    private XContentBuilder mapping(String type) throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject();
        {
            mapping.startObject("_doc");
            {
                mapping.startObject("properties");
                {
                    mapping.startObject("field");
                    {
                        mapping.field("type", "script").field("runtime_type", type);
                        mapping.startObject("script");
                        {
                            mapping.field("source", "dummy_source").field("lang", "test");
                        }
                        mapping.endObject();
                    }
                    mapping.endObject();
                }
                mapping.endObject();
            }
            mapping.endObject();
        }
        return mapping.endObject();
    }

    public static class TestScriptPlugin extends Plugin implements ScriptPlugin {
        @Override
        public ScriptEngine getScriptEngine(Settings settings, Collection<ScriptContext<?>> contexts) {
            return new ScriptEngine() {
                @Override
                public String getType() {
                    return "test";
                }

                @Override
                public <FactoryType> FactoryType compile(
                    String name,
                    String code,
                    ScriptContext<FactoryType> context,
                    Map<String, String> paramsMap
                ) {
                    if ("dummy_source".equals(code)) {
                        @SuppressWarnings("unchecked")
                        FactoryType castFactory = (FactoryType) dummyScriptFactory(context);
                        return castFactory;
                    }
                    throw new IllegalArgumentException("No test script for [" + code + "]");
                }

                private Object dummyScriptFactory(ScriptContext<?> context) {
                    if (context == StringScriptFieldScript.CONTEXT) {
                        return (StringScriptFieldScript.Factory) (params, lookup) -> ctx -> new StringScriptFieldScript(
                            params,
                            lookup,
                            ctx
                        ) {
                            @Override
                            public void execute() {
                                results.add("test");
                            }
                        };
                    }
                    if (context == LongScriptFieldScript.CONTEXT) {
                        return (LongScriptFieldScript.Factory) (params, lookup) -> ctx -> new LongScriptFieldScript(params, lookup, ctx) {
                            @Override
                            public void execute() {
                                add(1);
                            }
                        };
                    }
                    throw new IllegalArgumentException("No test script for [" + context + "]");
                }

                @Override
                public Set<ScriptContext<?>> getSupportedContexts() {
                    return Set.of(StringScriptFieldScript.CONTEXT, LongScriptFieldScript.CONTEXT);
                }
            };
        }
    }
}

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
import org.elasticsearch.xpack.runtimefields.RuntimeFields;
import org.elasticsearch.xpack.runtimefields.StringScriptFieldScript;

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

    @AwaitsFix(bugUrl = "needs to be fixed upstream")
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

    @AwaitsFix(bugUrl = "needs to be fixed upstream")
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

    public void testUnsupportedRuntimeType() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("field")
            .field("type", "script")
            .field("runtime_type", "unsupported")
            .startObject("script")
            .field("source", "value('test')")
            .field("lang", "test")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        MapperParsingException exc = expectThrows(MapperParsingException.class, () -> createIndex("test", Settings.EMPTY, mapping));
        assertEquals("Failed to parse mapping: runtime_type [unsupported] not supported", exc.getMessage());
    }

    public void testDefaultMapping() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("field")
            .field("type", "script")
            .field("runtime_type", randomFrom(SUPPORTED_RUNTIME_TYPES))
            .startObject("script")
            .field("source", "value('test')")
            .field("lang", "test")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        MapperService mapperService = createIndex("test", Settings.EMPTY, mapping).mapperService();
        FieldMapper mapper = (FieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(mapper, instanceOf(ScriptFieldMapper.class));
        assertEquals(Strings.toString(mapping), Strings.toString(mapperService.documentMapper()));
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
                    if ("value('test')".equals(code)) {
                        StringScriptFieldScript.Factory factory = (params, lookup) -> ctx -> new StringScriptFieldScript(
                            params,
                            lookup,
                            ctx
                        ) {
                            @Override
                            public void execute() {
                                this.results.add("test");
                            }
                        };
                        @SuppressWarnings("unchecked")
                        FactoryType castFactory = (FactoryType) factory;
                        return castFactory;
                    }
                    throw new IllegalArgumentException("No test script for [" + code + "]");
                }

                @Override
                public Set<ScriptContext<?>> getSupportedContexts() {
                    return Set.of(StringScriptFieldScript.CONTEXT);
                }
            };
        }
    }
}

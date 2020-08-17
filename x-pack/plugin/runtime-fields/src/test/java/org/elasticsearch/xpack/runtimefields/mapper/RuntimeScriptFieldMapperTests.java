/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.mapper;

import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.CheckedSupplier;
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
import org.elasticsearch.xpack.runtimefields.BooleanScriptFieldScript;
import org.elasticsearch.xpack.runtimefields.DateScriptFieldScript;
import org.elasticsearch.xpack.runtimefields.DateScriptFieldScriptTests;
import org.elasticsearch.xpack.runtimefields.DoubleScriptFieldScript;
import org.elasticsearch.xpack.runtimefields.IpScriptFieldScript;
import org.elasticsearch.xpack.runtimefields.LongScriptFieldScript;
import org.elasticsearch.xpack.runtimefields.RuntimeFields;
import org.elasticsearch.xpack.runtimefields.StringScriptFieldScript;
import org.elasticsearch.xpack.runtimefields.TestScriptEngine;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class RuntimeScriptFieldMapperTests extends ESSingleNodeTestCase {

    private final String[] runtimeTypes;

    public RuntimeScriptFieldMapperTests() {
        this.runtimeTypes = RuntimeScriptFieldMapper.Builder.FIELD_TYPE_RESOLVER.keySet().toArray(new String[0]);
        Arrays.sort(runtimeTypes);
    }

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
            .field("type", "runtime_script")
            .field("script", "keyword('test')")
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        MapperParsingException exception = expectThrows(MapperParsingException.class, () -> createIndex("test", Settings.EMPTY, mapping));
        assertEquals("Failed to parse mapping: runtime_type must be specified for runtime_script field [my_field]", exception.getMessage());
    }

    public void testScriptIsRequired() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("my_field")
            .field("type", "runtime_script")
            .field("runtime_type", randomFrom(runtimeTypes))
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        MapperParsingException exception = expectThrows(MapperParsingException.class, () -> createIndex("test", Settings.EMPTY, mapping));
        assertEquals("Failed to parse mapping: script must be specified for runtime_script field [my_field]", exception.getMessage());
    }

    public void testCopyToIsNotSupported() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("my_field")
            .field("type", "runtime_script")
            .field("runtime_type", randomFrom(runtimeTypes))
            .field("script", "keyword('test')")
            .field("copy_to", "field")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        MapperParsingException exception = expectThrows(MapperParsingException.class, () -> createIndex("test", Settings.EMPTY, mapping));
        assertEquals("Failed to parse mapping: runtime_script field does not support [copy_to]", exception.getMessage());
    }

    public void testMultiFieldsIsNotSupported() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("my_field")
            .field("type", "runtime_script")
            .field("runtime_type", randomFrom(runtimeTypes))
            .field("script", "keyword('test')")
            .startObject("fields")
            .startObject("test")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        MapperParsingException exception = expectThrows(MapperParsingException.class, () -> createIndex("test", Settings.EMPTY, mapping));
        assertEquals("Failed to parse mapping: runtime_script field does not support [fields]", exception.getMessage());
    }

    public void testStoredScriptsAreNotSupported() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("my_field")
            .field("type", "runtime_script")
            .field("runtime_type", randomFrom(runtimeTypes))
            .startObject("script")
            .field("id", "test")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        MapperParsingException exception = expectThrows(MapperParsingException.class, () -> createIndex("test", Settings.EMPTY, mapping));
        assertEquals(
            "Failed to parse mapping: stored scripts specified but not supported for runtime_script field [my_field]",
            exception.getMessage()
        );
    }

    public void testUnsupportedRuntimeType() {
        MapperParsingException exc = expectThrows(
            MapperParsingException.class,
            () -> createIndex("test", Settings.EMPTY, mapping("unsupported"))
        );
        assertEquals(
            "Failed to parse mapping: runtime_type [unsupported] not supported for runtime_script field [field]",
            exc.getMessage()
        );
    }

    public void testBoolean() throws IOException {
        MapperService mapperService = createIndex("test", Settings.EMPTY, mapping("boolean")).mapperService();
        FieldMapper mapper = (FieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(mapper, instanceOf(RuntimeScriptFieldMapper.class));
        assertEquals(Strings.toString(mapping("boolean")), Strings.toString(mapperService.documentMapper()));
    }

    public void testDouble() throws IOException {
        MapperService mapperService = createIndex("test", Settings.EMPTY, mapping("double")).mapperService();
        FieldMapper mapper = (FieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(mapper, instanceOf(RuntimeScriptFieldMapper.class));
        assertEquals(Strings.toString(mapping("double")), Strings.toString(mapperService.documentMapper()));
    }

    public void testIp() throws IOException {
        MapperService mapperService = createIndex("test", Settings.EMPTY, mapping("ip")).mapperService();
        FieldMapper mapper = (FieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(mapper, instanceOf(RuntimeScriptFieldMapper.class));
        assertEquals(Strings.toString(mapping("ip")), Strings.toString(mapperService.documentMapper()));
    }

    public void testKeyword() throws IOException {
        MapperService mapperService = createIndex("test", Settings.EMPTY, mapping("keyword")).mapperService();
        FieldMapper mapper = (FieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(mapper, instanceOf(RuntimeScriptFieldMapper.class));
        assertEquals(Strings.toString(mapping("keyword")), Strings.toString(mapperService.documentMapper()));
    }

    public void testLong() throws IOException {
        MapperService mapperService = createIndex("test", Settings.EMPTY, mapping("long")).mapperService();
        FieldMapper mapper = (FieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(mapper, instanceOf(RuntimeScriptFieldMapper.class));
        assertEquals(Strings.toString(mapping("long")), Strings.toString(mapperService.documentMapper()));
    }

    public void testDate() throws IOException {
        MapperService mapperService = createIndex("test", Settings.EMPTY, mapping("date")).mapperService();
        FieldMapper mapper = (FieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(mapper, instanceOf(RuntimeScriptFieldMapper.class));
        assertEquals(Strings.toString(mapping("date")), Strings.toString(mapperService.documentMapper()));
    }

    public void testDateWithFormat() throws IOException {
        CheckedSupplier<XContentBuilder, IOException> mapping = () -> mapping("date", b -> b.field("format", "yyyy-MM-dd"));
        MapperService mapperService = createIndex("test", Settings.EMPTY, mapping.get()).mapperService();
        FieldMapper mapper = (FieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(mapper, instanceOf(RuntimeScriptFieldMapper.class));
        assertEquals(Strings.toString(mapping.get()), Strings.toString(mapperService.documentMapper()));
    }

    public void testDateWithLocale() throws IOException {
        CheckedSupplier<XContentBuilder, IOException> mapping = () -> mapping("date", b -> b.field("locale", "en_GB"));
        MapperService mapperService = createIndex("test", Settings.EMPTY, mapping.get()).mapperService();
        FieldMapper mapper = (FieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(mapper, instanceOf(RuntimeScriptFieldMapper.class));
        assertEquals(Strings.toString(mapping.get()), Strings.toString(mapperService.documentMapper()));
    }

    public void testDateWithLocaleAndFormat() throws IOException {
        CheckedSupplier<XContentBuilder, IOException> mapping = () -> mapping(
            "date",
            b -> b.field("format", "yyyy-MM-dd").field("locale", "en_GB")
        );
        MapperService mapperService = createIndex("test", Settings.EMPTY, mapping.get()).mapperService();
        FieldMapper mapper = (FieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(mapper, instanceOf(RuntimeScriptFieldMapper.class));
        assertEquals(Strings.toString(mapping.get()), Strings.toString(mapperService.documentMapper()));
    }

    public void testNonDateWithFormat() throws IOException {
        String runtimeType = randomValueOtherThan("date", () -> randomFrom(runtimeTypes));
        Exception e = expectThrows(
            MapperParsingException.class,
            () -> createIndex("test", Settings.EMPTY, mapping(runtimeType, b -> b.field("format", "yyyy-MM-dd")))
        );
        assertThat(e.getMessage(), equalTo("Failed to parse mapping: format can not be specified for runtime_type [" + runtimeType + "]"));
    }

    public void testNonDateWithLocale() throws IOException {
        String runtimeType = randomValueOtherThan("date", () -> randomFrom(runtimeTypes));
        Exception e = expectThrows(
            MapperParsingException.class,
            () -> createIndex("test", Settings.EMPTY, mapping(runtimeType, b -> b.field("locale", "en_GB")))
        );
        assertThat(e.getMessage(), equalTo("Failed to parse mapping: locale can not be specified for runtime_type [" + runtimeType + "]"));
    }

    public void testFieldCaps() throws Exception {
        for (String runtimeType : runtimeTypes) {
            String scriptIndex = "test_" + runtimeType + "_script";
            String concreteIndex = "test_" + runtimeType + "_concrete";
            createIndex(scriptIndex, Settings.EMPTY, mapping(runtimeType));
            {
                XContentBuilder mapping = XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("_doc")
                    .startObject("properties")
                    .startObject("field")
                    .field("type", runtimeType)
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject();
                createIndex(concreteIndex, Settings.EMPTY, mapping);
            }
            FieldCapabilitiesResponse response = client().prepareFieldCaps("test_" + runtimeType + "_*").setFields("field").get();
            assertThat(response.getIndices(), arrayContainingInAnyOrder(scriptIndex, concreteIndex));
            Map<String, FieldCapabilities> field = response.getField("field");
            assertEquals(1, field.size());
            FieldCapabilities fieldCapabilities = field.get(runtimeType);
            assertTrue(fieldCapabilities.isSearchable());
            assertTrue(fieldCapabilities.isAggregatable());
            assertEquals(runtimeType, fieldCapabilities.getType());
            assertNull(fieldCapabilities.nonAggregatableIndices());
            assertNull(fieldCapabilities.nonSearchableIndices());
            assertEquals("field", fieldCapabilities.getName());
        }
    }

    private XContentBuilder mapping(String type) throws IOException {
        return mapping(type, builder -> {});
    }

    private XContentBuilder mapping(String type, CheckedConsumer<XContentBuilder, IOException> extra) throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject();
        {
            mapping.startObject("_doc");
            {
                mapping.startObject("properties");
                {
                    mapping.startObject("field");
                    {
                        mapping.field("type", "runtime_script").field("runtime_type", type);
                        mapping.startObject("script");
                        {
                            mapping.field("source", "dummy_source").field("lang", "test");
                        }
                        mapping.endObject();
                        extra.accept(mapping);
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
            return new TestScriptEngine() {
                @Override
                protected Object buildScriptFactory(ScriptContext<?> context) {
                    if (context == BooleanScriptFieldScript.CONTEXT) {
                        return (BooleanScriptFieldScript.Factory) (params, lookup) -> ctx -> new BooleanScriptFieldScript(
                            params,
                            lookup,
                            ctx
                        ) {
                            @Override
                            public void execute() {
                                new BooleanScriptFieldScript.Value(this).value(true);
                            }
                        };
                    }
                    if (context == DateScriptFieldScript.CONTEXT) {
                        return DateScriptFieldScriptTests.DUMMY;
                    }
                    if (context == DoubleScriptFieldScript.CONTEXT) {
                        return (DoubleScriptFieldScript.Factory) (params, lookup) -> ctx -> new DoubleScriptFieldScript(
                            params,
                            lookup,
                            ctx
                        ) {
                            @Override
                            public void execute() {
                                new DoubleScriptFieldScript.Value(this).value(1.0);
                            }
                        };
                    }
                    if (context == IpScriptFieldScript.CONTEXT) {
                        return (IpScriptFieldScript.Factory) (params, lookup) -> ctx -> new IpScriptFieldScript(params, lookup, ctx) {
                            @Override
                            public void execute() {
                                new IpScriptFieldScript.StringValue(this).stringValue("192.168.0.1");
                            }
                        };
                    }
                    if (context == StringScriptFieldScript.CONTEXT) {
                        return (StringScriptFieldScript.Factory) (params, lookup) -> ctx -> new StringScriptFieldScript(
                            params,
                            lookup,
                            ctx
                        ) {
                            @Override
                            public void execute() {
                                new StringScriptFieldScript.Value(this).value("test");
                            }
                        };
                    }
                    if (context == LongScriptFieldScript.CONTEXT) {
                        return (LongScriptFieldScript.Factory) (params, lookup) -> ctx -> new LongScriptFieldScript(params, lookup, ctx) {
                            @Override
                            public void execute() {
                                new LongScriptFieldScript.Value(this).value(1);
                            }
                        };
                    }
                    throw new IllegalArgumentException("No test script for [" + context + "]");
                }

                @Override
                public Set<ScriptContext<?>> getSupportedContexts() {
                    return Set.of(
                        BooleanScriptFieldScript.CONTEXT,
                        DateScriptFieldScript.CONTEXT,
                        DoubleScriptFieldScript.CONTEXT,
                        StringScriptFieldScript.CONTEXT,
                        LongScriptFieldScript.CONTEXT
                    );
                }
            };
        }
    }
}

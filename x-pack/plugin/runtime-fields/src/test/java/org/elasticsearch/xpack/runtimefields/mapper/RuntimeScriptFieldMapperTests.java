/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.mapper;

import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperTestCase;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.xpack.runtimefields.BooleanScriptFieldScript;
import org.elasticsearch.xpack.runtimefields.BooleanScriptFieldScriptTests;
import org.elasticsearch.xpack.runtimefields.DateScriptFieldScript;
import org.elasticsearch.xpack.runtimefields.DateScriptFieldScriptTests;
import org.elasticsearch.xpack.runtimefields.DoubleScriptFieldScript;
import org.elasticsearch.xpack.runtimefields.DoubleScriptFieldScriptTests;
import org.elasticsearch.xpack.runtimefields.IpScriptFieldScript;
import org.elasticsearch.xpack.runtimefields.IpScriptFieldScriptTests;
import org.elasticsearch.xpack.runtimefields.LongScriptFieldScript;
import org.elasticsearch.xpack.runtimefields.LongScriptFieldScriptTests;
import org.elasticsearch.xpack.runtimefields.RuntimeFields;
import org.elasticsearch.xpack.runtimefields.StringScriptFieldScript;
import org.elasticsearch.xpack.runtimefields.StringScriptFieldScriptTests;
import org.elasticsearch.xpack.runtimefields.TestScriptEngine;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class RuntimeScriptFieldMapperTests extends MapperTestCase {

    private final String[] runtimeTypes;

    public RuntimeScriptFieldMapperTests() {
        this.runtimeTypes = RuntimeScriptFieldMapper.Builder.FIELD_TYPE_RESOLVER.keySet().toArray(new String[0]);
        Arrays.sort(runtimeTypes);
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "runtime_script").field("runtime_type", "keyword");
        b.startObject("script").field("source", "dummy_source").field("lang", "test").endObject();
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

        MapperParsingException exception = expectThrows(MapperParsingException.class, () -> createMapperService(mapping));
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

        MapperParsingException exception = expectThrows(MapperParsingException.class, () -> createMapperService(mapping));
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
        MapperParsingException exception = expectThrows(MapperParsingException.class, () -> createMapperService(mapping));
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
        MapperParsingException exception = expectThrows(MapperParsingException.class, () -> createMapperService(mapping));
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
        MapperParsingException exception = expectThrows(MapperParsingException.class, () -> createMapperService(mapping));
        assertEquals(
            "Failed to parse mapping: stored scripts specified but not supported for runtime_script field [my_field]",
            exception.getMessage()
        );
    }

    public void testUnsupportedRuntimeType() {
        MapperParsingException exc = expectThrows(MapperParsingException.class, () -> createMapperService(mapping("unsupported")));
        assertEquals(
            "Failed to parse mapping: runtime_type [unsupported] not supported for runtime_script field [field]",
            exc.getMessage()
        );
    }

    public void testBoolean() throws IOException {
        MapperService mapperService = createMapperService(mapping("boolean"));
        FieldMapper mapper = (FieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(mapper, instanceOf(RuntimeScriptFieldMapper.class));
        assertEquals(Strings.toString(mapping("boolean")), Strings.toString(mapperService.documentMapper()));
    }

    public void testDouble() throws IOException {
        MapperService mapperService = createMapperService(mapping("double"));
        FieldMapper mapper = (FieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(mapper, instanceOf(RuntimeScriptFieldMapper.class));
        assertEquals(Strings.toString(mapping("double")), Strings.toString(mapperService.documentMapper()));
    }

    public void testIp() throws IOException {
        MapperService mapperService = createMapperService(mapping("ip"));
        FieldMapper mapper = (FieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(mapper, instanceOf(RuntimeScriptFieldMapper.class));
        assertEquals(Strings.toString(mapping("ip")), Strings.toString(mapperService.documentMapper()));
    }

    public void testKeyword() throws IOException {
        MapperService mapperService = createMapperService(mapping("keyword"));
        FieldMapper mapper = (FieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(mapper, instanceOf(RuntimeScriptFieldMapper.class));
        assertEquals(Strings.toString(mapping("keyword")), Strings.toString(mapperService.documentMapper()));
    }

    public void testLong() throws IOException {
        MapperService mapperService = createMapperService(mapping("long"));
        FieldMapper mapper = (FieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(mapper, instanceOf(RuntimeScriptFieldMapper.class));
        assertEquals(Strings.toString(mapping("long")), Strings.toString(mapperService.documentMapper()));
    }

    public void testDate() throws IOException {
        MapperService mapperService = createMapperService(mapping("date"));
        FieldMapper mapper = (FieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(mapper, instanceOf(RuntimeScriptFieldMapper.class));
        assertEquals(Strings.toString(mapping("date")), Strings.toString(mapperService.documentMapper()));
    }

    public void testDateWithFormat() throws IOException {
        CheckedSupplier<XContentBuilder, IOException> mapping = () -> mapping("date", b -> b.field("format", "yyyy-MM-dd"));
        MapperService mapperService = createMapperService(mapping.get());
        FieldMapper mapper = (FieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(mapper, instanceOf(RuntimeScriptFieldMapper.class));
        assertEquals(Strings.toString(mapping.get()), Strings.toString(mapperService.documentMapper()));
    }

    public void testDateWithLocale() throws IOException {
        CheckedSupplier<XContentBuilder, IOException> mapping = () -> mapping("date", b -> b.field("locale", "en_GB"));
        MapperService mapperService = createMapperService(mapping.get());
        FieldMapper mapper = (FieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(mapper, instanceOf(RuntimeScriptFieldMapper.class));
        assertEquals(Strings.toString(mapping.get()), Strings.toString(mapperService.documentMapper()));
    }

    public void testDateWithLocaleAndFormat() throws IOException {
        CheckedSupplier<XContentBuilder, IOException> mapping = () -> mapping(
            "date",
            b -> b.field("format", "yyyy-MM-dd").field("locale", "en_GB")
        );
        MapperService mapperService = createMapperService(mapping.get());
        FieldMapper mapper = (FieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(mapper, instanceOf(RuntimeScriptFieldMapper.class));
        assertEquals(Strings.toString(mapping.get()), Strings.toString(mapperService.documentMapper()));
    }

    public void testNonDateWithFormat() throws IOException {
        String runtimeType = randomValueOtherThan("date", () -> randomFrom(runtimeTypes));
        Exception e = expectThrows(
            MapperParsingException.class,
            () -> createMapperService(mapping(runtimeType, b -> b.field("format", "yyyy-MM-dd")))
        );
        assertThat(e.getMessage(), equalTo("Failed to parse mapping: format can not be specified for runtime_type [" + runtimeType + "]"));
    }

    public void testNonDateWithLocale() throws IOException {
        String runtimeType = randomValueOtherThan("date", () -> randomFrom(runtimeTypes));
        Exception e = expectThrows(
            MapperParsingException.class,
            () -> createMapperService(mapping(runtimeType, b -> b.field("locale", "en_GB")))
        );
        assertThat(e.getMessage(), equalTo("Failed to parse mapping: locale can not be specified for runtime_type [" + runtimeType + "]"));
    }

    public void testFieldCaps() throws Exception {
        for (String runtimeType : runtimeTypes) {
            MapperService scriptIndexMapping = createMapperService(mapping(runtimeType));
            MapperService concreteIndexMapping;
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
                concreteIndexMapping = createMapperService(mapping);
            }
            MappedFieldType scriptFieldType = scriptIndexMapping.fieldType("field");
            MappedFieldType concreteIndexType = concreteIndexMapping.fieldType("field");
            assertEquals(concreteIndexType.familyTypeName(), scriptFieldType.familyTypeName());
            assertEquals(concreteIndexType.isSearchable(), scriptFieldType.isSearchable());
            assertEquals(concreteIndexType.isAggregatable(), scriptFieldType.isAggregatable());
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

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new RuntimeFields(), new TestScriptPlugin());
    }

    private class TestScriptPlugin extends Plugin implements ScriptPlugin {
        @Override
        public ScriptEngine getScriptEngine(Settings settings, Collection<ScriptContext<?>> contexts) {
            return new TestScriptEngine() {
                @Override
                protected Object buildScriptFactory(ScriptContext<?> context) {
                    if (context == BooleanScriptFieldScript.CONTEXT) {
                        return BooleanScriptFieldScriptTests.DUMMY;
                    }
                    if (context == DateScriptFieldScript.CONTEXT) {
                        return DateScriptFieldScriptTests.DUMMY;
                    }
                    if (context == DoubleScriptFieldScript.CONTEXT) {
                        return DoubleScriptFieldScriptTests.DUMMY;
                    }
                    if (context == IpScriptFieldScript.CONTEXT) {
                        return IpScriptFieldScriptTests.DUMMY;
                    }
                    if (context == LongScriptFieldScript.CONTEXT) {
                        return LongScriptFieldScriptTests.DUMMY;
                    }
                    if (context == StringScriptFieldScript.CONTEXT) {
                        return StringScriptFieldScriptTests.DUMMY;
                    }
                    throw new IllegalArgumentException("Unsupported context: " + context);
                };

                public Set<ScriptContext<?>> getSupportedContexts() {
                    return Set.of(
                        BooleanScriptFieldScript.CONTEXT,
                        DateScriptFieldScript.CONTEXT,
                        DoubleScriptFieldScript.CONTEXT,
                        IpScriptFieldScript.CONTEXT,
                        StringScriptFieldScript.CONTEXT,
                        LongScriptFieldScript.CONTEXT
                    );
                }
            };
        }
    }
}

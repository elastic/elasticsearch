/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.mapper;

import org.apache.lucene.search.Query;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexSortConfig;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperTestCase;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.xpack.runtimefields.RuntimeFields;
import org.elasticsearch.xpack.runtimefields.query.StringScriptFieldExistsQuery;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;

public class RuntimeFieldMapperTests extends MapperTestCase {

    private final String[] runtimeTypes;

    public RuntimeFieldMapperTests() {
        this.runtimeTypes = RuntimeFieldMapper.Builder.FIELD_TYPE_RESOLVER.keySet().toArray(new String[0]);
        Arrays.sort(runtimeTypes);
    }

    @Override
    protected void writeField(XContentBuilder builder) {
        // do nothing
    }

    @Override
    protected void writeFieldValue(XContentBuilder builder) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void assertExistsQuery(MappedFieldType fieldType, Query query, ParseContext.Document fields) {
        assertThat(query, instanceOf(StringScriptFieldExistsQuery.class));
        assertNoFieldNamesField(fields);
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "runtime").field("runtime_type", "keyword");
        b.startObject("script").field("source", "dummy_source").field("lang", "test").endObject();
    }

    @Override
    protected void registerParameters(ParameterChecker checker) {
        // TODO need to be able to pass a completely new config rather than updating minimal mapping
    }

    public void testRuntimeTypeIsRequired() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("my_field")
            .field("type", "runtime")
            .field("script", "keyword('test')")
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        MapperParsingException exception = expectThrows(MapperParsingException.class, () -> createMapperService(mapping));
        assertEquals("Failed to parse mapping: runtime_type must be specified for runtime field [my_field]", exception.getMessage());
    }

    public void testScriptIsRequired() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("my_field")
            .field("type", "runtime")
            .field("runtime_type", randomFrom(runtimeTypes))
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        MapperParsingException exception = expectThrows(MapperParsingException.class, () -> createMapperService(mapping));
        assertEquals("Failed to parse mapping: script must be specified for runtime field [my_field]", exception.getMessage());
    }

    public void testCopyToIsNotSupported() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("my_field")
            .field("type", "runtime")
            .field("runtime_type", randomFrom(runtimeTypes))
            .field("script", "keyword('test')")
            .field("copy_to", "field")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        MapperParsingException exception = expectThrows(MapperParsingException.class, () -> createMapperService(mapping));
        assertEquals("Failed to parse mapping: runtime field [my_field] does not support [copy_to]", exception.getMessage());
    }

    public void testMultiFieldsIsNotSupported() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("my_field")
            .field("type", "runtime")
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
        assertEquals("Failed to parse mapping: runtime field [my_field] does not support [fields]", exception.getMessage());
    }

    public void testStoredScriptsAreNotSupported() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("my_field")
            .field("type", "runtime")
            .field("runtime_type", randomFrom(runtimeTypes))
            .startObject("script")
            .field("id", "test")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        MapperParsingException exception = expectThrows(MapperParsingException.class, () -> createMapperService(mapping));
        assertEquals("Failed to parse mapping: stored scripts are not supported for runtime field [my_field]", exception.getMessage());
    }

    public void testUnsupportedRuntimeType() {
        MapperParsingException exc = expectThrows(MapperParsingException.class, () -> createMapperService(mapping("unsupported")));
        assertEquals("Failed to parse mapping: runtime_type [unsupported] not supported for runtime field [field]", exc.getMessage());
    }

    public void testBoolean() throws IOException {
        MapperService mapperService = createMapperService(mapping("boolean"));
        FieldMapper mapper = (FieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(mapper, instanceOf(RuntimeFieldMapper.class));
        assertEquals(Strings.toString(mapping("boolean")), Strings.toString(mapperService.documentMapper()));
    }

    public void testDouble() throws IOException {
        MapperService mapperService = createMapperService(mapping("double"));
        FieldMapper mapper = (FieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(mapper, instanceOf(RuntimeFieldMapper.class));
        assertEquals(Strings.toString(mapping("double")), Strings.toString(mapperService.documentMapper()));
    }

    public void testIp() throws IOException {
        MapperService mapperService = createMapperService(mapping("ip"));
        FieldMapper mapper = (FieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(mapper, instanceOf(RuntimeFieldMapper.class));
        assertEquals(Strings.toString(mapping("ip")), Strings.toString(mapperService.documentMapper()));
    }

    public void testKeyword() throws IOException {
        MapperService mapperService = createMapperService(mapping("keyword"));
        FieldMapper mapper = (FieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(mapper, instanceOf(RuntimeFieldMapper.class));
        assertEquals(Strings.toString(mapping("keyword")), Strings.toString(mapperService.documentMapper()));
    }

    public void testLong() throws IOException {
        MapperService mapperService = createMapperService(mapping("long"));
        FieldMapper mapper = (FieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(mapper, instanceOf(RuntimeFieldMapper.class));
        assertEquals(Strings.toString(mapping("long")), Strings.toString(mapperService.documentMapper()));
    }

    public void testDate() throws IOException {
        MapperService mapperService = createMapperService(mapping("date"));
        FieldMapper mapper = (FieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(mapper, instanceOf(RuntimeFieldMapper.class));
        assertEquals(Strings.toString(mapping("date")), Strings.toString(mapperService.documentMapper()));
    }

    public void testDateWithFormat() throws IOException {
        CheckedSupplier<XContentBuilder, IOException> mapping = () -> mapping("date", b -> b.field("format", "yyyy-MM-dd"));
        MapperService mapperService = createMapperService(mapping.get());
        FieldMapper mapper = (FieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(mapper, instanceOf(RuntimeFieldMapper.class));
        assertEquals(Strings.toString(mapping.get()), Strings.toString(mapperService.documentMapper()));
    }

    public void testDateWithLocale() throws IOException {
        CheckedSupplier<XContentBuilder, IOException> mapping = () -> mapping("date", b -> b.field("locale", "en_GB"));
        MapperService mapperService = createMapperService(mapping.get());
        FieldMapper mapper = (FieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(mapper, instanceOf(RuntimeFieldMapper.class));
        assertEquals(Strings.toString(mapping.get()), Strings.toString(mapperService.documentMapper()));
    }

    public void testDateWithLocaleAndFormat() throws IOException {
        CheckedSupplier<XContentBuilder, IOException> mapping = () -> mapping(
            "date",
            b -> b.field("format", "yyyy-MM-dd").field("locale", "en_GB")
        );
        MapperService mapperService = createMapperService(mapping.get());
        FieldMapper mapper = (FieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(mapper, instanceOf(RuntimeFieldMapper.class));
        assertEquals(Strings.toString(mapping.get()), Strings.toString(mapperService.documentMapper()));
    }

    public void testNonDateWithFormat() {
        String runtimeType = randomValueOtherThan("date", () -> randomFrom(runtimeTypes));
        Exception e = expectThrows(
            MapperParsingException.class,
            () -> createMapperService(mapping(runtimeType, b -> b.field("format", "yyyy-MM-dd")))
        );
        assertThat(
            e.getMessage(),
            equalTo(
                "Failed to parse mapping: format can not be specified for [runtime] field [field] "
                    + "of runtime_type ["
                    + runtimeType
                    + "]"
            )
        );
    }

    public void testNonDateWithLocale() {
        String runtimeType = randomValueOtherThan("date", () -> randomFrom(runtimeTypes));
        Exception e = expectThrows(
            MapperParsingException.class,
            () -> createMapperService(mapping(runtimeType, b -> b.field("locale", "en_GB")))
        );
        assertThat(
            e.getMessage(),
            equalTo(
                "Failed to parse mapping: locale can not be specified for [runtime] field [field] of "
                    + "runtime_type ["
                    + runtimeType
                    + "]"
            )
        );
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

    public void testIndexSorting() {
        Settings build = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put("index.sort.field", "runtime")
            .build();
        IndexSettings indexSettings = new IndexSettings(IndexMetadata.builder("index").settings(build).build(), Settings.EMPTY);
        IndicesFieldDataCache cache = new IndicesFieldDataCache(Settings.EMPTY, null);
        NoneCircuitBreakerService circuitBreakerService = new NoneCircuitBreakerService();
        final IndexFieldDataService indexFieldDataService = new IndexFieldDataService(indexSettings, cache, circuitBreakerService, null);
        IndexSortConfig config = indexSettings.getIndexSortConfig();
        assertTrue(config.hasIndexSort());
        IllegalArgumentException iae = expectThrows(
            IllegalArgumentException.class,
            () -> config.buildIndexSort(
                field -> new KeywordScriptFieldType(field, new Script(""), mock(StringFieldScript.Factory.class), Collections.emptyMap()),
                (fieldType, searchLookupSupplier) -> indexFieldDataService.getForField(fieldType, "index", searchLookupSupplier)
            )
        );
        assertEquals("docvalues not found for index sort field:[runtime]", iae.getMessage());
        assertThat(iae.getCause(), instanceOf(UnsupportedOperationException.class));
        assertEquals("index sorting not supported on runtime field [runtime]", iae.getCause().getMessage());
    }

    private static XContentBuilder mapping(String type) throws IOException {
        return mapping(type, builder -> {});
    }

    private static XContentBuilder mapping(String type, CheckedConsumer<XContentBuilder, IOException> extra) throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject();
        {
            mapping.startObject("_doc");
            {
                mapping.startObject("properties");
                {
                    mapping.startObject("field");
                    {
                        mapping.field("type", "runtime").field("runtime_type", type);
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

    private static class TestScriptPlugin extends Plugin implements ScriptPlugin {
        @Override
        public ScriptEngine getScriptEngine(Settings settings, Collection<ScriptContext<?>> contexts) {
            return new TestScriptEngine() {
                @Override
                protected Object buildScriptFactory(ScriptContext<?> context) {
                    if (context == BooleanFieldScript.CONTEXT) {
                        return BooleanFieldScriptTests.DUMMY;
                    }
                    if (context == DateFieldScript.CONTEXT) {
                        return DateFieldScriptTests.DUMMY;
                    }
                    if (context == DoubleFieldScript.CONTEXT) {
                        return DoubleFieldScriptTests.DUMMY;
                    }
                    if (context == IpFieldScript.CONTEXT) {
                        return IpFieldScriptTests.DUMMY;
                    }
                    if (context == LongFieldScript.CONTEXT) {
                        return LongFieldScriptTests.DUMMY;
                    }
                    if (context == StringFieldScript.CONTEXT) {
                        return StringFieldScriptTests.DUMMY;
                    }
                    throw new IllegalArgumentException("Unsupported context: " + context);
                };

                public Set<ScriptContext<?>> getSupportedContexts() {
                    return Set.copyOf(new RuntimeFields().getContexts());
                }
            };
        }
    }
}

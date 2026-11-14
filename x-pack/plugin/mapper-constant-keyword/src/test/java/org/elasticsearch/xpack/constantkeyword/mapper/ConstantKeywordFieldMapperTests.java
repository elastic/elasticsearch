/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.constantkeyword.mapper;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.IndexSortConfig;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentParsingException;
import org.elasticsearch.index.mapper.DummyBlockLoaderContext;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.index.mapper.MapperTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.TestBlock;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.constantkeyword.ConstantKeywordMapperPlugin;
import org.elasticsearch.xpack.constantkeyword.mapper.ConstantKeywordFieldMapper.ConstantKeywordFieldType;
import org.junit.AssumptionViolatedException;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.index.mapper.MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

public class ConstantKeywordFieldMapperTests extends MapperTestCase {

    @Override
    protected void writeField(XContentBuilder builder) {
        // do nothing
    }

    @Override
    protected Object getSampleValueForDocument() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected Object getSampleValueForQuery() {
        return "test";
    }

    @Override
    protected void assertExistsQuery(MappedFieldType fieldType, Query query, LuceneDocument fields) {
        assertThat(query, instanceOf(MatchNoDocsQuery.class));
        assertNoFieldNamesField(fields);
    }

    @Override
    protected Collection<Plugin> getPlugins() {
        return List.of(new ConstantKeywordMapperPlugin());
    }

    @Override
    protected boolean supportsStoredFields() {
        return false;
    }

    @Override
    protected boolean supportsIgnoreMalformed() {
        return false;
    }

    public void testDefaults() throws Exception {
        XContentBuilder mapping = fieldMapping(b -> b.field("type", "constant_keyword").field("value", "foo"));
        DocumentMapper mapper = createDocumentMapper(mapping);
        assertEquals(Strings.toString(mapping), mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse(source(b -> {}));
        assertNull(doc.rootDoc().getField("field"));

        doc = mapper.parse(source(b -> b.field("field", "foo")));
        assertNull(doc.rootDoc().getField("field"));

        DocumentParsingException e = expectThrows(DocumentParsingException.class, () -> mapper.parse(source(b -> b.field("field", "bar"))));
        assertEquals(
            "[constant_keyword] field [field] only accepts values that are equal to the value defined in the mappings, but got [bar]",
            e.getCause().getMessage()
        );
    }

    public void testDynamicValue() throws Exception {
        MapperService mapperService = createMapperService(fieldMapping(b -> b.field("type", "constant_keyword")));

        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.field("field", "foo")));
        assertNull(doc.rootDoc().getField("field"));
        assertNotNull(doc.dynamicMappingsUpdate());

        DocumentMapper updatedMapper = mapperService.merge("_doc", doc.dynamicMappingsUpdate(), MergeReason.MAPPING_AUTO_UPDATE);
        String expectedMapping = Strings.toString(fieldMapping(b -> b.field("type", "constant_keyword").field("value", "foo")));
        assertEquals(expectedMapping, updatedMapper.mappingSource().toString());

        doc = updatedMapper.parse(source(b -> b.field("field", "foo")));
        assertNull(doc.rootDoc().getField("field"));
        assertNull(doc.dynamicMappingsUpdate());
    }

    /**
     * The first document fixes the value with a dynamic update, which must leave the parameters the mapping already carries alone.
     */
    public void testDynamicValueKeepsMeta() throws Exception {
        MapperService mapperService = createMapperService(
            fieldMapping(b -> b.field("type", "constant_keyword").startObject("meta").field("unit", "bytes").endObject())
        );

        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.field("field", "foo")));
        assertNotNull(doc.dynamicMappingsUpdate());

        DocumentMapper updatedMapper = mapperService.merge("_doc", doc.dynamicMappingsUpdate(), MergeReason.MAPPING_AUTO_UPDATE);
        String expectedMapping = Strings.toString(
            fieldMapping(
                b -> b.field("type", "constant_keyword").field("value", "foo").startObject("meta").field("unit", "bytes").endObject()
            )
        );
        assertEquals(expectedMapping, updatedMapper.mappingSource().toString());
    }

    /**
     * A field that a dynamic template creates has no value, and the document that creates it fixes it, which is before the field is
     * part of the stored mapping.
     */
    public void testDynamicValueOfAFieldFromADynamicTemplate() throws Exception {
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.startArray("dynamic_templates");
            b.startObject().startObject("constants");
            b.field("match", "const*");
            b.startObject("mapping").field("type", "constant_keyword").endObject();
            b.endObject().endObject();
            b.endArray();
        }));

        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.field("const", "foo")));
        assertNotNull(doc.dynamicMappingsUpdate());

        DocumentMapper updatedMapper = mapperService.merge("_doc", doc.dynamicMappingsUpdate(), MergeReason.MAPPING_AUTO_UPDATE);
        assertThat(updatedMapper.mappingSource().string(), containsString("\"value\":\"foo\""));
    }

    /**
     * Each value in a document tells the field its value, and they must agree before any of them is fixed in the mapping.
     */
    public void testDifferentValuesInNestedObjectsAreRejected() throws Exception {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("n").field("type", "nested").startObject("properties");
            b.startObject("field").field("type", "constant_keyword").endObject();
            b.endObject().endObject();
        }));

        DocumentParsingException e = expectThrows(
            DocumentParsingException.class,
            () -> mapperService.documentMapper()
                .parse(
                    source(
                        b -> b.startArray("n")
                            .startObject()
                            .field("field", "a")
                            .endObject()
                            .startObject()
                            .field("field", "b")
                            .endObject()
                            .endArray()
                    )
                )
        );
        assertThat(e.getCause().getMessage(), containsString("only accepts values that are equal to the value defined in the mappings"));
    }

    public void testDynamicValueFieldLimit() throws Exception {
        MapperService mapperService = createMapperService(
            Settings.builder().put(INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey(), 1).build(),
            fieldMapping(b -> b.field("type", "constant_keyword"))
        );

        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.field("field", "foo")));
        assertNull(doc.rootDoc().getField("field"));
        assertNotNull(doc.dynamicMappingsUpdate());

        DocumentMapper updatedMapper = mapperService.merge("_doc", doc.dynamicMappingsUpdate(), MergeReason.MAPPING_AUTO_UPDATE);
        String expectedMapping = Strings.toString(fieldMapping(b -> b.field("type", "constant_keyword").field("value", "foo")));
        assertEquals(expectedMapping, updatedMapper.mappingSource().toString());

        doc = updatedMapper.parse(source(b -> b.field("field", "foo")));
        assertNull(doc.rootDoc().getField("field"));
        assertNull(doc.dynamicMappingsUpdate());
    }

    public void testBadValues() {
        {
            MapperParsingException e = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
                b.field("type", "constant_keyword");
                b.nullField("value");
            })));
            assertEquals(
                e.getMessage(),
                "Failed to parse mapping: [value] on mapper [field] of type [constant_keyword] must not have a [null] value"
            );
        }
        {
            MapperParsingException e = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
                b.field("type", "constant_keyword");
                b.startObject("value").field("foo", "bar").endObject();
            })));
            assertEquals(
                e.getMessage(),
                "Failed to parse mapping: Property [value] on field [field] must be a number or a string, but got [{foo=bar}]"
            );
        }
    }

    public void testNumericValue() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            b.field("type", "constant_keyword");
            b.field("value", 74);
        }));
        ConstantKeywordFieldType ft = (ConstantKeywordFieldType) mapperService.fieldType("field");
        assertEquals("74", ft.value());
    }

    public void testIndexSortRejected() throws IOException {
        Settings settings = Settings.builder().put("index.sort.field", "field").build();
        MapperService mapperService = createMapperService(settings, fieldMapping(b -> {
            b.field("type", "constant_keyword");
            if (randomBoolean()) {
                b.field("value", "foo");
            }
        }));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new IndexSortConfig(mapperService.getIndexSettings()).buildIndexSort(
                mapperService::fieldType,
                (ft, s) -> ft.fielddataBuilder(FieldDataContext.noRuntimeFields("index", "test")).build(null, null)
            )
        );
        assertThat(e.getMessage(), equalTo("invalid index sort field:[field]"));
    }

    public void testUpdate() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            b.field("type", "constant_keyword");
            b.field("value", "foo");
        }));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> merge(mapperService, fieldMapping(b -> {
            b.field("type", "constant_keyword");
            b.field("value", "bar");
        })));
        assertEquals(
            e.getMessage(),
            "Mapper for [field] conflicts with existing mapper:\n" + "\tCannot update parameter [value] from [foo] to [bar]"
        );
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "constant_keyword");
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerUpdateCheck("value", b -> b.field("value", "foo"), m -> {
            ConstantKeywordFieldType ft = (ConstantKeywordFieldType) m.fieldType();
            assertEquals("foo", ft.value());
        });
        checker.registerConflictCheck("value", fieldMapping(b -> {
            b.field("type", "constant_keyword");
            b.field("value", "foo");
        }), fieldMapping(b -> {
            b.field("type", "constant_keyword");
            b.field("value", "bar");
        }));
    }

    @Override
    protected String generateRandomInputValue(MappedFieldType ft) {
        return ((ConstantKeywordFieldType) ft).value();
    }

    @Override
    protected void randomFetchTestFieldConfig(XContentBuilder b) throws IOException {
        b.field("type", "constant_keyword").field("value", randomAlphaOfLengthBetween(1, 10));
    }

    @Override
    protected boolean allowsNullValues() {
        return false;   // null is an error for constant keyword
    }

    /**
     * Test loading blocks when there is no defined value. This is allowed
     * for newly created indices that haven't received any documents that
     * contain the field.
     */
    public void testNullValueBlockLoader() throws IOException {
        MapperService mapper = createSytheticSourceMapperService(mapping(b -> {
            b.startObject("field");
            b.field("type", "constant_keyword");
            b.endObject();
        }));
        BlockLoader loader = mapper.fieldType("field").blockLoader(new DummyBlockLoaderContext.MapperServiceBlockLoaderContext(mapper));
        try (Directory directory = newDirectory()) {
            CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(1));
            RandomIndexWriter iw = new RandomIndexWriter(random(), directory);
            LuceneDocument doc = mapper.documentMapper().parse(source(b -> {})).rootDoc();
            iw.addDocument(doc);
            iw.close();
            try (
                DirectoryReader reader = DirectoryReader.open(directory);
                BlockLoader.ColumnAtATimeReader columnReader = loader.columnAtATimeReader(reader.leaves().get(0)).apply(breaker)
            ) {
                TestBlock block = (TestBlock) columnReader.read(TestBlock.factory(), TestBlock.docs(0), 0, false, null);
                assertThat(block.get(0), nullValue());
            }
        }
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport(boolean ignoreMalformed) {
        assertFalse("constant_keyword doesn't support ignore_malformed", ignoreMalformed);
        String value = randomUnicodeOfLength(5);
        return new SyntheticSourceSupport() {
            @Override
            public SyntheticSourceExample example(int maxValues) {
                return new SyntheticSourceExample(value, value, b -> {
                    b.field("type", "constant_keyword");
                    b.field("value", value);
                });
            }

            @Override
            public List<SyntheticSourceInvalidExample> invalidExample() throws IOException {
                throw new AssumptionViolatedException("copy_to on constant_keyword not supported");
            }
        };
    }

    @Override
    protected IngestScriptSupport ingestScriptSupport() {
        throw new AssumptionViolatedException("not supported");
    }

    public void testNullValueSyntheticSource() throws IOException {
        DocumentMapper mapper = createSytheticSourceMapperService(mapping(b -> {
            b.startObject("field");
            b.field("type", "constant_keyword");
            b.endObject();
        })).documentMapper();
        assertThat(syntheticSource(mapper, b -> {}), equalTo("{}"));
    }

    public void testNoValueInDocumentSyntheticSource() throws IOException {
        DocumentMapper mapper = createSytheticSourceMapperService(mapping(b -> {
            b.startObject("field");
            b.field("type", "constant_keyword");
            b.field("value", randomAlphaOfLength(5));
            b.endObject();
        })).documentMapper();

        assertThat(syntheticSource(mapper, b -> {}), equalTo("{}"));
    }

    @Override
    protected boolean supportsEmptyInputArray() {
        return false;
    }

    @Override
    protected boolean addsValueWhenNotSupplied() {
        return true;
    }

    @Override
    protected List<SortShortcutSupport> getSortShortcutSupport() {
        return List.of(
            // TODO this should surely be able to support pruning
            new SortShortcutSupport(this::minimalMapping, this::writeField, false)
        );
    }

    @Override
    protected boolean supportsDocValuesSkippers() {
        return false;
    }

    @Override
    protected boolean supportsDynamicallyAddedMultiFields() {
        // a constant_keyword field has a single fixed value, so a multi-field has no effect, and adding one is only warned about
        return false;
    }
}

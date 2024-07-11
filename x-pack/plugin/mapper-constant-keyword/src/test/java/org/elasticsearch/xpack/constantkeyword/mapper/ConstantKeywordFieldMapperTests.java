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
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentParsingException;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.index.mapper.MapperTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.TestBlock;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.constantkeyword.ConstantKeywordMapperPlugin;
import org.elasticsearch.xpack.constantkeyword.mapper.ConstantKeywordFieldMapper.ConstantKeywordFieldType;
import org.junit.AssumptionViolatedException;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static org.elasticsearch.index.mapper.MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING;
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
            "[constant_keyword] field [field] only accepts values that are equal to the value defined in the mappings [foo], "
                + "but got [bar]",
            e.getCause().getMessage()
        );
    }

    public void testDynamicValue() throws Exception {
        MapperService mapperService = createMapperService(fieldMapping(b -> b.field("type", "constant_keyword")));

        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.field("field", "foo")));
        assertNull(doc.rootDoc().getField("field"));
        assertNotNull(doc.dynamicMappingsUpdate());

        CompressedXContent mappingUpdate = new CompressedXContent(Strings.toString(doc.dynamicMappingsUpdate()));
        DocumentMapper updatedMapper = mapperService.merge("_doc", mappingUpdate, MergeReason.MAPPING_UPDATE);
        String expectedMapping = Strings.toString(fieldMapping(b -> b.field("type", "constant_keyword").field("value", "foo")));
        assertEquals(expectedMapping, updatedMapper.mappingSource().toString());

        doc = updatedMapper.parse(source(b -> b.field("field", "foo")));
        assertNull(doc.rootDoc().getField("field"));
        assertNull(doc.dynamicMappingsUpdate());
    }

    public void testDynamicValueFieldLimit() throws Exception {
        MapperService mapperService = createMapperService(
            Settings.builder().put(INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey(), 1).build(),
            fieldMapping(b -> b.field("type", "constant_keyword"))
        );

        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.field("field", "foo")));
        assertNull(doc.rootDoc().getField("field"));
        assertNotNull(doc.dynamicMappingsUpdate());

        CompressedXContent mappingUpdate = new CompressedXContent(Strings.toString(doc.dynamicMappingsUpdate()));
        DocumentMapper updatedMapper = mapperService.merge("_doc", mappingUpdate, MergeReason.MAPPING_UPDATE);
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
        checker.registerUpdateCheck(b -> b.field("value", "foo"), m -> {
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
        MapperService mapper = createMapperService(syntheticSourceMapping(b -> {
            b.startObject("field");
            b.field("type", "constant_keyword");
            b.endObject();
        }));
        BlockLoader loader = mapper.fieldType("field").blockLoader(new MappedFieldType.BlockLoaderContext() {
            @Override
            public String indexName() {
                throw new UnsupportedOperationException();
            }

            @Override
            public IndexSettings indexSettings() {
                throw new UnsupportedOperationException();
            }

            @Override
            public MappedFieldType.FieldExtractPreference fieldExtractPreference() {
                return MappedFieldType.FieldExtractPreference.NONE;
            }

            @Override
            public SearchLookup lookup() {
                throw new UnsupportedOperationException();
            }

            @Override
            public Set<String> sourcePaths(String name) {
                return mapper.mappingLookup().sourcePaths(name);
            }

            @Override
            public String parentField(String field) {
                throw new UnsupportedOperationException();
            }

            @Override
            public FieldNamesFieldMapper.FieldNamesFieldType fieldNames() {
                return FieldNamesFieldMapper.FieldNamesFieldType.get(true);
            }
        });
        try (Directory directory = newDirectory()) {
            RandomIndexWriter iw = new RandomIndexWriter(random(), directory);
            LuceneDocument doc = mapper.documentMapper().parse(source(b -> {})).rootDoc();
            iw.addDocument(doc);
            iw.close();
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                TestBlock block = (TestBlock) loader.columnAtATimeReader(reader.leaves().get(0))
                    .read(TestBlock.factory(reader.numDocs()), new BlockLoader.Docs() {
                        @Override
                        public int count() {
                            return 1;
                        }

                        @Override
                        public int get(int i) {
                            return 0;
                        }
                    });
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

    @Override
    protected Function<Object, Object> loadBlockExpected() {
        return v -> ((BytesRef) v).utf8ToString();
    }

    public void testNullValueSyntheticSource() throws IOException {
        DocumentMapper mapper = createDocumentMapper(syntheticSourceMapping(b -> {
            b.startObject("field");
            b.field("type", "constant_keyword");
            b.endObject();
        }));
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
}

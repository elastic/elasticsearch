/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentParsingException;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.search.lookup.SourceProvider;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.InferencePlugin;
import org.junit.AssumptionViolatedException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OffsetSourceFieldMapperTests extends MapperTestCase {
    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new InferencePlugin(Settings.EMPTY));
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "offset_source");
    }

    @Override
    protected Object getSampleValueForDocument() {
        return getSampleObjectForDocument();
    }

    @Override
    protected Object getSampleObjectForDocument() {
        return Map.of("field", "foo", "start", 100, "end", 300);
    }

    @Override
    protected Object generateRandomInputValue(MappedFieldType ft) {
        return new OffsetSourceFieldMapper.OffsetSource("field", randomIntBetween(0, 100), randomIntBetween(101, 1000));
    }

    @Override
    protected IngestScriptSupport ingestScriptSupport() {
        throw new AssumptionViolatedException("not supported");
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerIgnoredParameter("charset");
    }

    @Override
    protected void assertSearchable(MappedFieldType fieldType) {
        assertFalse(fieldType.isSearchable());
    }

    @Override
    protected boolean supportsStoredFields() {
        return false;
    }

    @Override
    protected boolean supportsEmptyInputArray() {
        return false;
    }

    @Override
    protected boolean supportsCopyTo() {
        return false;
    }

    @Override
    protected boolean supportsIgnoreMalformed() {
        return false;
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport(boolean ignoreMalformed) {
        return new SyntheticSourceSupport() {
            @Override
            public SyntheticSourceExample example(int maxValues) {
                return new SyntheticSourceExample(getSampleValueForDocument(), getSampleValueForDocument(), b -> minimalMapping(b));
            }

            @Override
            public List<SyntheticSourceInvalidExample> invalidExample() {
                return List.of();
            }
        };
    }

    @Override
    public void testSyntheticSourceKeepArrays() {
        // This mapper doesn't support multiple values (array of objects).
    }

    public void testDefaults() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        assertEquals(Strings.toString(fieldMapping(this::minimalMapping)), mapper.mappingSource().toString());

        ParsedDocument doc1 = mapper.parse(
            source(b -> b.startObject("field").field("field", "foo").field("start", 0).field("end", 128).endObject())
        );
        List<IndexableField> fields = doc1.rootDoc().getFields("field");
        assertEquals(1, fields.size());
        assertThat(fields.get(0), instanceOf(OffsetSourceField.class));
        OffsetSourceField offsetField1 = (OffsetSourceField) fields.get(0);

        ParsedDocument doc2 = mapper.parse(
            source(b -> b.startObject("field").field("field", "bar").field("start", 128).field("end", 512).endObject())
        );
        OffsetSourceField offsetField2 = (OffsetSourceField) doc2.rootDoc().getFields("field").get(0);

        assertTokenStream(offsetField1.tokenStream(null, null), "foo", 0, 128);
        assertTokenStream(offsetField2.tokenStream(null, null), "bar", 128, 512);
    }

    @Override
    protected void assertFetch(MapperService mapperService, String field, Object value, String format) throws IOException {
        MappedFieldType ft = mapperService.fieldType(field);
        MappedFieldType.FielddataOperation fdt = MappedFieldType.FielddataOperation.SEARCH;
        SourceToParse source = source(b -> b.field(ft.name(), value));
        SearchExecutionContext searchExecutionContext = mock(SearchExecutionContext.class);
        when(searchExecutionContext.getIndexSettings()).thenReturn(mapperService.getIndexSettings());
        when(searchExecutionContext.indexVersionCreated()).thenReturn(mapperService.getIndexSettings().getIndexVersionCreated());
        when(searchExecutionContext.isSourceEnabled()).thenReturn(true);
        when(searchExecutionContext.sourcePath(field)).thenReturn(Set.of(field));
        when(searchExecutionContext.getForField(ft, fdt)).thenAnswer(inv -> fieldDataLookup(mapperService).apply(ft, () -> {
            throw new UnsupportedOperationException();
        }, fdt));
        ValueFetcher nativeFetcher = ft.valueFetcher(searchExecutionContext, format);
        ParsedDocument doc = mapperService.documentMapper().parse(source);
        withLuceneIndex(mapperService, iw -> iw.addDocuments(doc.docs()), ir -> {
            Source s = SourceProvider.fromLookup(mapperService.mappingLookup(), null, mapperService.getMapperMetrics().sourceFieldMetrics())
                .getSource(ir.leaves().get(0), 0);
            nativeFetcher.setNextReader(ir.leaves().get(0));
            List<Object> fromNative = nativeFetcher.fetchValues(s, 0, new ArrayList<>());
            assertThat(fromNative.size(), equalTo(1));
            assertThat("fetching " + value, fromNative.get(0), equalTo(value));
        });
    }

    @Override
    protected void assertFetchMany(MapperService mapperService, String field, Object value, String format, int count) throws IOException {
        assumeFalse("[offset_source] currently don't support multiple values in the same field", false);
    }

    public void testInvalidCharset() {
        var exc = expectThrows(Exception.class, () -> createDocumentMapper(mapping(b -> {
            b.startObject("field").field("type", "offset_source").field("charset", "utf_8").endObject();
        })));
        assertThat(exc.getCause().getMessage(), containsString("Unknown value [utf_8] for field [charset]"));
    }

    public void testRejectMultiValuedFields() throws IOException {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> { b.startObject("field").field("type", "offset_source").endObject(); }));

        DocumentParsingException exc = expectThrows(DocumentParsingException.class, () -> mapper.parse(source(b -> {
            b.startArray("field");
            {
                b.startObject().field("field", "bar1").field("start", 128).field("end", 512).endObject();
                b.startObject().field("field", "bar2").field("start", 128).field("end", 512).endObject();
            }
            b.endArray();
        })));
        assertThat(exc.getCause().getMessage(), containsString("[offset_source] fields do not support indexing multiple values"));
    }

    public void testInvalidOffsets() throws IOException {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> { b.startObject("field").field("type", "offset_source").endObject(); }));

        DocumentParsingException exc = expectThrows(DocumentParsingException.class, () -> mapper.parse(source(b -> {
            b.startArray("field");
            {
                b.startObject().field("field", "bar1").field("start", -1).field("end", 512).endObject();
            }
            b.endArray();
        })));
        assertThat(rootCause(exc).getMessage(), containsString("Illegal offsets"));
    }

    public void testInputIndex() throws Exception {
        assumeTrue("Semantic field feature flag is enabled", SemanticFieldMapper.SEMANTIC_FIELD_FEATURE_FLAG.isEnabled());

        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));

        ParsedDocument doc1 = mapper.parse(source(b -> b.startObject("field").field("field", "foo").field("input_index", 0).endObject()));
        List<IndexableField> fields = doc1.rootDoc().getFields("field");
        assertEquals(1, fields.size());
        assertThat(fields.get(0), instanceOf(OffsetSourceField.class));
        OffsetSourceField offsetField1 = (OffsetSourceField) fields.get(0);

        ParsedDocument doc2 = mapper.parse(source(b -> b.startObject("field").field("field", "bar").field("input_index", 42).endObject()));
        OffsetSourceField offsetField2 = (OffsetSourceField) doc2.rootDoc().getFields("field").get(0);

        assertTokenStream(offsetField1.tokenStream(null, null), "foo", 0);
        assertTokenStream(offsetField2.tokenStream(null, null), "bar", 42);
    }

    public void testInvalidInputIndex() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        DocumentParsingException exc = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(source(b -> b.startObject("field").field("field", "foo").field("input_index", -1).endObject()))
        );
        assertThat(rootCause(exc).getMessage(), containsString("Illegal input index"));
    }

    public void testInputIndexWithNoFeatureFlag() throws Exception {
        assumeFalse("Semantic field feature flag is not enabled", SemanticFieldMapper.SEMANTIC_FIELD_FEATURE_FLAG.isEnabled());

        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        DocumentParsingException exc = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(source(b -> b.startObject("field").field("field", "foo").field("input_index", 7).endObject()))
        );
        Throwable rootCause = rootCause(exc);
        assertThat(rootCause, instanceOf(UnsupportedOperationException.class));
        assertThat(rootCause.getMessage(), containsString("Input index is not supported yet"));
    }

    public void testRejectBothOffsetsAndInputIndex() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        DocumentParsingException exc = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(
                source(
                    b -> b.startObject("field").field("field", "foo").field("start", 0).field("end", 10).field("input_index", 1).endObject()
                )
            )
        );
        assertThat(rootCause(exc).getMessage(), containsString("must not specify both"));
    }

    public void testRejectMissingOffsetsAndInputIndex() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        DocumentParsingException exc = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(source(b -> b.startObject("field").field("field", "foo").endObject()))
        );
        assertThat(rootCause(exc).getMessage(), containsString("requires either"));
    }

    public void testOffsetSentinel() throws IOException {
        for (int i = 0; i < 20; i++) {
            // Pre-sentinel mapper: (0, 0) is read back as a legitimate zero-length offset span.
            assertOffsetSentinelFetch(
                IndexVersionUtils.randomPreviousCompatibleVersion(IndexVersions.SEMANTIC_FIELD_TYPE),
                new OffsetSourceFieldMapper.OffsetSource("foo", 0, 0)
            );

            if (SemanticFieldMapper.SEMANTIC_FIELD_FEATURE_FLAG.isEnabled()) {
                // Post-sentinel mapper: (0, 0) is read back as the inputIndex sentinel with inputIndex == 0
                // (the default PositionIncrementAttribute of 1 lands the token at absolute position 0).
                assertOffsetSentinelFetch(
                    IndexVersionUtils.randomVersionOnOrAfter(IndexVersions.SEMANTIC_FIELD_TYPE),
                    new OffsetSourceFieldMapper.OffsetSource("foo", 0)
                );
            }
        }
    }

    private void assertOffsetSentinelFetch(IndexVersion indexVersion, OffsetSourceFieldMapper.OffsetSource expected) throws IOException {
        MapperService mapperService = createMapperService(indexVersion, fieldMapping(this::minimalMapping));
        MappedFieldType ft = mapperService.fieldType("field");
        SourceToParse source = source(b -> b.startObject("field").field("field", "foo").field("start", 0).field("end", 0).endObject());

        SearchExecutionContext searchExecutionContext = mock(SearchExecutionContext.class);
        when(searchExecutionContext.getIndexSettings()).thenReturn(mapperService.getIndexSettings());
        when(searchExecutionContext.indexVersionCreated()).thenReturn(indexVersion);

        ValueFetcher fetcher = ft.valueFetcher(searchExecutionContext, null);
        ParsedDocument doc = mapperService.documentMapper().parse(source);
        withLuceneIndex(mapperService, iw -> iw.addDocuments(doc.docs()), ir -> {
            Source s = SourceProvider.fromLookup(mapperService.mappingLookup(), null, mapperService.getMapperMetrics().sourceFieldMetrics())
                .getSource(ir.leaves().get(0), 0);
            fetcher.setNextReader(ir.leaves().get(0));
            List<Object> fromNative = fetcher.fetchValues(s, 0, new ArrayList<>());
            assertThat(fromNative, hasSize(1));
            assertThat(fromNative.get(0), equalTo(expected));
        });
    }

    @Override
    protected List<SortShortcutSupport> getSortShortcutSupport() {
        return List.of();
    }

    @Override
    protected boolean supportsDocValuesSkippers() {
        return false;
    }

    private static Throwable rootCause(Throwable t) {
        Throwable cause = t;
        while (cause.getCause() != null && cause.getCause() != cause) {
            cause = cause.getCause();
        }
        return cause;
    }

    private static void assertTokenStream(TokenStream tk, String expectedTerm, int expectedStartOffset, int expectedEndOffset)
        throws IOException {
        CharTermAttribute termAttribute = tk.addAttribute(CharTermAttribute.class);
        OffsetAttribute offsetAttribute = tk.addAttribute(OffsetAttribute.class);

        tk.reset();
        assertTrue(tk.incrementToken());
        assertThat(new String(termAttribute.buffer(), 0, termAttribute.length()), equalTo(expectedTerm));
        assertThat(offsetAttribute.startOffset(), equalTo(expectedStartOffset));
        assertThat(offsetAttribute.endOffset(), equalTo(expectedEndOffset));
        assertFalse(tk.incrementToken());
    }

    private static void assertTokenStream(TokenStream tk, String expectedTerm, int expectedInputIndex) throws IOException {
        CharTermAttribute termAttribute = tk.addAttribute(CharTermAttribute.class);
        OffsetAttribute offsetAttribute = tk.addAttribute(OffsetAttribute.class);
        PositionIncrementAttribute posIncAttribute = tk.addAttribute(PositionIncrementAttribute.class);

        tk.reset();
        assertTrue(tk.incrementToken());
        assertThat(new String(termAttribute.buffer(), 0, termAttribute.length()), equalTo(expectedTerm));
        assertThat(offsetAttribute.startOffset(), equalTo(0));
        assertThat(offsetAttribute.endOffset(), equalTo(0));
        // PositionIncrementAttribute is cumulative from an initial position of -1, so for a
        // single-token stream the increment is inputIndex + 1 to land at absolute position inputIndex.
        assertThat(posIncAttribute.getPositionIncrement(), equalTo(expectedInputIndex + 1));
        assertFalse(tk.incrementToken());
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.mapper.extras;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.intervals.Intervals;
import org.apache.lucene.queries.intervals.IntervalsSource;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.tests.analysis.CannedTokenStream;
import org.apache.lucene.tests.analysis.Token;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.AutomatonQueries;
import org.elasticsearch.common.lucene.search.MultiPhrasePrefixQuery;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.BlockSourceReader;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.FieldTypeTestCase;
import org.elasticsearch.index.mapper.IndexType;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.MappingParserContext;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.mapper.blockloader.DelegatingBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.BytesRefsFromBinaryMultiSeparateCountBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.BytesRefsFromCustomBinaryBlockLoader;
import org.elasticsearch.index.mapper.extras.MatchOnlyTextFieldMapper.MatchOnlyTextFieldType;
import org.elasticsearch.lucene.queries.SlowCustomBinaryDocValuesPrefixQuery;
import org.elasticsearch.lucene.queries.SlowCustomBinaryDocValuesWildcardQuery;
import org.elasticsearch.script.ScriptCompiler;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.runtime.StringScriptFieldPrefixQuery;
import org.elasticsearch.search.runtime.StringScriptFieldRegexpQuery;
import org.elasticsearch.search.runtime.StringScriptFieldWildcardQuery;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MatchOnlyTextFieldTypeTests extends FieldTypeTestCase {

    // Doc values disabled, the match_only_text default; HIGH cardinality and multiValue are irrelevant when enabled is false.
    private static final FieldMapper.DocValuesParameter.Values DOC_VALUES_DISABLED = new FieldMapper.DocValuesParameter.Values(
        false,
        FieldMapper.DocValuesParameter.Values.Cardinality.HIGH,
        true
    );

    public void testTermQuery() {
        MappedFieldType ft = new MatchOnlyTextFieldType("field");
        assertEquals(new ConstantScoreQuery(new TermQuery(new Term("field", "foo"))), ft.termQuery("foo", null));
        assertEquals(AutomatonQueries.caseInsensitiveTermQuery(new Term("field", "fOo")), ft.termQueryCaseInsensitive("fOo", null));
    }

    public void testTermsQuery() {
        MappedFieldType ft = new MatchOnlyTextFieldType("field");
        List<BytesRef> terms = new ArrayList<>();
        terms.add(new BytesRef("foo"));
        terms.add(new BytesRef("bar"));
        assertEquals(new TermInSetQuery("field", terms), ft.termsQuery(Arrays.asList("foo", "bar"), null));
    }

    public void testRangeQuery() {
        MappedFieldType ft = new MatchOnlyTextFieldType("field");
        assertEquals(
            new TermRangeQuery("field", BytesRefs.toBytesRef("foo"), BytesRefs.toBytesRef("bar"), true, false),
            ft.rangeQuery("foo", "bar", true, false, null, null, null, MOCK_CONTEXT)
        );

        ElasticsearchException ee = expectThrows(
            ElasticsearchException.class,
            () -> ft.rangeQuery("foo", "bar", true, false, null, null, null, MOCK_CONTEXT_DISALLOW_EXPENSIVE)
        );
        assertEquals(
            "[range] queries on [text] or [keyword] fields cannot be executed when " + "'search.allow_expensive_queries' is set to false.",
            ee.getMessage()
        );
    }

    public void testRegexpQuery() {
        MappedFieldType ft = new MatchOnlyTextFieldType("field");
        assertEquals(new RegexpQuery(new Term("field", "foo.*")), ft.regexpQuery("foo.*", 0, 0, 10, null, MOCK_CONTEXT));

        ElasticsearchException ee = expectThrows(
            ElasticsearchException.class,
            () -> ft.regexpQuery("foo.*", randomInt(10), 0, randomInt(10) + 1, null, MOCK_CONTEXT_DISALLOW_EXPENSIVE)
        );
        assertEquals("[regexp] queries cannot be executed when 'search.allow_expensive_queries' is set to false.", ee.getMessage());
    }

    public void testFuzzyQuery() {
        MappedFieldType ft = new MatchOnlyTextFieldType("field");
        assertEquals(
            new ConstantScoreQuery(new FuzzyQuery(new Term("field", "foo"), 2, 1, 50, true)),
            ft.fuzzyQuery("foo", Fuzziness.fromEdits(2), 1, 50, true, MOCK_CONTEXT)
        );

        ElasticsearchException ee = expectThrows(
            ElasticsearchException.class,
            () -> ft.fuzzyQuery(
                "foo",
                Fuzziness.AUTO,
                randomInt(10) + 1,
                randomInt(10) + 1,
                randomBoolean(),
                MOCK_CONTEXT_DISALLOW_EXPENSIVE
            )
        );
        assertEquals("[fuzzy] queries cannot be executed when 'search.allow_expensive_queries' is set to false.", ee.getMessage());
    }

    public void testFetchSourceValue() throws IOException {
        MatchOnlyTextFieldType fieldType = new MatchOnlyTextFieldType("field");

        assertEquals(List.of("value"), fetchSourceValue(fieldType, "value"));
        assertEquals(List.of("42"), fetchSourceValue(fieldType, 42L));
        assertEquals(List.of("true"), fetchSourceValue(fieldType, true));
    }

    private Query unwrapPositionalQuery(Query query) {
        query = ((ConstantScoreQuery) query).getQuery();
        query = ((SourceConfirmedTextQuery) query).getQuery();
        return query;
    }

    public void testPhraseQuery() throws IOException {
        MappedFieldType ft = new MatchOnlyTextFieldType("field");
        TokenStream ts = new CannedTokenStream(new Token("a", 0, 3), new Token("b", 4, 7));
        Query query = ft.phraseQuery(ts, 0, true, MOCK_CONTEXT);
        Query delegate = unwrapPositionalQuery(query);
        assertEquals(new PhraseQuery("field", "a", "b"), delegate);
        assertNotEquals(Queries.ALL_DOCS_INSTANCE, SourceConfirmedTextQuery.approximate(delegate));
    }

    public void testMultiPhraseQuery() throws IOException {
        MappedFieldType ft = new MatchOnlyTextFieldType("field");
        TokenStream ts = new CannedTokenStream(new Token("a", 0, 3), new Token("b", 0, 0, 3), new Token("c", 4, 7));
        Query query = ft.multiPhraseQuery(ts, 0, true, MOCK_CONTEXT);
        Query delegate = unwrapPositionalQuery(query);
        MultiPhraseQuery expected = new MultiPhraseQuery.Builder().add(new Term[] { new Term("field", "a"), new Term("field", "b") })
            .add(new Term("field", "c"))
            .build();
        assertEquals(expected, delegate);
        assertNotEquals(Queries.ALL_DOCS_INSTANCE, SourceConfirmedTextQuery.approximate(delegate));
    }

    public void testPhrasePrefixQuery() throws IOException {
        MappedFieldType ft = new MatchOnlyTextFieldType("field");
        TokenStream ts = new CannedTokenStream(new Token("a", 0, 3), new Token("b", 0, 0, 3), new Token("c", 4, 7));
        Query query = ft.phrasePrefixQuery(ts, 0, 10, MOCK_CONTEXT);
        Query delegate = unwrapPositionalQuery(query);
        MultiPhrasePrefixQuery expected = new MultiPhrasePrefixQuery("field");
        expected.add(new Term[] { new Term("field", "a"), new Term("field", "b") });
        expected.add(new Term("field", "c"));
        assertEquals(expected, delegate);
        assertNotEquals(Queries.ALL_DOCS_INSTANCE, SourceConfirmedTextQuery.approximate(delegate));
    }

    public void testTermIntervals() {
        MatchOnlyTextFieldType ft = new MatchOnlyTextFieldType("field");
        IntervalsSource termIntervals = ft.termIntervals(new BytesRef("foo"), MOCK_CONTEXT);
        assertThat(termIntervals, Matchers.instanceOf(SourceIntervalsSource.class));
        assertEquals(Intervals.term(new BytesRef("foo")), ((SourceIntervalsSource) termIntervals).getIntervalsSource());
    }

    public void testPrefixIntervals() {
        MatchOnlyTextFieldType ft = new MatchOnlyTextFieldType("field");
        IntervalsSource prefixIntervals = ft.prefixIntervals(new BytesRef("foo"), MOCK_CONTEXT);
        assertThat(prefixIntervals, Matchers.instanceOf(SourceIntervalsSource.class));
        assertEquals(
            Intervals.prefix(new BytesRef("foo"), IndexSearcher.getMaxClauseCount()),
            ((SourceIntervalsSource) prefixIntervals).getIntervalsSource()
        );
    }

    public void testWildcardIntervals() {
        MatchOnlyTextFieldType ft = new MatchOnlyTextFieldType("field");
        IntervalsSource wildcardIntervals = ft.wildcardIntervals(new BytesRef("foo"), MOCK_CONTEXT);
        assertThat(wildcardIntervals, Matchers.instanceOf(SourceIntervalsSource.class));
        assertEquals(
            Intervals.wildcard(new BytesRef("foo"), IndexSearcher.getMaxClauseCount()),
            ((SourceIntervalsSource) wildcardIntervals).getIntervalsSource()
        );
    }

    public void testRegexpIntervals() {
        MatchOnlyTextFieldType ft = new MatchOnlyTextFieldType("field");
        IntervalsSource regexpIntervals = ft.regexpIntervals(new BytesRef("foo"), MOCK_CONTEXT);
        assertThat(regexpIntervals, Matchers.instanceOf(SourceIntervalsSource.class));
        assertEquals(
            Intervals.regexp(new BytesRef("foo"), IndexSearcher.getMaxClauseCount()),
            ((SourceIntervalsSource) regexpIntervals).getIntervalsSource()
        );
    }

    public void testFuzzyIntervals() {
        MatchOnlyTextFieldType ft = new MatchOnlyTextFieldType("field");
        IntervalsSource fuzzyIntervals = ft.fuzzyIntervals("foo", 1, 2, true, MOCK_CONTEXT);
        assertThat(fuzzyIntervals, Matchers.instanceOf(SourceIntervalsSource.class));
    }

    public void testRangeIntervals() {
        MatchOnlyTextFieldType ft = new MatchOnlyTextFieldType("field");
        IntervalsSource rangeIntervals = ft.rangeIntervals(new BytesRef("foo"), new BytesRef("foo1"), true, true, MOCK_CONTEXT);
        assertThat(rangeIntervals, Matchers.instanceOf(SourceIntervalsSource.class));
        assertEquals(
            Intervals.range(new BytesRef("foo"), new BytesRef("foo1"), true, true, IndexSearcher.getMaxClauseCount()),
            ((SourceIntervalsSource) rangeIntervals).getIntervalsSource()
        );
    }

    public void testBlockLoaderUsesStoredFieldsForLoadingWhenSyntheticSourceDelegateIsAbsent() {
        // given
        MatchOnlyTextFieldMapper.MatchOnlyTextFieldType ft = new MatchOnlyTextFieldMapper.MatchOnlyTextFieldType(
            "field",
            new TextSearchInfo(TextFieldMapper.Defaults.FIELD_TYPE, null, Lucene.STANDARD_ANALYZER, Lucene.STANDARD_ANALYZER),
            mock(NamedAnalyzer.class),
            true,
            Collections.emptyMap(),
            false,
            false,
            null,
            false,
            IndexVersion.current(),
            true,
            false,
            false,
            DOC_VALUES_DISABLED
        );

        // when
        BlockLoader blockLoader = ft.blockLoader(mockContext());

        // then - should load from a fallback stored field
        assertThat(blockLoader, Matchers.instanceOf(MatchOnlyTextFieldType.BytesFromMixedStringsBytesRefBlockLoader.class));
    }

    public void testBlockLoaderUsesBinaryDocValuesForLoadingWhenSyntheticSourceDelegateIsAbsent() {
        // given
        MatchOnlyTextFieldMapper.MatchOnlyTextFieldType ft = new MatchOnlyTextFieldMapper.MatchOnlyTextFieldType(
            "field",
            new TextSearchInfo(TextFieldMapper.Defaults.FIELD_TYPE, null, Lucene.STANDARD_ANALYZER, Lucene.STANDARD_ANALYZER),
            mock(NamedAnalyzer.class),
            true,
            Collections.emptyMap(),
            false,
            false,
            null,
            true,
            IndexVersion.current(),
            true,
            false,
            false,
            DOC_VALUES_DISABLED
        );

        // when
        BlockLoader blockLoader = ft.blockLoader(mockContext());

        // then - should load from binary doc values using separate count format
        assertThat(blockLoader, Matchers.instanceOf(BytesRefsFromBinaryMultiSeparateCountBlockLoader.class));
    }

    public void testBlockLoaderUsesSyntheticSourceDelegateWhenIgnoreAboveIsNotSet() {
        // given
        KeywordFieldMapper.KeywordFieldType syntheticSourceDelegate = new KeywordFieldMapper.KeywordFieldType(
            "child",
            true,
            true,
            Collections.emptyMap()
        );

        MatchOnlyTextFieldMapper.MatchOnlyTextFieldType ft = new MatchOnlyTextFieldMapper.MatchOnlyTextFieldType(
            "parent",
            new TextSearchInfo(TextFieldMapper.Defaults.FIELD_TYPE, null, Lucene.STANDARD_ANALYZER, Lucene.STANDARD_ANALYZER),
            mock(NamedAnalyzer.class),
            true,
            Collections.emptyMap(),
            false,
            false,
            syntheticSourceDelegate,
            true,
            IndexVersion.current(),
            true,
            false,
            false,
            DOC_VALUES_DISABLED
        );

        // when
        BlockLoader blockLoader = ft.blockLoader(mockContext());

        // then
        // verify that we delegate block loading to the synthetic source delegate
        assertThat(blockLoader, Matchers.instanceOf(DelegatingBlockLoader.class));
    }

    public void testBlockLoaderDoesNotUseSyntheticSourceDelegateWhenIgnoreAboveIsSet() {
        // given
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put(IndexSettings.MODE.getKey(), IndexMode.STANDARD)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .build();
        IndexSettings indexSettings = new IndexSettings(IndexMetadata.builder("index").settings(settings).build(), settings);
        MappingParserContext mappingParserContext = mock(MappingParserContext.class);
        doReturn(settings).when(mappingParserContext).getSettings();
        doReturn(indexSettings).when(mappingParserContext).getIndexSettings();
        doReturn(mock(ScriptCompiler.class)).when(mappingParserContext).scriptCompiler();

        KeywordFieldMapper.Builder builder = new KeywordFieldMapper.Builder("child", mappingParserContext);
        builder.ignoreAbove(123);

        KeywordFieldMapper.KeywordFieldType syntheticSourceDelegate = new KeywordFieldMapper.KeywordFieldType(
            "child",
            IndexType.terms(true, true),
            new TextSearchInfo(mock(FieldType.class), null, mock(NamedAnalyzer.class), mock(NamedAnalyzer.class)),
            mock(NamedAnalyzer.class),
            builder,
            true
        );

        MatchOnlyTextFieldMapper.MatchOnlyTextFieldType ft = new MatchOnlyTextFieldMapper.MatchOnlyTextFieldType(
            "parent",
            new TextSearchInfo(TextFieldMapper.Defaults.FIELD_TYPE, null, Lucene.STANDARD_ANALYZER, Lucene.STANDARD_ANALYZER),
            mock(NamedAnalyzer.class),
            true,
            Collections.emptyMap(),
            false,
            false,
            syntheticSourceDelegate,
            false,
            IndexVersion.current(),
            true,
            false,
            false,
            DOC_VALUES_DISABLED
        );

        // when
        MappedFieldType.BlockLoaderContext blContext = mock(MappedFieldType.BlockLoaderContext.class);
        doReturn(FieldNamesFieldMapper.FieldNamesFieldType.get(false)).when(blContext).fieldNames();
        when(blContext.indexSettings()).thenReturn(indexSettings);
        BlockLoader blockLoader = ft.blockLoader(blContext);

        // then
        // verify that we don't delegate anything
        assertThat(blockLoader, Matchers.not(Matchers.instanceOf(DelegatingBlockLoader.class)));
    }

    public void testBlockLoaderDoesNotUseSyntheticSourceDelegateWhenIgnoreAboveIsSetAtIndexLevel() {
        // given
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put(IndexSettings.MODE.getKey(), IndexMode.STANDARD)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexSettings.IGNORE_ABOVE_SETTING.getKey(), 123)
            .build();
        IndexSettings indexSettings = new IndexSettings(IndexMetadata.builder("index").settings(settings).build(), settings);
        MappingParserContext mappingParserContext = mock(MappingParserContext.class);
        doReturn(settings).when(mappingParserContext).getSettings();
        doReturn(indexSettings).when(mappingParserContext).getIndexSettings();
        doReturn(mock(ScriptCompiler.class)).when(mappingParserContext).scriptCompiler();

        KeywordFieldMapper.Builder builder = new KeywordFieldMapper.Builder("child", mappingParserContext);

        KeywordFieldMapper.KeywordFieldType syntheticSourceDelegate = new KeywordFieldMapper.KeywordFieldType(
            "child",
            IndexType.terms(true, true),
            new TextSearchInfo(mock(FieldType.class), null, mock(NamedAnalyzer.class), mock(NamedAnalyzer.class)),
            mock(NamedAnalyzer.class),
            builder,
            true
        );

        MatchOnlyTextFieldMapper.MatchOnlyTextFieldType ft = new MatchOnlyTextFieldMapper.MatchOnlyTextFieldType(
            "parent",
            new TextSearchInfo(TextFieldMapper.Defaults.FIELD_TYPE, null, Lucene.STANDARD_ANALYZER, Lucene.STANDARD_ANALYZER),
            mock(NamedAnalyzer.class),
            true,
            Collections.emptyMap(),
            false,
            false,
            syntheticSourceDelegate,
            false,
            IndexVersion.current(),
            true,
            false,
            false,
            DOC_VALUES_DISABLED
        );

        // when
        MappedFieldType.BlockLoaderContext blContext = mock(MappedFieldType.BlockLoaderContext.class);
        when(blContext.indexSettings()).thenReturn(indexSettings);
        doReturn(FieldNamesFieldMapper.FieldNamesFieldType.get(false)).when(blContext).fieldNames();
        BlockLoader blockLoader = ft.blockLoader(blContext);

        // then
        // verify that we don't delegate anything
        assertThat(blockLoader, Matchers.not(Matchers.instanceOf(DelegatingBlockLoader.class)));
    }

    public void testBlockLoaderDelegateToKeywordFieldWhenSyntheticSourceIsDisabled() {
        String parentFieldName = "foo";
        String childFieldName = "foo.bar";
        // given
        KeywordFieldMapper.KeywordFieldType keywordFieldType = new KeywordFieldMapper.KeywordFieldType(
            parentFieldName,
            true,
            true,
            Collections.emptyMap()
        );

        MatchOnlyTextFieldMapper.MatchOnlyTextFieldType ft = new MatchOnlyTextFieldMapper.MatchOnlyTextFieldType(
            childFieldName,
            new TextSearchInfo(TextFieldMapper.Defaults.FIELD_TYPE, null, Lucene.STANDARD_ANALYZER, Lucene.STANDARD_ANALYZER),
            mock(NamedAnalyzer.class),
            false,
            Collections.emptyMap(),
            true,
            false,
            keywordFieldType,
            false,
            IndexVersion.current(),
            true,
            false,
            false,
            DOC_VALUES_DISABLED
        );

        var mockedSearchLookup = mock(SearchLookup.class);
        when(mockedSearchLookup.fieldType(parentFieldName)).thenReturn(keywordFieldType);

        var mockedBlockLoaderContext = mockContext();
        when(mockedBlockLoaderContext.parentField(childFieldName)).thenReturn(parentFieldName);
        when(mockedBlockLoaderContext.lookup()).thenReturn(mockedSearchLookup);
        BlockLoader blockLoader = ft.blockLoader(mockedBlockLoaderContext);
        assertThat(blockLoader, Matchers.instanceOf(DelegatingBlockLoader.class));
    }

    public void testBlockLoaderLoadsFromSourceByDefault() {
        // given
        MatchOnlyTextFieldType ft = new MatchOnlyTextFieldType("field");

        // we must mock IndexSettings as the block loader will check them for the index version
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .build();
        IndexSettings indexSettings = new IndexSettings(IndexMetadata.builder("index").settings(settings).build(), settings);

        // when
        var context = mock(MappedFieldType.BlockLoaderContext.class);
        when(context.indexSettings()).thenReturn(indexSettings);
        when(context.parentField("field")).thenReturn(null);
        when(context.fieldNames()).thenReturn(FieldNamesFieldMapper.FieldNamesFieldType.get(false));
        BlockLoader blockLoader = ft.blockLoader(context);

        // then
        assertThat(blockLoader, Matchers.instanceOf(BlockSourceReader.BytesRefsBlockLoader.class));
    }

    public void testBlockLoaderLoadsFromFallbackStoredFieldWhenSyntheticSourceIsEnabled() {
        // given
        MatchOnlyTextFieldType ft = new MatchOnlyTextFieldMapper.MatchOnlyTextFieldType(
            "field",
            new TextSearchInfo(TextFieldMapper.Defaults.FIELD_TYPE, null, Lucene.STANDARD_ANALYZER, Lucene.STANDARD_ANALYZER),
            mock(NamedAnalyzer.class),
            true,
            Collections.emptyMap(),
            false,
            false,
            null,
            false,
            IndexVersion.current(),
            true,
            false,
            false,
            DOC_VALUES_DISABLED
        );

        // when
        var context = mock(MappedFieldType.BlockLoaderContext.class);
        doReturn(MappingLookup.EMPTY).when(context).mappingLookup();
        BlockLoader blockLoader = ft.blockLoader(context);

        // then - should load from a fallback binary doc values field
        assertThat(blockLoader, Matchers.instanceOf(MatchOnlyTextFieldType.BytesFromMixedStringsBytesRefBlockLoader.class));
    }

    public void testBlockLoaderLoadsFromFallbackBinaryDocValuesWhenSyntheticSourceIsEnabled() {
        // given
        MatchOnlyTextFieldType ft = new MatchOnlyTextFieldMapper.MatchOnlyTextFieldType(
            "field",
            new TextSearchInfo(TextFieldMapper.Defaults.FIELD_TYPE, null, Lucene.STANDARD_ANALYZER, Lucene.STANDARD_ANALYZER),
            mock(NamedAnalyzer.class),
            true,
            Collections.emptyMap(),
            false,
            false,
            null,
            true,
            IndexVersion.current(),
            true,
            false,
            false,
            DOC_VALUES_DISABLED
        );

        // when
        var context = mock(MappedFieldType.BlockLoaderContext.class);
        doReturn(MappingLookup.EMPTY).when(context).mappingLookup();
        BlockLoader blockLoader = ft.blockLoader(context);

        // then - should load from a fallback binary doc values field using separate count format
        assertThat(blockLoader, Matchers.instanceOf(BytesRefsFromBinaryMultiSeparateCountBlockLoader.class));
    }

    public void testBlockLoaderLoadsFromFallbackBinaryDocValuesWhenSyntheticSourceIsEnabledWithPreviousIndexVersion() {
        // given
        MatchOnlyTextFieldType ft = new MatchOnlyTextFieldMapper.MatchOnlyTextFieldType(
            "field",
            new TextSearchInfo(TextFieldMapper.Defaults.FIELD_TYPE, null, Lucene.STANDARD_ANALYZER, Lucene.STANDARD_ANALYZER),
            mock(NamedAnalyzer.class),
            true,
            Collections.emptyMap(),
            false,
            false,
            null,
            true,
            IndexVersionUtils.getPreviousVersion(IndexVersions.DEPRECATE_INTEGRATED_COUNTS_BINARY_DOC_VALUES),
            true,
            false,
            false,
            DOC_VALUES_DISABLED
        );

        // when
        var context = mock(MappedFieldType.BlockLoaderContext.class);
        doReturn(MappingLookup.EMPTY).when(context).mappingLookup();
        BlockLoader blockLoader = ft.blockLoader(context);

        // then - should load from a fallback binary doc values field using integrated count format
        assertThat(blockLoader, Matchers.instanceOf(BytesRefsFromCustomBinaryBlockLoader.class));
    }

    public void testTermQueryWithBinaryDocValues() throws IOException {
        assertTermQueryWithBinaryDocValues(binaryDocValuesOnly());
    }

    public void testPrefixQueryDocValuesOnly() {
        MatchOnlyTextFieldType sortedSet = sortedSetDocValuesOnly();
        MatchOnlyTextFieldType binary = binaryDocValuesOnly();

        // SortedSet DV, case-sensitive: native PrefixQuery with DOC_VALUES_REWRITE
        assertThat(
            sortedSet.prefixQuery("foo", null, false, MOCK_CONTEXT),
            Matchers.equalTo(new PrefixQuery(new Term("field", "foo"), MultiTermQuery.DOC_VALUES_REWRITE))
        );

        // SortedSet DV, case-insensitive: script-backed query
        assertThat(sortedSet.prefixQuery("foo", null, true, MOCK_CONTEXT), Matchers.instanceOf(StringScriptFieldPrefixQuery.class));

        // Binary DV: SlowCustomBinaryDocValuesPrefixQuery
        assertThat(binary.prefixQuery("foo", null, false, MOCK_CONTEXT), Matchers.instanceOf(SlowCustomBinaryDocValuesPrefixQuery.class));

        // Doc-values only, expensive queries disabled
        ElasticsearchException ee = expectThrows(
            ElasticsearchException.class,
            () -> sortedSetDocValuesOnly().prefixQuery("foo", null, false, MOCK_CONTEXT_DISALLOW_EXPENSIVE)
        );
        assertThat(ee.getMessage(), Matchers.containsString("not indexed and 'search.allow_expensive_queries' is set to false"));
    }

    public void testWildcardQueryDocValuesOnly() {
        MatchOnlyTextFieldType sortedSet = sortedSetDocValuesOnly();
        MatchOnlyTextFieldType binary = binaryDocValuesOnly();

        // SortedSet DV, case-sensitive: WildcardQuery with DOC_VALUES_REWRITE
        assertThat(
            sortedSet.wildcardQuery("foo*", null, false, MOCK_CONTEXT),
            Matchers.equalTo(
                new WildcardQuery(new Term("field", "foo*"), Operations.DEFAULT_DETERMINIZE_WORK_LIMIT, MultiTermQuery.DOC_VALUES_REWRITE)
            )
        );

        // SortedSet DV, case-insensitive: script-backed query
        assertThat(sortedSet.wildcardQuery("foo*", null, true, MOCK_CONTEXT), Matchers.instanceOf(StringScriptFieldWildcardQuery.class));

        // Binary DV: SlowCustomBinaryDocValuesWildcardQuery
        assertThat(
            binary.wildcardQuery("foo*", null, false, MOCK_CONTEXT),
            Matchers.instanceOf(SlowCustomBinaryDocValuesWildcardQuery.class)
        );

        // Doc-values only, expensive queries disabled
        ElasticsearchException ee = expectThrows(
            ElasticsearchException.class,
            () -> sortedSetDocValuesOnly().wildcardQuery("foo*", null, false, MOCK_CONTEXT_DISALLOW_EXPENSIVE)
        );
        assertThat(ee.getMessage(), Matchers.containsString("not indexed and 'search.allow_expensive_queries' is set to false"));
    }

    public void testRegexpQueryDocValuesOnly() {
        MatchOnlyTextFieldType sortedSet = sortedSetDocValuesOnly();
        MatchOnlyTextFieldType binary = binaryDocValuesOnly();

        // SortedSet DV: RegexpQuery with DOC_VALUES_REWRITE
        assertThat(
            sortedSet.regexpQuery("foo.*", 0, 0, 10, null, MOCK_CONTEXT),
            Matchers.equalTo(
                new RegexpQuery(new Term("field", "foo.*"), 0, 0, RegexpQuery.DEFAULT_PROVIDER, 10, MultiTermQuery.DOC_VALUES_REWRITE)
            )
        );

        // Binary DV: script-backed query
        assertThat(binary.regexpQuery("foo.*", 0, 0, 10, null, MOCK_CONTEXT), Matchers.instanceOf(StringScriptFieldRegexpQuery.class));

        // Doc-values only, expensive queries disabled
        ElasticsearchException ee = expectThrows(
            ElasticsearchException.class,
            () -> sortedSetDocValuesOnly().regexpQuery("foo.*", 0, 0, 10, null, MOCK_CONTEXT_DISALLOW_EXPENSIVE)
        );
        assertThat(ee.getMessage(), Matchers.containsString("not indexed and 'search.allow_expensive_queries' is set to false"));
    }

    public void testRegexpQueryDocValuesOnlyCaseInsensitive() {
        // SortedSet DV → RegexpQuery with DOC_VALUES_REWRITE and ASCII_CASE_INSENSITIVE matchFlag
        Query q = sortedSetDocValuesOnly().regexpQuery("foo.*", 0, RegExp.ASCII_CASE_INSENSITIVE, 10, null, MOCK_CONTEXT);
        assertThat(q, Matchers.instanceOf(RegexpQuery.class));
        assertEquals(MultiTermQuery.DOC_VALUES_REWRITE, ((RegexpQuery) q).getRewriteMethod());

        // Binary DV → StringScriptFieldRegexpQuery with ASCII_CASE_INSENSITIVE matchFlag
        assertThat(
            binaryDocValuesOnly().regexpQuery("foo.*", 0, RegExp.ASCII_CASE_INSENSITIVE, 10, null, MOCK_CONTEXT),
            Matchers.instanceOf(StringScriptFieldRegexpQuery.class)
        );
    }

    private static MatchOnlyTextFieldType sortedSetDocValuesOnly() {
        return new MatchOnlyTextFieldType(
            "field",
            TextSearchInfo.NONE,
            Lucene.STANDARD_ANALYZER,
            false,
            Collections.emptyMap(),
            false,
            false,
            null,
            false,
            IndexVersion.current(),
            false,
            false,
            false,
            new FieldMapper.DocValuesParameter.Values(true, FieldMapper.DocValuesParameter.Values.Cardinality.HIGH, true),
            false
        );
    }

    private static MatchOnlyTextFieldType binaryDocValuesOnly() {
        return new MatchOnlyTextFieldType(
            "field",
            TextSearchInfo.NONE,
            Lucene.STANDARD_ANALYZER,
            false,
            Collections.emptyMap(),
            false,
            false,
            null,
            false,
            IndexVersion.current(),
            false,
            true,
            false,
            new FieldMapper.DocValuesParameter.Values(true, FieldMapper.DocValuesParameter.Values.Cardinality.HIGH, true),
            false
        );
    }

    private static MappedFieldType.BlockLoaderContext mockContext() {
        MappedFieldType.BlockLoaderContext context = mock(MappedFieldType.BlockLoaderContext.class);
        when(context.ordinalsByteSize()).thenReturn(MappedFieldType.BlockLoaderContext.DEFAULT_ORDINALS_BYTE_SIZE);
        doReturn(MappingLookup.EMPTY).when(context).mappingLookup();
        return context;
    }
}

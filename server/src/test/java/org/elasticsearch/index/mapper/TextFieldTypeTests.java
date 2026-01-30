/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.mapper;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.intervals.Intervals;
import org.apache.lucene.queries.intervals.IntervalsSource;
import org.apache.lucene.search.AutomatonQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.AutomatonQueries;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.TextFieldMapper.TextFieldType;
import org.elasticsearch.index.mapper.blockloader.DelegatingBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.BytesRefsFromBinaryMultiSeparateCountBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.BytesRefsFromCustomBinaryBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.BytesRefsFromOrdsBlockLoader;
import org.elasticsearch.script.ScriptCompiler;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.lucene.search.MultiTermQuery.CONSTANT_SCORE_BLENDED_REWRITE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TextFieldTypeTests extends FieldTypeTestCase {

    private static TextFieldType createFieldType() {
        return new TextFieldType("field", randomBoolean(), false);
    }

    public void testIsAggregatableDependsOnFieldData() {
        TextFieldType ft = createFieldType();
        assertFalse(ft.isAggregatable());
        ft.setFielddata(true);
        assertTrue(ft.isAggregatable());
    }

    public void testTermQuery() {
        MappedFieldType ft = createFieldType();
        assertEquals(new TermQuery(new Term("field", "foo")), ft.termQuery("foo", MOCK_CONTEXT));
        assertEquals(AutomatonQueries.caseInsensitiveTermQuery(new Term("field", "fOo")), ft.termQueryCaseInsensitive("fOo", null));

        MappedFieldType unsearchable = new TextFieldType("field", false, false, Collections.emptyMap());
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> unsearchable.termQuery("bar", MOCK_CONTEXT));
        assertEquals("Cannot search on field [field] since it is not indexed nor has doc values.", e.getMessage());
    }

    public void testTermsQuery() {
        MappedFieldType ft = createFieldType();
        List<BytesRef> terms = new ArrayList<>();
        terms.add(new BytesRef("foo"));
        terms.add(new BytesRef("bar"));
        assertEquals(new TermInSetQuery("field", terms), ft.termsQuery(Arrays.asList("foo", "bar"), MOCK_CONTEXT));

        MappedFieldType unsearchable = new TextFieldType("field", false, false, Collections.emptyMap());
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> unsearchable.termsQuery(Arrays.asList("foo", "bar"), MOCK_CONTEXT)
        );
        assertEquals("Cannot search on field [field] since it is not indexed nor has doc values.", e.getMessage());
    }

    public void testRangeQuery() {
        MappedFieldType ft = createFieldType();
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
        MappedFieldType ft = createFieldType();
        assertEquals(new RegexpQuery(new Term("field", "foo.*")), ft.regexpQuery("foo.*", 0, 0, 10, null, MOCK_CONTEXT));

        MappedFieldType unsearchable = new TextFieldType("field", false, false, Collections.emptyMap());
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> unsearchable.regexpQuery("foo.*", 0, 0, 10, null, MOCK_CONTEXT)
        );
        assertEquals("Cannot search on field [field] since it is not indexed.", e.getMessage());

        ElasticsearchException ee = expectThrows(
            ElasticsearchException.class,
            () -> ft.regexpQuery("foo.*", randomInt(10), 0, randomInt(10) + 1, null, MOCK_CONTEXT_DISALLOW_EXPENSIVE)
        );
        assertEquals("[regexp] queries cannot be executed when 'search.allow_expensive_queries' is set to false.", ee.getMessage());
    }

    public void testFuzzyQuery() {
        MappedFieldType ft = createFieldType();
        assertEquals(
            new FuzzyQuery(new Term("field", "foo"), 2, 1, 50, true),
            ft.fuzzyQuery("foo", Fuzziness.fromEdits(2), 1, 50, true, MOCK_CONTEXT)
        );

        MappedFieldType unsearchable = new TextFieldType("field", false, false, Collections.emptyMap());
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> unsearchable.fuzzyQuery("foo", Fuzziness.fromEdits(2), 1, 50, true, MOCK_CONTEXT)
        );
        assertEquals("Cannot search on field [field] since it is not indexed.", e.getMessage());

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

        assertEquals(
            new FuzzyQuery(new Term("field", "foo"), 2, 1, 50, true, MultiTermQuery.CONSTANT_SCORE_BOOLEAN_REWRITE),
            ft.fuzzyQuery("foo", Fuzziness.fromEdits(2), 1, 50, true, MOCK_CONTEXT, MultiTermQuery.CONSTANT_SCORE_BOOLEAN_REWRITE)
        );

    }

    public void testIndexPrefixes() {
        TextFieldType ft = createFieldType();
        ft.setIndexPrefixes(2, 10);

        Query q = ft.prefixQuery("goin", randomBoolean() ? null : CONSTANT_SCORE_BLENDED_REWRITE, false, randomMockContext());
        assertEquals(new ConstantScoreQuery(new TermQuery(new Term("field._index_prefix", "goin"))), q);

        q = ft.prefixQuery("internationalisatio", randomBoolean() ? null : CONSTANT_SCORE_BLENDED_REWRITE, false, MOCK_CONTEXT);
        assertEquals(new PrefixQuery(new Term("field", "internationalisatio")), q);

        q = ft.prefixQuery("Internationalisatio", randomBoolean() ? null : CONSTANT_SCORE_BLENDED_REWRITE, true, MOCK_CONTEXT);
        assertEquals(AutomatonQueries.caseInsensitivePrefixQuery(new Term("field", "Internationalisatio")), q);

        ElasticsearchException ee = expectThrows(
            ElasticsearchException.class,
            () -> ft.prefixQuery("internationalisatio", null, false, MOCK_CONTEXT_DISALLOW_EXPENSIVE)
        );
        assertEquals(
            "[prefix] queries cannot be executed when 'search.allow_expensive_queries' is set to false. "
                + "For optimised prefix queries on text fields please enable [index_prefixes].",
            ee.getMessage()
        );

        q = ft.prefixQuery("g", randomBoolean() ? null : CONSTANT_SCORE_BLENDED_REWRITE, false, randomMockContext());
        Automaton automaton = Operations.concatenate(Arrays.asList(Automata.makeChar('g'), Automata.makeAnyChar()));
        Query expected = new ConstantScoreQuery(
            new BooleanQuery.Builder().add(new AutomatonQuery(new Term("field._index_prefix", "g*"), automaton), BooleanClause.Occur.SHOULD)
                .add(new TermQuery(new Term("field", "g")), BooleanClause.Occur.SHOULD)
                .build()
        );
        assertThat(q, equalTo(expected));
    }

    public void testFetchSourceValue() throws IOException {
        TextFieldType fieldType = createFieldType();

        assertEquals(List.of("value"), fetchSourceValue(fieldType, "value"));
        assertEquals(List.of("42"), fetchSourceValue(fieldType, 42L));
        assertEquals(List.of("true"), fetchSourceValue(fieldType, true));
    }

    public void testWildcardQuery() {
        TextFieldType ft = createFieldType();

        // case sensitive
        AutomatonQuery actual = (AutomatonQuery) ft.wildcardQuery("*Butterflies*", null, false, MOCK_CONTEXT);
        AutomatonQuery expected = new WildcardQuery(new Term("field", new BytesRef("*Butterflies*")));
        assertEquals(expected, actual);
        assertFalse(new CharacterRunAutomaton(actual.getAutomaton()).run("some butterflies somewhere"));

        // case insensitive
        actual = (AutomatonQuery) ft.wildcardQuery("*Butterflies*", null, true, MOCK_CONTEXT);
        expected = AutomatonQueries.caseInsensitiveWildcardQuery(new Term("field", new BytesRef("*Butterflies*")));
        assertEquals(expected, actual);
        assertTrue(new CharacterRunAutomaton(actual.getAutomaton()).run("some butterflies somewhere"));
        assertTrue(new CharacterRunAutomaton(actual.getAutomaton()).run("some Butterflies somewhere"));

        ElasticsearchException ee = expectThrows(
            ElasticsearchException.class,
            () -> ft.wildcardQuery("valu*", null, MOCK_CONTEXT_DISALLOW_EXPENSIVE)
        );
        assertEquals("[wildcard] queries cannot be executed when 'search.allow_expensive_queries' is set to false.", ee.getMessage());
    }

    /**
     * we use this e.g. in query string query parser to normalize terms on text fields
     */
    public void testNormalizedWildcardQuery() {
        TextFieldType ft = createFieldType();

        AutomatonQuery actual = (AutomatonQuery) ft.normalizedWildcardQuery("*Butterflies*", null, MOCK_CONTEXT);
        AutomatonQuery expected = new WildcardQuery(new Term("field", new BytesRef("*butterflies*")));
        assertEquals(expected, actual);
        assertTrue(new CharacterRunAutomaton(actual.getAutomaton()).run("some butterflies somewhere"));
        assertFalse(new CharacterRunAutomaton(actual.getAutomaton()).run("some Butterflies somewhere"));

        ElasticsearchException ee = expectThrows(
            ElasticsearchException.class,
            () -> ft.wildcardQuery("valu*", null, MOCK_CONTEXT_DISALLOW_EXPENSIVE)
        );
        assertEquals("[wildcard] queries cannot be executed when 'search.allow_expensive_queries' is set to false.", ee.getMessage());
    }

    public void testTermIntervals() throws IOException {
        TextFieldType ft = createFieldType();
        IntervalsSource termIntervals = ft.termIntervals(new BytesRef("foo"), MOCK_CONTEXT);
        assertEquals(Intervals.term(new BytesRef("foo")), termIntervals);
    }

    public void testPrefixIntervals() throws IOException {
        TextFieldType ft = createFieldType();
        IntervalsSource prefixIntervals = ft.prefixIntervals(new BytesRef("foo"), MOCK_CONTEXT);
        assertEquals(Intervals.prefix(new BytesRef("foo"), IndexSearcher.getMaxClauseCount()), prefixIntervals);
    }

    public void testWildcardIntervals() {
        TextFieldType ft = createFieldType();
        IntervalsSource wildcardIntervals = ft.wildcardIntervals(new BytesRef("foo"), MOCK_CONTEXT);
        assertEquals(Intervals.wildcard(new BytesRef("foo"), IndexSearcher.getMaxClauseCount()), wildcardIntervals);
    }

    public void testRegexpIntervals() {
        TextFieldType ft = createFieldType();
        IntervalsSource regexpIntervals = ft.regexpIntervals(new BytesRef("foo"), MOCK_CONTEXT);
        assertEquals(Intervals.regexp(new BytesRef("foo"), IndexSearcher.getMaxClauseCount()), regexpIntervals);
    }

    public void testFuzzyIntervals() {
        TextFieldType ft = createFieldType();
        IntervalsSource fuzzyIntervals = ft.fuzzyIntervals("foo", 1, 2, true, MOCK_CONTEXT);
        FuzzyQuery fq = new FuzzyQuery(new Term("field", "foo"), 1, 2, 128, true);
        IntervalsSource expectedIntervals = Intervals.multiterm(fq.getAutomata(), IndexSearcher.getMaxClauseCount(), "foo");
        assertEquals(expectedIntervals, fuzzyIntervals);
    }

    public void testPrefixIntervalsWithIndexedPrefixes() {
        TextFieldType ft = createFieldType();
        ft.setIndexPrefixes(1, 4);
        IntervalsSource prefixIntervals = ft.prefixIntervals(new BytesRef("foo"), MOCK_CONTEXT);
        assertEquals(Intervals.fixField("field._index_prefix", Intervals.term(new BytesRef("foo"))), prefixIntervals);
    }

    public void testWildcardIntervalsWithIndexedPrefixes() {
        TextFieldType ft = createFieldType();
        ft.setIndexPrefixes(1, 4);
        IntervalsSource wildcardIntervals = ft.wildcardIntervals(new BytesRef("foo"), MOCK_CONTEXT);
        assertEquals(Intervals.wildcard(new BytesRef("foo"), IndexSearcher.getMaxClauseCount()), wildcardIntervals);
    }

    public void testRangeIntervals() {
        TextFieldType ft = createFieldType();
        IntervalsSource rangeIntervals = ft.rangeIntervals(new BytesRef("foo"), new BytesRef("foo1"), true, true, MOCK_CONTEXT);
        assertEquals(
            Intervals.range(new BytesRef("foo"), new BytesRef("foo1"), true, true, IndexSearcher.getMaxClauseCount()),
            rangeIntervals
        );
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
        doReturn(true).when(mappingParserContext).isWithinMultiField();

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

        TextFieldType ft = new TextFieldType(
            "parent",
            true,
            false,
            new TextSearchInfo(TextFieldMapper.Defaults.FIELD_TYPE, null, Lucene.STANDARD_ANALYZER, Lucene.STANDARD_ANALYZER),
            true,
            false,
            syntheticSourceDelegate,
            Collections.singletonMap("potato", "tomato"),
            false,
            false
        );

        // when
        var context = mock(MappedFieldType.BlockLoaderContext.class);
        when(context.indexSettings()).thenReturn(indexSettings);
        BlockLoader blockLoader = ft.blockLoader(context);

        // then
        // verify that we don't delegate anything
        assertFalse(blockLoader instanceof DelegatingBlockLoader);
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
        doReturn(true).when(mappingParserContext).isWithinMultiField();

        KeywordFieldMapper.Builder builder = new KeywordFieldMapper.Builder("child", mappingParserContext);

        KeywordFieldMapper.KeywordFieldType syntheticSourceDelegate = new KeywordFieldMapper.KeywordFieldType(
            "child",
            IndexType.terms(true, true),
            new TextSearchInfo(mock(FieldType.class), null, mock(NamedAnalyzer.class), mock(NamedAnalyzer.class)),
            mock(NamedAnalyzer.class),
            builder,
            true
        );

        TextFieldType ft = new TextFieldType(
            "parent",
            true,
            false,
            new TextSearchInfo(TextFieldMapper.Defaults.FIELD_TYPE, null, Lucene.STANDARD_ANALYZER, Lucene.STANDARD_ANALYZER),
            true,
            false,
            syntheticSourceDelegate,
            Collections.singletonMap("potato", "tomato"),
            false,
            false
        );

        // when
        var context = mock(MappedFieldType.BlockLoaderContext.class);
        when(context.indexSettings()).thenReturn(indexSettings);
        BlockLoader blockLoader = ft.blockLoader(context);

        // then
        // verify that we don't delegate anything
        assertFalse(blockLoader instanceof DelegatingBlockLoader);
    }

    public void testBlockLoaderLoadsFromSourceByDefault() {
        // given - text field with default settings (no synthetic source, no store)
        TextFieldType ft = new TextFieldType("field", false, false);

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
        BlockLoader blockLoader = ft.blockLoader(context);

        // then
        assertThat(blockLoader, instanceOf(BlockSourceReader.BytesRefsBlockLoader.class));
    }

    public void testBlockLoaderLoadsFromStoredField() {
        // given - text field thats stored
        TextFieldType ft = new TextFieldType(
            "field",
            true,
            true,
            new TextSearchInfo(TextFieldMapper.Defaults.FIELD_TYPE, null, Lucene.STANDARD_ANALYZER, Lucene.STANDARD_ANALYZER),
            false,
            false,
            null,
            Collections.emptyMap(),
            false,
            false
        );

        // when
        var context = mock(MappedFieldType.BlockLoaderContext.class);
        BlockLoader blockLoader = ft.blockLoader(context);

        // then
        assertThat(blockLoader, instanceOf(BlockStoredFieldsReader.BytesFromStringsBlockLoader.class));
    }

    public void testBlockLoaderLoadsFromStoredFieldWhenSyntheticSourceIsEnabled() {
        // given - text field thats stored
        TextFieldType ft = new TextFieldType(
            "field",
            true,
            true,
            new TextSearchInfo(TextFieldMapper.Defaults.FIELD_TYPE, null, Lucene.STANDARD_ANALYZER, Lucene.STANDARD_ANALYZER),
            true,
            false,
            null,
            Collections.emptyMap(),
            false,
            false
        );

        // when
        var context = mock(MappedFieldType.BlockLoaderContext.class);
        BlockLoader blockLoader = ft.blockLoader(context);

        // then
        assertThat(blockLoader, instanceOf(BlockStoredFieldsReader.BytesFromStringsBlockLoader.class));
    }

    public void testBlockLoaderLoadsFromFallbackStoredFieldWhenSyntheticSourceIsEnabled() {
        // given - text field thats not stored and has no synthetic source delegate
        TextFieldType ft = new TextFieldType("field", true, false);

        // when
        var context = mock(MappedFieldType.BlockLoaderContext.class);
        BlockLoader blockLoader = ft.blockLoader(context);

        // then
        assertThat(blockLoader, instanceOf(BlockStoredFieldsReader.BytesFromStringsBlockLoader.class));
    }

    public void testBlockLoaderDelegatesToSyntheticSourceDelegate() {
        // given
        KeywordFieldMapper.KeywordFieldType keywordDelegate = new KeywordFieldMapper.KeywordFieldType("field.keyword");

        TextFieldType ft = new TextFieldType("field", true, false, keywordDelegate);

        // when
        var context = mock(MappedFieldType.BlockLoaderContext.class);
        BlockLoader blockLoader = ft.blockLoader(context);

        // then
        // should delegate to the keyword multi-field
        assertThat(blockLoader, instanceOf(DelegatingBlockLoader.class));
        assertThat(((DelegatingBlockLoader) blockLoader).delegatingTo(), equalTo("field.keyword"));
    }

    public void testBlockLoaderDelegatesToParentKeywordField() {
        // given - text field as a multi-field of a keyword parent
        String parentFieldName = "parent";
        String childFieldName = "parent.text";

        KeywordFieldMapper.KeywordFieldType parentKeywordType = new KeywordFieldMapper.KeywordFieldType(parentFieldName);

        TextFieldType ft = new TextFieldType(childFieldName, false, true);

        // when
        var mockedSearchLookup = mock(SearchLookup.class);
        when(mockedSearchLookup.fieldType(parentFieldName)).thenReturn(parentKeywordType);

        var context = mock(MappedFieldType.BlockLoaderContext.class);
        when(context.parentField(childFieldName)).thenReturn(parentFieldName);
        when(context.lookup()).thenReturn(mockedSearchLookup);

        BlockLoader blockLoader = ft.blockLoader(context);

        // then - should delegate to parent keyword field
        assertThat(blockLoader, instanceOf(DelegatingBlockLoader.class));
        assertThat(((DelegatingBlockLoader) blockLoader).delegatingTo(), equalTo(parentFieldName));
    }

    public void testBlockLoaderLoadsFromIgnoredSourceInLegacyIndexVersion() {
        // given - text field created in version range where text fields were unintentionally stored in ignored source
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersions.KEYWORD_MULTI_FIELDS_NOT_STORED_WHEN_IGNORED)
            .put(IndexSettings.MODE.getKey(), IndexMode.STANDARD)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .build();
        IndexSettings indexSettings = new IndexSettings(IndexMetadata.builder("index").settings(settings).build(), settings);

        TextFieldType ft = new TextFieldType(
            "field",
            true,
            false,
            false,
            new TextSearchInfo(TextFieldMapper.Defaults.FIELD_TYPE, null, Lucene.STANDARD_ANALYZER, Lucene.STANDARD_ANALYZER),
            true,
            false,
            null,
            Collections.emptyMap(),
            false,
            false,
            IndexVersions.KEYWORD_MULTI_FIELDS_NOT_STORED_WHEN_IGNORED,
            true,
            false
        );

        // when
        var context = mock(MappedFieldType.BlockLoaderContext.class);
        when(context.indexSettings()).thenReturn(indexSettings);
        when(context.fieldNames()).thenReturn(FieldNamesFieldMapper.FieldNamesFieldType.get(false));

        BlockLoader blockLoader = ft.blockLoader(context);

        // then
        assertThat(blockLoader, instanceOf(FallbackSyntheticSourceBlockLoader.class));
    }

    public void testBlockLoaderLoadsFromFallbackStoredFieldWhenSyntheticSourceIsEnabledInLegacyIndexVersion() {
        // given
        IndexVersion legacyVersion = IndexVersions.PATTERN_TEXT_ARGS_IN_BINARY_DOC_VALUES;

        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, legacyVersion)
            .put(IndexSettings.MODE.getKey(), IndexMode.STANDARD)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .build();
        IndexSettings indexSettings = new IndexSettings(IndexMetadata.builder("index").settings(settings).build(), settings);

        TextFieldType ft = new TextFieldType(
            "field",
            true,
            false,
            false,
            new TextSearchInfo(TextFieldMapper.Defaults.FIELD_TYPE, null, Lucene.STANDARD_ANALYZER, Lucene.STANDARD_ANALYZER),
            true,
            false,
            null,
            Collections.emptyMap(),
            false,
            false,
            legacyVersion,
            false,
            false
        );

        // when
        var context = mock(MappedFieldType.BlockLoaderContext.class);
        when(context.parentField("field")).thenReturn(null);
        when(context.indexSettings()).thenReturn(indexSettings);
        BlockLoader blockLoader = ft.blockLoader(context);

        // then
        assertThat(blockLoader, instanceOf(BlockStoredFieldsReader.BytesFromStringsBlockLoader.class));
    }

    public void testBlockLoaderLoadsFromFallbackBinaryDocValuesWhenSyntheticSourceIsEnabled() {
        // given
        IndexVersion legacyVersion = IndexVersions.PATTERN_TEXT_ARGS_IN_BINARY_DOC_VALUES;

        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, legacyVersion)
            .put(IndexSettings.MODE.getKey(), IndexMode.LOGSDB)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .build();
        IndexSettings indexSettings = new IndexSettings(IndexMetadata.builder("index").settings(settings).build(), settings);

        TextFieldType ft = new TextFieldType(
            "field",
            true,
            false,
            false,
            new TextSearchInfo(TextFieldMapper.Defaults.FIELD_TYPE, null, Lucene.STANDARD_ANALYZER, Lucene.STANDARD_ANALYZER),
            true,
            false,
            null,
            Collections.emptyMap(),
            false,
            false,
            legacyVersion,
            true,
            false
        );

        // when
        var context = mock(MappedFieldType.BlockLoaderContext.class);
        when(context.parentField("field")).thenReturn(null);
        when(context.indexSettings()).thenReturn(indexSettings);
        BlockLoader blockLoader = ft.blockLoader(context);

        // then
        assertThat(blockLoader, instanceOf(BytesRefsFromCustomBinaryBlockLoader.class));
    }

    public void testBlockLoaderWithDocValues() {
        // given
        TextFieldType ft = new TextFieldType(
            "field",
            true,
            false,
            true,
            new TextSearchInfo(TextFieldMapper.Defaults.FIELD_TYPE, null, Lucene.STANDARD_ANALYZER, Lucene.STANDARD_ANALYZER),
            false,
            false,
            null,
            Collections.emptyMap(),
            false,
            false,
            IndexVersion.current(),
            false,
            false
        );

        // when
        var context = mock(MappedFieldType.BlockLoaderContext.class);
        when(context.parentField("field")).thenReturn(null);
        BlockLoader blockLoader = ft.blockLoader(context);

        // then
        assertThat(blockLoader, instanceOf(BytesRefsFromOrdsBlockLoader.class));
    }

    public void testBlockLoaderWithDocValuesHighCardinality() {
        // given
        TextFieldType ft = new TextFieldType(
            "field",
            true,
            false,
            true,
            new TextSearchInfo(TextFieldMapper.Defaults.FIELD_TYPE, null, Lucene.STANDARD_ANALYZER, Lucene.STANDARD_ANALYZER),
            false,
            false,
            null,
            Collections.emptyMap(),
            false,
            false,
            IndexVersion.current(),
            false,
            true
        );

        // when
        var context = mock(MappedFieldType.BlockLoaderContext.class);
        when(context.parentField("field")).thenReturn(null);
        BlockLoader blockLoader = ft.blockLoader(context);

        // then
        assertThat(blockLoader, instanceOf(BytesRefsFromBinaryMultiSeparateCountBlockLoader.class));
    }

}

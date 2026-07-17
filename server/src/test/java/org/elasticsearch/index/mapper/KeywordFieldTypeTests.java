/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.mapper;

import com.carrotsearch.randomizedtesting.generators.RandomStrings;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.RegExp;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.CharFilterFactory;
import org.elasticsearch.index.analysis.CustomAnalyzer;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.LowercaseNormalizer;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.analysis.TokenizerFactory;
import org.elasticsearch.index.mapper.KeywordFieldMapper.KeywordFieldType;
import org.elasticsearch.index.mapper.MappedFieldType.Relation;
import org.elasticsearch.lucene.queries.ScanningBinaryDocValuesPrefixQuery;
import org.elasticsearch.lucene.queries.ScanningBinaryDocValuesRegexpQuery;
import org.elasticsearch.lucene.queries.ScanningBinaryDocValuesTermQuery;
import org.elasticsearch.lucene.queries.ScanningBinaryDocValuesWildcardQuery;
import org.elasticsearch.script.ScriptCompiler;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class KeywordFieldTypeTests extends FieldTypeTestCase {

    public void testIsFieldWithinQuery() throws IOException {
        KeywordFieldType ft = new KeywordFieldType("field", randomBoolean(), randomBoolean(), Map.of());
        // current impl ignores args and should always return INTERSECTS
        assertEquals(
            Relation.INTERSECTS,
            ft.isFieldWithinQuery(
                null,
                RandomStrings.randomAsciiLettersOfLengthBetween(random(), 0, 5),
                RandomStrings.randomAsciiLettersOfLengthBetween(random(), 0, 5),
                randomBoolean(),
                randomBoolean(),
                null,
                null,
                MOCK_CONTEXT
            )
        );
    }

    public void testTermQuery() {
        MappedFieldType ft = new KeywordFieldType("field");
        assertEquals(new TermQuery(new Term("field", "foo")), ft.termQuery("foo", MOCK_CONTEXT));

        MappedFieldType ft2 = new KeywordFieldType("field", false, true, Map.of());
        assertEquals(SortedSetDocValuesField.newSlowExactQuery("field", new BytesRef("foo")), ft2.termQuery("foo", MOCK_CONTEXT));

        MappedFieldType unsearchable = new KeywordFieldType("field", false, false, Collections.emptyMap());
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> unsearchable.termQuery("bar", MOCK_CONTEXT));
        assertEquals("Cannot search on field [field] since it is not indexed nor has doc values.", e.getMessage());
    }

    public void testTermQueryWithSingleValueDocValues() throws IOException {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
            .put(IndexSettings.USE_TIME_SERIES_DOC_VALUES_FORMAT_SETTING.getKey(), true)
            .put(FieldMapper.DOC_VALUES_MULTI_VALUE_SETTING.getKey(), false)
            .build();
        IndexSettings indexSettings = new IndexSettings(
            IndexMetadata.builder("index").settings(settings).numberOfShards(1).numberOfReplicas(0).build(),
            Settings.EMPTY
        );
        KeywordFieldMapper.Builder builder = new KeywordFieldMapper.Builder("field", indexSettings);
        builder.docValues(FieldMapper.DocValuesParameter.Values.Cardinality.HIGH);
        builder.indexed(false);
        KeywordFieldType ft = new KeywordFieldType(
            "field",
            IndexType.docValuesOnly(),
            TextSearchInfo.SIMPLE_MATCH_ONLY,
            null,
            builder,
            false
        );
        assertTermQueryWithBinaryDocValues(ft);
    }

    public void testTermQueryHighCardinality() {
        KeywordFieldMapper.Builder builder = new KeywordFieldMapper.Builder("field", defaultIndexSettings());
        builder.docValues(FieldMapper.DocValuesParameter.Values.Cardinality.HIGH);
        MappedFieldType ft = new KeywordFieldType(
            "field",
            IndexType.docValuesOnly(),
            TextSearchInfo.SIMPLE_MATCH_ONLY,
            null,
            builder,
            true
        );
        assertEquals(new ScanningBinaryDocValuesTermQuery("field", new BytesRef("foo"), false), ft.termQuery("foo", MOCK_CONTEXT));
    }

    public void testTermQueryWithNormalizer() {
        Analyzer normalizer = new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                Tokenizer in = new WhitespaceTokenizer();
                TokenFilter out = new LowerCaseFilter(in);
                return new TokenStreamComponents(in, out);
            }

            @Override
            protected TokenStream normalize(String fieldName, TokenStream in) {
                return new LowerCaseFilter(in);
            }
        };
        MappedFieldType ft = new KeywordFieldType("field", new NamedAnalyzer("my_normalizer", AnalyzerScope.INDEX, normalizer));
        assertEquals(new TermQuery(new Term("field", "foo bar")), ft.termQuery("fOo BaR", MOCK_CONTEXT));
    }

    public void testNormalizeWildcardPatternReescapesOperators() {
        // A normalizer can map fullwidth forms to the ASCII wildcard control characters (#150699). Operators the
        // normalizer produces out of literal data are re-escaped, and escape contents are normalized like any literal.
        Analyzer normalizer = new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                Tokenizer in = new WhitespaceTokenizer();
                return new TokenStreamComponents(in, fullwidthToAsciiFilter(in));
            }

            @Override
            protected TokenStream normalize(String fieldName, TokenStream in) {
                return fullwidthToAsciiFilter(in);
            }
        };
        NamedAnalyzer named = new NamedAnalyzer("fullwidth_nfkc", AnalyzerScope.INDEX, normalizer);

        // An escaped fullwidth '＊' is normalized to an escaped ASCII '*': still the literal star.
        assertEquals("foo\\*bar", StringFieldType.normalizeWildcardPattern("f", "foo\\＊bar", named));
        // A bare fullwidth '＊' also normalizes to the literal star, not a wildcard operator.
        assertEquals("foo\\*bar", StringFieldType.normalizeWildcardPattern("f", "foo＊bar", named));
        // Same for the fullwidth '？'.
        assertEquals("foo\\?bar", StringFieldType.normalizeWildcardPattern("f", "foo？bar", named));
        // Real ASCII wildcard operators are preserved verbatim, including at the start and end of the pattern.
        assertEquals("foo*bar", StringFieldType.normalizeWildcardPattern("f", "foo*bar", named));
        assertEquals("foo?bar*", StringFieldType.normalizeWildcardPattern("f", "foo?bar*", named));
        assertEquals("*bar", StringFieldType.normalizeWildcardPattern("f", "*bar", named));
        assertEquals("foo*", StringFieldType.normalizeWildcardPattern("f", "foo*", named));
        // A fullwidth backslash '＼' that normalizes to '\' is re-escaped to a literal backslash, whether the user
        // wrote it bare or escaped.
        assertEquals("foo\\\\bar", StringFieldType.normalizeWildcardPattern("f", "foo＼bar", named));
        assertEquals("foo\\\\bar", StringFieldType.normalizeWildcardPattern("f", "foo\\＼bar", named));
        // A trailing lone backslash is literal data and is re-escaped.
        assertEquals("abc\\\\", StringFieldType.normalizeWildcardPattern("f", "abc\\", named));
        // An escape before a line terminator is still an escape (WILDCARD_PATTERN is DOTALL): "a\<LF>b" is the literal
        // "a<LF>b", and the backslash must not be re-introduced as a literal backslash.
        assertEquals("a\nb", StringFieldType.normalizeWildcardPattern("f", "a\\\nb", named));
    }

    private static TokenFilter fullwidthToAsciiFilter(TokenStream in) {
        // Mimics the ICU NFKC mapping of the wildcard control characters: ＊ -> *, ？ -> ?, ＼ -> \
        return new TokenFilter(in) {
            private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);

            @Override
            public boolean incrementToken() throws IOException {
                if (input.incrementToken() == false) {
                    return false;
                }
                String normalized = termAtt.toString().replace('＊', '*').replace('？', '?').replace('＼', '\\');
                termAtt.setEmpty().append(normalized);
                return true;
            }
        };
    }

    public void testNormalizeWildcardPatternNormalizesContiguousLiteralRunAcrossEscape() {
        // Regression test for #150699: an escape sequence in the middle of a literal run must not split normalization.
        // A context-sensitive normalizer (here a multi-character mapping "ab" -> "x") must see the whole run "ab",
        // even when it is written as "a\b".
        Analyzer normalizer = new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                Tokenizer in = new WhitespaceTokenizer();
                return new TokenStreamComponents(in, mappingFilter(in));
            }

            @Override
            protected TokenStream normalize(String fieldName, TokenStream in) {
                return mappingFilter(in);
            }
        };
        NamedAnalyzer named = new NamedAnalyzer("ab_to_x", AnalyzerScope.INDEX, normalizer);

        // "a\b" is the literal "ab" in Lucene; normalizing the whole contiguous run yields "x".
        assertEquals("x", StringFieldType.normalizeWildcardPattern("f", "a\\b", named));
        // Adjacent escapes accumulate into the same run, so "\a\b" (also literal "ab") yields "x" too.
        assertEquals("x", StringFieldType.normalizeWildcardPattern("f", "\\a\\b", named));
        // A wildcard operator between the two characters keeps them in separate runs, so the mapping does not apply.
        assertEquals("a*b", StringFieldType.normalizeWildcardPattern("f", "a*b", named));
    }

    private static TokenFilter mappingFilter(TokenStream in) {
        // A deliberately context-sensitive normalizer: it only maps the two-character sequence "ab" to "x".
        return new TokenFilter(in) {
            private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);

            @Override
            public boolean incrementToken() throws IOException {
                if (input.incrementToken() == false) {
                    return false;
                }
                String normalized = termAtt.toString().replace("ab", "x");
                termAtt.setEmpty().append(normalized);
                return true;
            }
        };
    }

    public void testTermsQuery() {
        MappedFieldType ft = new KeywordFieldType("field");
        List<BytesRef> terms = List.of(new BytesRef("foo"), new BytesRef("bar"));
        assertEquals(new TermInSetQuery("field", terms), ft.termsQuery(Arrays.asList("foo", "bar"), MOCK_CONTEXT));

        MappedFieldType ft2 = new KeywordFieldType("field", false, true, Map.of());
        assertEquals(SortedSetDocValuesField.newSlowSetQuery("field", terms), ft2.termsQuery(Arrays.asList("foo", "bar"), MOCK_CONTEXT));

        MappedFieldType unsearchable = new KeywordFieldType("field", false, false, Collections.emptyMap());
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> unsearchable.termsQuery(Arrays.asList("foo", "bar"), MOCK_CONTEXT)
        );
        assertEquals("Cannot search on field [field] since it is not indexed nor has doc values.", e.getMessage());
    }

    public void testExistsQuery() {
        {
            KeywordFieldType ft = new KeywordFieldType("field");
            assertEquals(new FieldExistsQuery("field"), ft.existsQuery(MOCK_CONTEXT));
        }
        {
            KeywordFieldType ft = new KeywordFieldType("field", false, true, Map.of());
            assertEquals(new FieldExistsQuery("field"), ft.existsQuery(MOCK_CONTEXT));
        }
        {
            FieldType fieldType = new FieldType();
            fieldType.setOmitNorms(false);
            KeywordFieldType ft = new KeywordFieldType("field", fieldType, false);
            // updated in #130531 so that a field that is neither indexed nor has doc values will generate a TermQuery
            // to avoid ISE from FieldExistsQuery
            assertEquals(new TermQuery(new Term(FieldNamesFieldMapper.NAME, "field")), ft.existsQuery(MOCK_CONTEXT));
        }
        {
            KeywordFieldType ft = new KeywordFieldType("field", true, false, Collections.emptyMap());
            assertEquals(new TermQuery(new Term(FieldNamesFieldMapper.NAME, "field")), ft.existsQuery(MOCK_CONTEXT));
        }
    }

    public void testRangeQuery() {
        MappedFieldType ft = new KeywordFieldType("field");
        assertEquals(
            new TermRangeQuery("field", BytesRefs.toBytesRef("foo"), BytesRefs.toBytesRef("bar"), true, false),
            ft.rangeQuery("foo", "bar", true, false, null, null, null, MOCK_CONTEXT)
        );

        MappedFieldType ft2 = new KeywordFieldType("field", false, true, Map.of());
        assertEquals(
            SortedSetDocValuesField.newSlowRangeQuery("field", BytesRefs.toBytesRef("foo"), BytesRefs.toBytesRef("bar"), true, false),
            ft2.rangeQuery("foo", "bar", true, false, null, null, null, MOCK_CONTEXT)
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

    public void testPrefixQueryHighCardinality() {
        KeywordFieldMapper.Builder builder = new KeywordFieldMapper.Builder("field", defaultIndexSettings());
        builder.docValues(FieldMapper.DocValuesParameter.Values.Cardinality.HIGH);
        MappedFieldType ft = new KeywordFieldType(
            "field",
            IndexType.docValuesOnly(),
            TextSearchInfo.SIMPLE_MATCH_ONLY,
            null,
            builder,
            true
        );
        assertEquals(
            new ScanningBinaryDocValuesPrefixQuery("field", "foo", false, false),
            ft.prefixQuery("foo", null, false, MOCK_CONTEXT)
        );
        assertEquals(new ScanningBinaryDocValuesPrefixQuery("field", "foo", true, false), ft.prefixQuery("foo", null, true, MOCK_CONTEXT));
    }

    public void testWildcardQueryHighCardinality() {
        KeywordFieldMapper.Builder builder = new KeywordFieldMapper.Builder("field", defaultIndexSettings());
        builder.docValues(FieldMapper.DocValuesParameter.Values.Cardinality.HIGH);
        MappedFieldType ft = new KeywordFieldType(
            "field",
            IndexType.docValuesOnly(),
            TextSearchInfo.SIMPLE_MATCH_ONLY,
            null,
            builder,
            true
        );
        assertEquals(new ScanningBinaryDocValuesWildcardQuery("field", "foo*", false, false), ft.wildcardQuery("foo*", null, MOCK_CONTEXT));
    }

    public void testRegexpQueryHighCardinality() {
        KeywordFieldMapper.Builder builder = new KeywordFieldMapper.Builder("field", defaultIndexSettings());
        builder.docValues(FieldMapper.DocValuesParameter.Values.Cardinality.HIGH);
        MappedFieldType ft = new KeywordFieldType(
            "field",
            IndexType.docValuesOnly(),
            TextSearchInfo.SIMPLE_MATCH_ONLY,
            null,
            builder,
            true
        );
        assertEquals(
            new ScanningBinaryDocValuesRegexpQuery("field", "foo.*", 0, 0, 10, false),
            ft.regexpQuery("foo.*", 0, 0, 10, null, MOCK_CONTEXT)
        );
    }

    public void testRegexpQueryHighCardinalityWithNormalizer() {
        Analyzer lowercaseAnalyzer = new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                Tokenizer in = new WhitespaceTokenizer();
                return new TokenStreamComponents(in, new LowerCaseFilter(in));
            }

            @Override
            protected TokenStream normalize(String fieldName, TokenStream in) {
                return new LowerCaseFilter(in);
            }
        };
        NamedAnalyzer normalizer = new NamedAnalyzer("lowercase", AnalyzerScope.INDEX, lowercaseAnalyzer);

        KeywordFieldMapper.Builder builder = new KeywordFieldMapper.Builder("field", defaultIndexSettings());
        builder.docValues(FieldMapper.DocValuesParameter.Values.Cardinality.HIGH);
        TextSearchInfo textSearchInfo = new TextSearchInfo(KeywordFieldMapper.Defaults.FIELD_TYPE, null, normalizer, normalizer);
        MappedFieldType ft = new KeywordFieldType("field", IndexType.docValuesOnly(), textSearchInfo, normalizer, builder, true);

        // The normalizer must lowercase the pattern before building the regexp query
        assertEquals(
            new ScanningBinaryDocValuesRegexpQuery("field", "foo.*", 0, 0, 10, false),
            ft.regexpQuery("FOO.*", 0, 0, 10, null, MOCK_CONTEXT)
        );
    }

    public void testRegexpQuery() {
        MappedFieldType ft = new KeywordFieldType("field");
        assertEquals(new RegexpQuery(new Term("field", "foo.*")), ft.regexpQuery("foo.*", 0, 0, 10, null, MOCK_CONTEXT));

        MappedFieldType unsearchable = new KeywordFieldType("field", false, false, Collections.emptyMap());
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> unsearchable.regexpQuery("foo.*", 0, 0, 10, null, MOCK_CONTEXT)
        );
        assertEquals("Cannot search on field [field] since it is not indexed nor has doc values.", e.getMessage());

        ElasticsearchException ee = expectThrows(
            ElasticsearchException.class,
            () -> ft.regexpQuery("foo.*", randomInt(10), 0, randomInt(10) + 1, null, MOCK_CONTEXT_DISALLOW_EXPENSIVE)
        );
        assertEquals("[regexp] queries cannot be executed when 'search.allow_expensive_queries' is set to false.", ee.getMessage());
    }

    public void testRegexpQueryDocValuesOnlyCaseInsensitive() {
        // SortedSet DV → RegexpQuery with DOC_VALUES_REWRITE and ASCII_CASE_INSENSITIVE matchFlag
        MappedFieldType ft = new KeywordFieldType("field", false, true, Map.of());
        Query q = ft.regexpQuery("foo.*", 0, RegExp.ASCII_CASE_INSENSITIVE, 10, null, MOCK_CONTEXT);
        assertThat(q, instanceOf(RegexpQuery.class));
        assertEquals(MultiTermQuery.DOC_VALUES_REWRITE, ((RegexpQuery) q).getRewriteMethod());

        // Binary DV → ScanningBinaryDocValuesRegexpQuery, which handles matchFlags via RegExp(pattern, syntaxFlags, matchFlags)
        MappedFieldType binaryFt = new KeywordFieldType("field", false, true, true, Map.of());
        q = binaryFt.regexpQuery("foo.*", 0, RegExp.ASCII_CASE_INSENSITIVE, 10, null, MOCK_CONTEXT);
        assertEquals(new ScanningBinaryDocValuesRegexpQuery("field", "foo.*", 0, RegExp.ASCII_CASE_INSENSITIVE, 10, false), q);
    }

    public void testFuzzyQuery() {
        MappedFieldType ft = new KeywordFieldType("field");
        assertEquals(
            new FuzzyQuery(new Term("field", "foo"), 2, 1, 50, true),
            ft.fuzzyQuery("foo", Fuzziness.fromEdits(2), 1, 50, true, MOCK_CONTEXT)
        );

        MappedFieldType unsearchable = new KeywordFieldType("field", false, false, Collections.emptyMap());
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> unsearchable.fuzzyQuery("foo", Fuzziness.fromEdits(2), 1, 50, true, MOCK_CONTEXT)
        );
        assertEquals("Cannot search on field [field] since it is not indexed nor has doc values.", e.getMessage());

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

    public void testNormalizeQueries() {
        MappedFieldType ft = new KeywordFieldType("field");
        assertEquals(new TermQuery(new Term("field", new BytesRef("FOO"))), ft.termQuery("FOO", null));
        ft = new KeywordFieldType("field", Lucene.STANDARD_ANALYZER);
        assertEquals(new TermQuery(new Term("field", new BytesRef("foo"))), ft.termQuery("FOO", null));
    }

    public void testFetchSourceValue() throws IOException {
        MappedFieldType mapper = new KeywordFieldMapper.Builder("field", defaultIndexSettings()).build(
            MapperBuilderContext.root(false, false)
        ).fieldType();
        assertEquals(List.of("value"), fetchSourceValue(mapper, "value"));
        assertEquals(List.of("42"), fetchSourceValue(mapper, 42L));
        assertEquals(List.of("true"), fetchSourceValue(mapper, true));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> fetchSourceValue(mapper, "value", "format"));
        assertEquals("Field [field] of type [keyword] doesn't support formats.", e.getMessage());

        MappedFieldType ignoreAboveMapper = new KeywordFieldMapper.Builder("field", defaultIndexSettings()).ignoreAbove(4)
            .build(MapperBuilderContext.root(false, false))
            .fieldType();
        assertEquals(List.of(), fetchSourceValue(ignoreAboveMapper, "value"));
        assertEquals(List.of("42"), fetchSourceValue(ignoreAboveMapper, 42L));
        assertEquals(List.of("true"), fetchSourceValue(ignoreAboveMapper, true));

        IndexMetadata indexMetadata = IndexMetadata.builder("index")
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        IndexSettings indexSettings = new IndexSettings(
            indexMetadata,
            Settings.builder().put(Mapper.SYNTHETIC_SOURCE_KEEP_INDEX_SETTING.getKey(), randomFrom("arrays", "none").toString()).build()
        );

        MappedFieldType normalizerMapper = new KeywordFieldMapper.Builder(
            "field",
            createIndexAnalyzers(),
            ScriptCompiler.NONE,
            indexSettings,
            false,
            false
        ).normalizer("lowercase").build(MapperBuilderContext.root(false, false)).fieldType();
        assertEquals(List.of("value"), fetchSourceValue(normalizerMapper, "VALUE"));
        assertEquals(List.of("42"), fetchSourceValue(normalizerMapper, 42L));
        assertEquals(List.of("value"), fetchSourceValue(normalizerMapper, "value"));

        MappedFieldType nullValueMapper = new KeywordFieldMapper.Builder("field", defaultIndexSettings()).nullValue("NULL")
            .build(MapperBuilderContext.root(false, false))
            .fieldType();
        assertEquals(List.of("NULL"), fetchSourceValue(nullValueMapper, null));
    }

    public void testGetTerms() throws IOException {
        MappedFieldType ft = new KeywordFieldType("field");
        try (Directory dir = newDirectory()) {
            RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
            for (int i = 0; i < 20; i++) {
                Document doc = new Document();
                doc.add(new StringField("field", "prefix-" + "x".repeat(i), Field.Store.NO));
                writer.addDocument(doc);
            }
            IndexReader reader = writer.getReader();
            writer.close();

            int from = randomIntBetween(1, 20);
            TermsEnum terms = ft.getTerms(reader, "prefix-" + "x".repeat(from), randomBoolean(), null);
            int numTerms = 0;
            while (terms.next() != null) {
                numTerms++;
            }
            assertEquals(20 - from, numTerms);

            terms = ft.getTerms(reader, "prefix-", randomBoolean(), "prefix-" + "x".repeat(from - 1));
            numTerms = 0;
            while (terms.next() != null) {
                numTerms++;
            }
            assertEquals(20 - from, numTerms);

            terms = ft.getTerms(reader, "prefix-" + "x".repeat(IndexWriter.MAX_TERM_LENGTH), randomBoolean(), null);
            reader.close();
        }
    }

    public void testIgnoreAboveIndexLevelSetting() {
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

        KeywordFieldMapper.Builder builder = new KeywordFieldMapper.Builder("field", mappingParserContext);

        KeywordFieldMapper.KeywordFieldType fieldType = new KeywordFieldMapper.KeywordFieldType(
            "field",
            IndexType.terms(true, true),
            new TextSearchInfo(mock(FieldType.class), null, mock(NamedAnalyzer.class), mock(NamedAnalyzer.class)),
            mock(NamedAnalyzer.class),
            builder,
            true
        );

        // when/then
        assertTrue(fieldType.ignoreAbove().isSet());
        assertEquals(123, fieldType.ignoreAbove().get());
    }

    public void testIgnoreAboveIsSetReturnsTrueWhenIgnoreAboveIsGiven() {
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

        KeywordFieldMapper.Builder builder = new KeywordFieldMapper.Builder("field", mappingParserContext);
        builder.ignoreAbove(123);

        KeywordFieldMapper.KeywordFieldType fieldType = new KeywordFieldMapper.KeywordFieldType(
            "field",
            IndexType.terms(true, true),
            new TextSearchInfo(mock(FieldType.class), null, mock(NamedAnalyzer.class), mock(NamedAnalyzer.class)),
            mock(NamedAnalyzer.class),
            builder,
            true
        );

        // when/then
        assertTrue(fieldType.ignoreAbove().isSet());
        assertEquals(123, fieldType.ignoreAbove().get());
    }

    public void testIgnoreAboveIsSetReturnsFalseWhenIgnoreAboveIsNotGiven() {
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

        KeywordFieldMapper.Builder builder = new KeywordFieldMapper.Builder("field", mappingParserContext);

        KeywordFieldMapper.KeywordFieldType fieldType = new KeywordFieldMapper.KeywordFieldType(
            "field",
            IndexType.terms(true, true),
            new TextSearchInfo(mock(FieldType.class), null, mock(NamedAnalyzer.class), mock(NamedAnalyzer.class)),
            mock(NamedAnalyzer.class),
            builder,
            true
        );

        // when/then
        assertFalse(fieldType.ignoreAbove().isSet());
        assertEquals(Mapper.IgnoreAbove.IGNORE_ABOVE_DEFAULT_VALUE, fieldType.ignoreAbove().get());
    }

    public void testIgnoreAboveIsSetReturnsFalseWhenIgnoreAboveIsGivenButItsTheSameAsDefault() {
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

        KeywordFieldMapper.Builder builder = new KeywordFieldMapper.Builder("field", mappingParserContext);
        builder.ignoreAbove(Mapper.IgnoreAbove.IGNORE_ABOVE_DEFAULT_VALUE);

        KeywordFieldMapper.KeywordFieldType fieldType = new KeywordFieldMapper.KeywordFieldType(
            "field",
            IndexType.terms(true, true),
            new TextSearchInfo(mock(FieldType.class), null, mock(NamedAnalyzer.class), mock(NamedAnalyzer.class)),
            mock(NamedAnalyzer.class),
            builder,
            true
        );

        // when/then
        assertFalse(fieldType.ignoreAbove().isSet());
        assertEquals(Mapper.IgnoreAbove.IGNORE_ABOVE_DEFAULT_VALUE, fieldType.ignoreAbove().get());
    }

    public void testIgnoreAboveIsSetReturnsFalseWhenIgnoreAboveIsGivenButItsTheSameAsDefaultForLogsdbIndices() {
        // given
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put(IndexSettings.MODE.getKey(), IndexMode.LOGSDB)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .build();
        IndexSettings indexSettings = new IndexSettings(IndexMetadata.builder("index").settings(settings).build(), settings);
        MappingParserContext mappingParserContext = mock(MappingParserContext.class);
        doReturn(settings).when(mappingParserContext).getSettings();
        doReturn(indexSettings).when(mappingParserContext).getIndexSettings();
        doReturn(mock(ScriptCompiler.class)).when(mappingParserContext).scriptCompiler();

        KeywordFieldMapper.Builder builder = new KeywordFieldMapper.Builder("field", mappingParserContext);
        builder.ignoreAbove(Mapper.IgnoreAbove.IGNORE_ABOVE_DEFAULT_VALUE_FOR_LOGSDB_INDICES);

        KeywordFieldMapper.KeywordFieldType fieldType = new KeywordFieldMapper.KeywordFieldType(
            "field",
            IndexType.terms(true, true),
            new TextSearchInfo(mock(FieldType.class), null, mock(NamedAnalyzer.class), mock(NamedAnalyzer.class)),
            mock(NamedAnalyzer.class),
            builder,
            true
        );

        // when/then
        assertFalse(fieldType.ignoreAbove().isSet());
        assertEquals(Mapper.IgnoreAbove.IGNORE_ABOVE_DEFAULT_VALUE_FOR_LOGSDB_INDICES, fieldType.ignoreAbove().get());
    }

    public void testIgnoreAboveIsSetReturnsFalseWhenIgnoreAboveIsGivenButItsTheSameAsDefaultForColumnarLogsdbIndices() {
        // given
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put(IndexSettings.MODE.getKey(), IndexMode.LOGSDB_COLUMNAR)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .build();
        IndexSettings indexSettings = new IndexSettings(IndexMetadata.builder("index").settings(settings).build(), settings);
        MappingParserContext mappingParserContext = mock(MappingParserContext.class);
        doReturn(settings).when(mappingParserContext).getSettings();
        doReturn(indexSettings).when(mappingParserContext).getIndexSettings();
        doReturn(mock(ScriptCompiler.class)).when(mappingParserContext).scriptCompiler();

        KeywordFieldMapper.Builder builder = new KeywordFieldMapper.Builder("field", mappingParserContext);
        builder.ignoreAbove(Mapper.IgnoreAbove.IGNORE_ABOVE_DEFAULT_VALUE_FOR_LOGSDB_INDICES);

        KeywordFieldMapper.KeywordFieldType fieldType = new KeywordFieldMapper.KeywordFieldType(
            "field",
            IndexType.terms(true, true),
            new TextSearchInfo(mock(FieldType.class), null, mock(NamedAnalyzer.class), mock(NamedAnalyzer.class)),
            mock(NamedAnalyzer.class),
            builder,
            true
        );

        // when/then
        assertFalse(fieldType.ignoreAbove().isSet());
        assertEquals(Mapper.IgnoreAbove.IGNORE_ABOVE_DEFAULT_VALUE_FOR_LOGSDB_INDICES, fieldType.ignoreAbove().get());
    }

    public void testIgnoreAboveIsSetReturnsTrueWhenIgnoreAboveIsGivenAsLogsdbDefaultButIndexModIsNotLogsdb() {
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

        KeywordFieldMapper.Builder builder = new KeywordFieldMapper.Builder("field", mappingParserContext);
        builder.ignoreAbove(Mapper.IgnoreAbove.IGNORE_ABOVE_DEFAULT_VALUE_FOR_LOGSDB_INDICES);

        KeywordFieldMapper.KeywordFieldType fieldType = new KeywordFieldMapper.KeywordFieldType(
            "field",
            IndexType.terms(true, true),
            new TextSearchInfo(mock(FieldType.class), null, mock(NamedAnalyzer.class), mock(NamedAnalyzer.class)),
            mock(NamedAnalyzer.class),
            builder,
            true
        );

        // when/then
        assertTrue(fieldType.ignoreAbove().isSet());
        assertEquals(Mapper.IgnoreAbove.IGNORE_ABOVE_DEFAULT_VALUE_FOR_LOGSDB_INDICES, fieldType.ignoreAbove().get());
    }

    public void testIgnoreAboveIsSetReturnsTrueWhenIgnoreAboveIsConfiguredAtIndexLevel() {
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

        KeywordFieldMapper.Builder builder = new KeywordFieldMapper.Builder("field", mappingParserContext);

        KeywordFieldMapper.KeywordFieldType fieldType = new KeywordFieldMapper.KeywordFieldType(
            "field",
            IndexType.terms(true, true),
            new TextSearchInfo(mock(FieldType.class), null, mock(NamedAnalyzer.class), mock(NamedAnalyzer.class)),
            mock(NamedAnalyzer.class),
            builder,
            true
        );

        // when/then
        assertTrue(fieldType.ignoreAbove().isSet());
        assertEquals(123, fieldType.ignoreAbove().get());
    }

    public void testIgnoreAboveIsSetReturnsFalseForNonPrimaryConstructor() {
        // given
        KeywordFieldType fieldType1 = new KeywordFieldType("field");
        KeywordFieldType fieldType2 = new KeywordFieldType("field", mock(FieldType.class), false);
        KeywordFieldType fieldType3 = new KeywordFieldType("field", true, true, Collections.emptyMap());
        KeywordFieldType fieldType4 = new KeywordFieldType("field", mock(NamedAnalyzer.class));

        // when/then
        assertFalse(fieldType1.ignoreAbove().isSet());
        assertFalse(fieldType2.ignoreAbove().isSet());
        assertFalse(fieldType3.ignoreAbove().isSet());
        assertFalse(fieldType4.ignoreAbove().isSet());
    }

    private static IndexAnalyzers createIndexAnalyzers() {
        return IndexAnalyzers.of(
            Map.of("default", new NamedAnalyzer("default", AnalyzerScope.INDEX, new StandardAnalyzer())),
            Map.ofEntries(
                Map.entry("lowercase", new NamedAnalyzer("lowercase", AnalyzerScope.INDEX, new LowercaseNormalizer())),
                Map.entry("other_lowercase", new NamedAnalyzer("other_lowercase", AnalyzerScope.INDEX, new LowercaseNormalizer()))
            ),
            Map.of(
                "lowercase",
                new NamedAnalyzer(
                    "lowercase",
                    AnalyzerScope.INDEX,
                    new CustomAnalyzer(
                        TokenizerFactory.newFactory("lowercase", WhitespaceTokenizer::new),
                        new CharFilterFactory[0],
                        new TokenFilterFactory[] { new TokenFilterFactory() {

                            @Override
                            public String name() {
                                return "lowercase";
                            }

                            @Override
                            public TokenStream create(TokenStream tokenStream) {
                                return new org.apache.lucene.analysis.core.LowerCaseFilter(tokenStream);
                            }
                        } }
                    )
                )
            )
        );
    }
}

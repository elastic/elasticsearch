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
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.unit.Fuzziness;
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
import org.elasticsearch.script.ScriptCompiler;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

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
            KeywordFieldType ft = new KeywordFieldType("field", fieldType);
            assertEquals(new FieldExistsQuery("field"), ft.existsQuery(MOCK_CONTEXT));
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
        MappedFieldType mapper = new KeywordFieldMapper.Builder("field", IndexVersion.current()).build(
            MapperBuilderContext.root(false, false)
        ).fieldType();
        assertEquals(List.of("value"), fetchSourceValue(mapper, "value"));
        assertEquals(List.of("42"), fetchSourceValue(mapper, 42L));
        assertEquals(List.of("true"), fetchSourceValue(mapper, true));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> fetchSourceValue(mapper, "value", "format"));
        assertEquals("Field [field] of type [keyword] doesn't support formats.", e.getMessage());

        MappedFieldType ignoreAboveMapper = new KeywordFieldMapper.Builder("field", IndexVersion.current()).ignoreAbove(4)
            .build(MapperBuilderContext.root(false, false))
            .fieldType();
        assertEquals(List.of(), fetchSourceValue(ignoreAboveMapper, "value"));
        assertEquals(List.of("42"), fetchSourceValue(ignoreAboveMapper, 42L));
        assertEquals(List.of("true"), fetchSourceValue(ignoreAboveMapper, true));

        MappedFieldType normalizerMapper = new KeywordFieldMapper.Builder(
            "field",
            createIndexAnalyzers(),
            ScriptCompiler.NONE,
            Integer.MAX_VALUE,
            IndexVersion.current(),
            randomFrom(Mapper.SourceKeepMode.values())
        ).normalizer("lowercase").build(MapperBuilderContext.root(false, false)).fieldType();
        assertEquals(List.of("value"), fetchSourceValue(normalizerMapper, "VALUE"));
        assertEquals(List.of("42"), fetchSourceValue(normalizerMapper, 42L));
        assertEquals(List.of("value"), fetchSourceValue(normalizerMapper, "value"));

        MappedFieldType nullValueMapper = new KeywordFieldMapper.Builder("field", IndexVersion.current()).nullValue("NULL")
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

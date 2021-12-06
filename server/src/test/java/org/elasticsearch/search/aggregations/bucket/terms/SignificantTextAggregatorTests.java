/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.terms;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.BinaryFieldMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MockFieldMapper;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.index.mapper.TextFieldMapper.TextFieldType;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.bucket.sampler.InternalSampler;
import org.elasticsearch.search.aggregations.bucket.sampler.SamplerAggregationBuilder;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.search.aggregations.AggregationBuilders.sampler;
import static org.elasticsearch.search.aggregations.AggregationBuilders.significantText;

public class SignificantTextAggregatorTests extends AggregatorTestCase {
    @Override
    protected AggregationBuilder createAggBuilderForTypeTest(MappedFieldType fieldType, String fieldName) {
        return new SignificantTextAggregationBuilder("foo", fieldName);
    }

    @Override
    protected List<ValuesSourceType> getSupportedValuesSourceTypes() {
        return List.of(CoreValuesSourceType.BOOLEAN, CoreValuesSourceType.KEYWORD);
    }

    @Override
    protected List<String> unsupportedMappedFieldTypes() {
        return List.of(
            BinaryFieldMapper.CONTENT_TYPE // binary fields are not supported because they do not have analyzers
        );
    }

    /**
     * Uses the significant text aggregation to find the keywords in text fields
     */
    public void testSignificance() throws IOException {
        TextFieldType textFieldType = new TextFieldType("text");

        IndexWriterConfig indexWriterConfig = newIndexWriterConfig(new StandardAnalyzer());
        indexWriterConfig.setMaxBufferedDocs(100);
        indexWriterConfig.setRAMBufferSizeMB(100); // flush on open to have a single segment
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, indexWriterConfig)) {
            indexDocuments(w);

            SignificantTextAggregationBuilder sigAgg = new SignificantTextAggregationBuilder("sig_text", "text").filterDuplicateText(true);
            if (randomBoolean()) {
                sigAgg.sourceFieldNames(Arrays.asList(new String[] { "json_only_field" }));
            }
            SamplerAggregationBuilder aggBuilder = new SamplerAggregationBuilder("sampler").subAggregation(sigAgg);

            try (IndexReader reader = DirectoryReader.open(w)) {
                assertEquals("test expects a single segment", 1, reader.leaves().size());
                IndexSearcher searcher = new IndexSearcher(reader);

                // Search "odd" which should have no duplication
                InternalSampler sampler = searchAndReduce(searcher, new TermQuery(new Term("text", "odd")), aggBuilder, textFieldType);
                SignificantTerms terms = sampler.getAggregations().get("sig_text");

                assertNull(terms.getBucketByKey("even"));
                assertNull(terms.getBucketByKey("duplicate"));
                assertNull(terms.getBucketByKey("common"));
                assertNotNull(terms.getBucketByKey("odd"));

                // Search "even" which will have duplication
                sampler = searchAndReduce(searcher, new TermQuery(new Term("text", "even")), aggBuilder, textFieldType);
                terms = sampler.getAggregations().get("sig_text");

                assertNull(terms.getBucketByKey("odd"));
                assertNull(terms.getBucketByKey("duplicate"));
                assertNull(terms.getBucketByKey("common"));
                assertNull(terms.getBucketByKey("separator2"));
                assertNull(terms.getBucketByKey("separator4"));
                assertNull(terms.getBucketByKey("separator6"));

                assertNotNull(terms.getBucketByKey("even"));

                assertTrue(AggregationInspectionHelper.hasValue(sampler));
            }
        }
    }

    /**
     * Uses the significant text aggregation to find the keywords in text fields and include/exclude selected terms
     */
    public void testIncludeExcludes() throws IOException {
        TextFieldType textFieldType = new TextFieldType("text");

        IndexWriterConfig indexWriterConfig = newIndexWriterConfig(new StandardAnalyzer());
        indexWriterConfig.setMaxBufferedDocs(100);
        indexWriterConfig.setRAMBufferSizeMB(100); // flush on open to have a single segment
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, indexWriterConfig)) {
            indexDocuments(w);

            String[] incExcValues = { "duplicate" };

            try (IndexReader reader = DirectoryReader.open(w)) {
                assertEquals("test expects a single segment", 1, reader.leaves().size());
                IndexSearcher searcher = new IndexSearcher(reader);

                // Inclusive of values
                {
                    SignificantTextAggregationBuilder sigAgg = new SignificantTextAggregationBuilder("sig_text", "text").includeExclude(
                        new IncludeExclude(incExcValues, null)
                    );
                    SamplerAggregationBuilder aggBuilder = new SamplerAggregationBuilder("sampler").subAggregation(sigAgg);
                    if (randomBoolean()) {
                        sigAgg.sourceFieldNames(Arrays.asList(new String[] { "json_only_field" }));
                    }
                    // Search "even" which should have duplication
                    InternalSampler sampler = searchAndReduce(searcher, new TermQuery(new Term("text", "even")), aggBuilder, textFieldType);
                    SignificantTerms terms = sampler.getAggregations().get("sig_text");

                    assertNull(terms.getBucketByKey("even"));
                    assertNotNull(terms.getBucketByKey("duplicate"));
                    assertTrue(AggregationInspectionHelper.hasValue(sampler));

                }
                // Exclusive of values
                {
                    SignificantTextAggregationBuilder sigAgg = new SignificantTextAggregationBuilder("sig_text", "text").includeExclude(
                        new IncludeExclude(null, incExcValues)
                    );
                    SamplerAggregationBuilder aggBuilder = new SamplerAggregationBuilder("sampler").subAggregation(sigAgg);
                    if (randomBoolean()) {
                        sigAgg.sourceFieldNames(Arrays.asList(new String[] { "json_only_field" }));
                    }
                    // Search "even" which should have duplication
                    InternalSampler sampler = searchAndReduce(searcher, new TermQuery(new Term("text", "even")), aggBuilder, textFieldType);
                    SignificantTerms terms = sampler.getAggregations().get("sig_text");

                    assertNotNull(terms.getBucketByKey("even"));
                    assertNull(terms.getBucketByKey("duplicate"));
                    assertTrue(AggregationInspectionHelper.hasValue(sampler));

                }
            }
        }
    }

    public void testMissingField() throws IOException {
        TextFieldType textFieldType = new TextFieldType("text");

        IndexWriterConfig indexWriterConfig = newIndexWriterConfig();
        indexWriterConfig.setMaxBufferedDocs(100);
        indexWriterConfig.setRAMBufferSizeMB(100);
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, indexWriterConfig)) {
            indexDocuments(w);

            SignificantTextAggregationBuilder sigAgg = new SignificantTextAggregationBuilder("sig_text", "this_field_does_not_exist")
                .filterDuplicateText(true);
            if (randomBoolean()) {
                sigAgg.sourceFieldNames(Arrays.asList(new String[] { "json_only_field" }));
            }
            SamplerAggregationBuilder aggBuilder = new SamplerAggregationBuilder("sampler").subAggregation(sigAgg);

            try (IndexReader reader = DirectoryReader.open(w)) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalSampler sampler = searchAndReduce(searcher, new TermQuery(new Term("text", "odd")), aggBuilder, textFieldType);
                SignificantTerms terms = sampler.getAggregations().get("sig_text");
                assertTrue(terms.getBuckets().isEmpty());
            }
        }
    }

    public void testFieldAlias() throws IOException {
        TextFieldType textFieldType = new TextFieldType("text");

        IndexWriterConfig indexWriterConfig = newIndexWriterConfig(new StandardAnalyzer());
        indexWriterConfig.setMaxBufferedDocs(100);
        indexWriterConfig.setRAMBufferSizeMB(100); // flush on open to have a single segment
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, indexWriterConfig)) {
            indexDocuments(w);

            SignificantTextAggregationBuilder agg = significantText("sig_text", "text").filterDuplicateText(true);
            SignificantTextAggregationBuilder aliasAgg = significantText("sig_text", "text-alias").filterDuplicateText(true);

            if (randomBoolean()) {
                List<String> sourceFieldNames = Arrays.asList(new String[] { "json_only_field" });
                agg.sourceFieldNames(sourceFieldNames);
                aliasAgg.sourceFieldNames(sourceFieldNames);
            }

            try (IndexReader reader = DirectoryReader.open(w)) {
                assertEquals("test expects a single segment", 1, reader.leaves().size());
                IndexSearcher searcher = new IndexSearcher(reader);

                SamplerAggregationBuilder samplerAgg = sampler("sampler").subAggregation(agg);
                SamplerAggregationBuilder aliasSamplerAgg = sampler("sampler").subAggregation(aliasAgg);

                InternalSampler sampler = searchAndReduce(searcher, new TermQuery(new Term("text", "odd")), samplerAgg, textFieldType);
                InternalSampler aliasSampler = searchAndReduce(
                    searcher,
                    new TermQuery(new Term("text", "odd")),
                    aliasSamplerAgg,
                    textFieldType
                );

                SignificantTerms terms = sampler.getAggregations().get("sig_text");
                SignificantTerms aliasTerms = aliasSampler.getAggregations().get("sig_text");
                assertFalse(terms.getBuckets().isEmpty());
                assertEquals(terms, aliasTerms);

                sampler = searchAndReduce(searcher, new TermQuery(new Term("text", "even")), samplerAgg, textFieldType);
                aliasSampler = searchAndReduce(searcher, new TermQuery(new Term("text", "even")), aliasSamplerAgg, textFieldType);

                terms = sampler.getAggregations().get("sig_text");
                aliasTerms = aliasSampler.getAggregations().get("sig_text");
                assertFalse(terms.getBuckets().isEmpty());
                assertEquals(terms, aliasTerms);

                assertTrue(AggregationInspectionHelper.hasValue(sampler));
                assertTrue(AggregationInspectionHelper.hasValue(aliasSampler));
            }
        }
    }

    public void testInsideTermsAgg() throws IOException {
        TextFieldType textFieldType = new TextFieldType("text");

        IndexWriterConfig indexWriterConfig = newIndexWriterConfig(new StandardAnalyzer());
        indexWriterConfig.setMaxBufferedDocs(100);
        indexWriterConfig.setRAMBufferSizeMB(100); // flush on open to have a single segment
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, indexWriterConfig)) {
            indexDocuments(w);

            SignificantTextAggregationBuilder sigAgg = new SignificantTextAggregationBuilder("sig_text", "text").filterDuplicateText(true);
            TermsAggregationBuilder aggBuilder = new TermsAggregationBuilder("terms").field("kwd").subAggregation(sigAgg);

            try (IndexReader reader = DirectoryReader.open(w)) {
                assertEquals("test expects a single segment", 1, reader.leaves().size());
                IndexSearcher searcher = new IndexSearcher(reader);

                StringTerms terms = searchAndReduce(searcher, new MatchAllDocsQuery(), aggBuilder, textFieldType, keywordField("kwd"));
                SignificantTerms sigOdd = terms.getBucketByKey("odd").getAggregations().get("sig_text");
                assertNull(sigOdd.getBucketByKey("even"));
                assertNull(sigOdd.getBucketByKey("duplicate"));
                assertNull(sigOdd.getBucketByKey("common"));
                assertNotNull(sigOdd.getBucketByKey("odd"));

                SignificantStringTerms sigEven = terms.getBucketByKey("even").getAggregations().get("sig_text");
                assertNull(sigEven.getBucketByKey("odd"));
                assertNull(sigEven.getBucketByKey("duplicate"));
                assertNull(sigEven.getBucketByKey("common"));
                assertNull(sigEven.getBucketByKey("separator2"));
                assertNull(sigEven.getBucketByKey("separator4"));
                assertNull(sigEven.getBucketByKey("separator6"));
                assertNotNull(sigEven.getBucketByKey("even"));
            }
        }
    }

    private void indexDocuments(IndexWriter writer) throws IOException {
        for (int i = 0; i < 10; i++) {
            Document doc = new Document();
            StringBuilder text = new StringBuilder("common ");
            if (i % 2 == 0) {
                text.append("even separator" + i + " duplicate duplicate duplicate duplicate duplicate duplicate ");
            } else {
                text.append("odd ");
            }

            doc.add(new Field("text", text.toString(), TextFieldMapper.Defaults.FIELD_TYPE));
            String json = "{ \"text\" : \"" + text.toString() + "\"," + " \"json_only_field\" : \"" + text.toString() + "\"" + " }";
            doc.add(new StoredField("_source", new BytesRef(json)));
            doc.add(new SortedSetDocValuesField("kwd", i % 2 == 0 ? new BytesRef("even") : new BytesRef("odd")));
            doc.add(new Field("kwd", i % 2 == 0 ? new BytesRef("even") : new BytesRef("odd"), KeywordFieldMapper.Defaults.FIELD_TYPE));
            writer.addDocument(doc);
        }
    }

    /**
     * Test documents with arrays of text
     */
    public void testSignificanceOnTextArrays() throws IOException {
        TextFieldType textFieldType = new TextFieldType("text");

        IndexWriterConfig indexWriterConfig = newIndexWriterConfig(new StandardAnalyzer());
        indexWriterConfig.setMaxBufferedDocs(100);
        indexWriterConfig.setRAMBufferSizeMB(100); // flush on open to have a single segment
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, indexWriterConfig)) {
            for (int i = 0; i < 10; i++) {
                Document doc = new Document();
                doc.add(new Field("text", "foo", TextFieldMapper.Defaults.FIELD_TYPE));
                String json = "{ \"text\" : [\"foo\",\"foo\"], \"title\" : [\"foo\", \"foo\"]}";
                doc.add(new StoredField("_source", new BytesRef(json)));
                w.addDocument(doc);
            }

            SignificantTextAggregationBuilder sigAgg = new SignificantTextAggregationBuilder("sig_text", "text");
            sigAgg.sourceFieldNames(Arrays.asList(new String[] { "title", "text" }));
            try (IndexReader reader = DirectoryReader.open(w)) {
                assertEquals("test expects a single segment", 1, reader.leaves().size());
                IndexSearcher searcher = new IndexSearcher(reader);
                searchAndReduce(searcher, new TermQuery(new Term("text", "foo")), sigAgg, textFieldType);
                // No significant results to be found in this test - only checking we don't end up
                // with the internal exception discovered in issue https://github.com/elastic/elasticsearch/issues/25029
            }
        }
    }

    @Override
    protected FieldMapper buildMockFieldMapper(MappedFieldType ft) {
        return new MockFieldMapper(ft, Map.of(ft.name(), ft.getTextSearchInfo().getSearchAnalyzer()));
    }
}

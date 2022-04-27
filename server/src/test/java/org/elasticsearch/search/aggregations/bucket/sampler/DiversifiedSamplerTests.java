/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket.sampler;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleDocValuesField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.search.function.FieldValueFactorFunction;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues.Doubles;
import org.elasticsearch.index.fielddata.ScriptDocValues.DoublesSupplier;
import org.elasticsearch.index.fielddata.plain.SortedDoublesIndexFieldData;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.script.field.DelegateDocValuesField;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.greaterThan;

public class DiversifiedSamplerTests extends AggregatorTestCase {

    private void writeBooks(RandomIndexWriter iw) throws IOException {
        String data[] = {
            // "id,cat,name,price,inStock,author_t,series_t,sequence_i,genre_s,genre_id",
            "0553573403,book,A Game of Thrones,7.99,true,George R.R. Martin,A Song of Ice and Fire,1,fantasy,0",
            "0553579908,book,A Clash of Kings,7.99,true,George R.R. Martin,A Song of Ice and Fire,2,fantasy,0",
            "055357342X,book,A Storm of Swords,7.99,true,George R.R. Martin,A Song of Ice and Fire,3,fantasy,0",
            "0553293354,book,Foundation,17.99,true,Isaac Asimov,Foundation Novels,1,scifi,1",
            "0812521390,book,The Black Company,6.99,false,Glen Cook,The Chronicles of The Black Company,1,fantasy,0",
            "0812550706,book,Ender's Game,6.99,true,Orson Scott Card,Ender,1,scifi,1",
            "0441385532,book,Jhereg,7.95,false,Steven Brust,Vlad Taltos,1,fantasy,0",
            "0380014300,book,Nine Princes In Amber,6.99,true,Roger Zelazny,the Chronicles of Amber,1,fantasy,0",
            "0805080481,book,The Book of Three,5.99,true,Lloyd Alexander,The Chronicles of Prydain,1,fantasy,0",
            "080508049X,book,The Black Cauldron,5.99,true,Lloyd Alexander,The Chronicles of Prydain,2,fantasy,0" };

        List<Document> docs = new ArrayList<>();
        for (String entry : data) {
            String[] parts = entry.split(",");
            Document document = new Document();
            document.add(new SortedDocValuesField("id", new BytesRef(parts[0])));
            document.add(new StringField("cat", parts[1], Field.Store.NO));
            document.add(new TextField("name", parts[2], Field.Store.NO));
            document.add(new DoubleDocValuesField("price", Double.valueOf(parts[3])));
            document.add(new StringField("inStock", parts[4], Field.Store.NO));
            document.add(new StringField("author", parts[5], Field.Store.NO));
            document.add(new StringField("series", parts[6], Field.Store.NO));
            document.add(new StringField("sequence", parts[7], Field.Store.NO));
            document.add(new SortedDocValuesField("genre", new BytesRef(parts[8])));
            document.add(new NumericDocValuesField("genre_id", Long.valueOf(parts[9])));
            docs.add(document);
        }
        /*
         * Add all documents at once to force the test to aggregate all
         * values together at the same time. *That* is required because
         * the tests assume that all books are on a shard together. And
         * they aren't always if they end up in separate leaves.
         */
        iw.addDocuments(docs);
    }

    public void testDiversifiedSampler() throws Exception {
        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
        MappedFieldType genreFieldType = new KeywordFieldMapper.KeywordFieldType("genre");
        writeBooks(indexWriter);
        indexWriter.close();
        IndexReader indexReader = DirectoryReader.open(directory);
        IndexSearcher indexSearcher = new IndexSearcher(indexReader);
        Consumer<InternalSampler> verify = result -> {
            Terms terms = result.getAggregations().get("terms");
            assertEquals(2, terms.getBuckets().size());
            assertEquals("0805080481", terms.getBuckets().get(0).getKeyAsString());
            assertEquals("0812550706", terms.getBuckets().get(1).getKeyAsString());
        };
        testCase(indexSearcher, genreFieldType, "map", verify);
        testCase(indexSearcher, genreFieldType, "global_ordinals", verify);
        testCase(indexSearcher, genreFieldType, "bytes_hash", verify);

        genreFieldType = new NumberFieldMapper.NumberFieldType("genre_id", NumberFieldMapper.NumberType.LONG);
        testCase(indexSearcher, genreFieldType, null, verify);

        // wrong field:
        genreFieldType = new KeywordFieldMapper.KeywordFieldType("wrong_field");
        testCase(indexSearcher, genreFieldType, null, result -> {
            Terms terms = result.getAggregations().get("terms");
            assertEquals(1, terms.getBuckets().size());
            assertEquals("0805080481", terms.getBuckets().get(0).getKeyAsString());
        });

        indexReader.close();
        directory.close();
    }

    public void testRidiculousSize() throws Exception {
        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
        writeBooks(indexWriter);
        indexWriter.close();
        IndexReader indexReader = DirectoryReader.open(directory);
        IndexSearcher indexSearcher = new IndexSearcher(indexReader);

        MappedFieldType genreFieldType = new KeywordFieldMapper.KeywordFieldType("genre");
        Consumer<InternalSampler> verify = result -> {
            Terms terms = result.getAggregations().get("terms");
            assertThat(terms.getBuckets().size(), greaterThan(0));
        };

        try {
            // huge shard_size
            testCase(indexSearcher, genreFieldType, "map", verify, Integer.MAX_VALUE, 1);
            testCase(indexSearcher, genreFieldType, "global_ordinals", verify, Integer.MAX_VALUE, 1);
            testCase(indexSearcher, genreFieldType, "bytes_hash", verify, Integer.MAX_VALUE, 1);

            // huge maxDocsPerValue
            testCase(indexSearcher, genreFieldType, "map", verify, 100, Integer.MAX_VALUE);
            testCase(indexSearcher, genreFieldType, "global_ordinals", verify, 100, Integer.MAX_VALUE);
            testCase(indexSearcher, genreFieldType, "bytes_hash", verify, 100, Integer.MAX_VALUE);
        } finally {
            indexReader.close();
            directory.close();
        }
    }

    private void testCase(
        IndexSearcher indexSearcher,
        MappedFieldType genreFieldType,
        String executionHint,
        Consumer<InternalSampler> verify
    ) throws IOException {
        testCase(indexSearcher, genreFieldType, executionHint, verify, 100, 1);
    }

    private void testCase(
        IndexSearcher indexSearcher,
        MappedFieldType genreFieldType,
        String executionHint,
        Consumer<InternalSampler> verify,
        int shardSize,
        int maxDocsPerValue
    ) throws IOException {
        MappedFieldType idFieldType = new KeywordFieldMapper.KeywordFieldType("id");

        SortedDoublesIndexFieldData fieldData = new SortedDoublesIndexFieldData(
            "price",
            IndexNumericFieldData.NumericType.DOUBLE,
            (dv, n) -> new DelegateDocValuesField(new Doubles(new DoublesSupplier(dv)), n)
        );
        FunctionScoreQuery query = new FunctionScoreQuery(
            new MatchAllDocsQuery(),
            new FieldValueFactorFunction("price", 1, FieldValueFactorFunction.Modifier.RECIPROCAL, null, fieldData)
        );

        DiversifiedAggregationBuilder builder = new DiversifiedAggregationBuilder("_name").field(genreFieldType.name())
            .executionHint(executionHint)
            .maxDocsPerValue(maxDocsPerValue)
            .shardSize(shardSize)
            .subAggregation(new TermsAggregationBuilder("terms").field("id"));

        InternalSampler result = searchAndReduce(indexSearcher, query, builder, genreFieldType, idFieldType);
        verify.accept(result);
    }

    public void testDiversifiedSampler_noDocs() throws Exception {
        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
        indexWriter.close();
        IndexReader indexReader = DirectoryReader.open(directory);
        IndexSearcher indexSearcher = new IndexSearcher(indexReader);

        MappedFieldType idFieldType = new KeywordFieldMapper.KeywordFieldType("id");

        MappedFieldType genreFieldType = new KeywordFieldMapper.KeywordFieldType("genre");

        DiversifiedAggregationBuilder builder = new DiversifiedAggregationBuilder("_name").field(genreFieldType.name())
            .subAggregation(new TermsAggregationBuilder("terms").field("id"));

        InternalSampler result = searchAndReduce(indexSearcher, new MatchAllDocsQuery(), builder, genreFieldType, idFieldType);
        Terms terms = result.getAggregations().get("terms");
        assertEquals(0, terms.getBuckets().size());
        indexReader.close();
        directory.close();
    }
}

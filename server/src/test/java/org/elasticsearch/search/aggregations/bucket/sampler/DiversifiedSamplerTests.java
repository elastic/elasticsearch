/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.search.function.FieldValueFactorFunction;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.plain.SortedNumericDVIndexFieldData;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;

import java.io.IOException;
import java.util.function.Consumer;

public class DiversifiedSamplerTests extends AggregatorTestCase {

    public void testDiversifiedSampler() throws Exception {
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
                "080508049X,book,The Black Cauldron,5.99,true,Lloyd Alexander,The Chronicles of Prydain,2,fantasy,0"
        };

        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
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
            indexWriter.addDocument(document);
        }

        indexWriter.close();
        IndexReader indexReader = DirectoryReader.open(directory);
        IndexSearcher indexSearcher = new IndexSearcher(indexReader);

        MappedFieldType genreFieldType = new KeywordFieldMapper.KeywordFieldType();
        genreFieldType.setName("genre");
        genreFieldType.setHasDocValues(true);
        Consumer<InternalSampler> verify = result -> {
            Terms terms = result.getAggregations().get("terms");
            assertEquals(2, terms.getBuckets().size());
            assertEquals("0805080481", terms.getBuckets().get(0).getKeyAsString());
            assertEquals("0812550706", terms.getBuckets().get(1).getKeyAsString());
        };
        testCase(indexSearcher, genreFieldType, "map", verify);
        testCase(indexSearcher, genreFieldType, "global_ordinals", verify);
        testCase(indexSearcher, genreFieldType, "bytes_hash", verify);

        genreFieldType = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.LONG);
        genreFieldType.setName("genre_id");
        testCase(indexSearcher, genreFieldType, null, verify);

        // wrong field:
        genreFieldType = new KeywordFieldMapper.KeywordFieldType();
        genreFieldType.setName("wrong_field");
        genreFieldType.setHasDocValues(true);
        testCase(indexSearcher, genreFieldType, null, result -> {
            Terms terms = result.getAggregations().get("terms");
            assertEquals(1, terms.getBuckets().size());
            assertEquals("0805080481", terms.getBuckets().get(0).getKeyAsString());
        });

        indexReader.close();
        directory.close();
    }

    private void testCase(IndexSearcher indexSearcher, MappedFieldType genreFieldType, String executionHint,
                          Consumer<InternalSampler> verify) throws IOException {
        MappedFieldType idFieldType = new KeywordFieldMapper.KeywordFieldType();
        idFieldType.setName("id");
        idFieldType.setHasDocValues(true);

        SortedNumericDVIndexFieldData fieldData = new SortedNumericDVIndexFieldData(new Index("index", "index"), "price",
                IndexNumericFieldData.NumericType.DOUBLE);
        FunctionScoreQuery query = new FunctionScoreQuery(new MatchAllDocsQuery(),
                new FieldValueFactorFunction("price", 1, FieldValueFactorFunction.Modifier.RECIPROCAL, null, fieldData));

        DiversifiedAggregationBuilder builder = new DiversifiedAggregationBuilder("_name")
                .field(genreFieldType.name())
                .executionHint(executionHint)
                .subAggregation(new TermsAggregationBuilder("terms", null).field("id"));

        InternalSampler result = search(indexSearcher, query, builder, genreFieldType, idFieldType);
        verify.accept(result);
    }

    public void testDiversifiedSampler_noDocs() throws Exception {
        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
        indexWriter.close();
        IndexReader indexReader = DirectoryReader.open(directory);
        IndexSearcher indexSearcher = new IndexSearcher(indexReader);

        MappedFieldType idFieldType = new KeywordFieldMapper.KeywordFieldType();
        idFieldType.setName("id");
        idFieldType.setHasDocValues(true);

        MappedFieldType genreFieldType = new KeywordFieldMapper.KeywordFieldType();
        genreFieldType.setName("genre");
        genreFieldType.setHasDocValues(true);

        DiversifiedAggregationBuilder builder = new DiversifiedAggregationBuilder("_name")
                .field(genreFieldType.name())
                .subAggregation(new TermsAggregationBuilder("terms", null).field("id"));

        InternalSampler result = search(indexSearcher, new MatchAllDocsQuery(), builder, genreFieldType, idFieldType);
        Terms terms = result.getAggregations().get("terms");
        assertEquals(0, terms.getBuckets().size());
        indexReader.close();
        directory.close();
    }
}

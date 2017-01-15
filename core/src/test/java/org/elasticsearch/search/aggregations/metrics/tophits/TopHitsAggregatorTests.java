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
package org.elasticsearch.search.aggregations.metrics.tophits;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.UidFieldMapper;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.sort.SortOrder;

public class TopHitsAggregatorTests extends AggregatorTestCase {

    public void testTermsAggregator() throws Exception {
        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
        Document document = new Document();
        document.add(new Field(UidFieldMapper.NAME, Uid.createUid("type", "1"), UidFieldMapper.Defaults.FIELD_TYPE));
        document.add(new SortedSetDocValuesField("string", new BytesRef("a")));
        document.add(new SortedSetDocValuesField("string", new BytesRef("b")));
        indexWriter.addDocument(document);
        document = new Document();
        document.add(new Field(UidFieldMapper.NAME, Uid.createUid("type", "2"), UidFieldMapper.Defaults.FIELD_TYPE));
        document.add(new SortedSetDocValuesField("string", new BytesRef("c")));
        document.add(new SortedSetDocValuesField("string", new BytesRef("a")));
        indexWriter.addDocument(document);
        document = new Document();
        document.add(new Field(UidFieldMapper.NAME, Uid.createUid("type", "3"), UidFieldMapper.Defaults.FIELD_TYPE));
        document.add(new SortedSetDocValuesField("string", new BytesRef("b")));
        document.add(new SortedSetDocValuesField("string", new BytesRef("d")));
        indexWriter.addDocument(document);
        indexWriter.close();

        IndexReader indexReader = DirectoryReader.open(directory);
        IndexSearcher indexSearcher = newSearcher(indexReader, true, true);

        MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType();
        fieldType.setName("string");
        fieldType.setHasDocValues(true );
        TopHitsAggregationBuilder aggregationBuilder = new TopHitsAggregationBuilder("_name");
        aggregationBuilder.sort("string", SortOrder.DESC);
        try (TopHitsAggregator aggregator = createAggregator(aggregationBuilder, fieldType, indexSearcher)){
            aggregator.preCollection();
            indexSearcher.search(new MatchAllDocsQuery(), aggregator);
            aggregator.postCollection();
            TopHits topHits = (TopHits) aggregator.buildAggregation(0L);
            SearchHits searchHits = topHits.getHits();
            assertEquals(3L, searchHits.getTotalHits());
            assertEquals("3", searchHits.getAt(0).getId());
            assertEquals("type", searchHits.getAt(0).getType());
            assertEquals("2", searchHits.getAt(1).getId());
            assertEquals("type", searchHits.getAt(1).getType());
            assertEquals("1", searchHits.getAt(2).getId());
            assertEquals("type", searchHits.getAt(2).getType());
        }
        indexReader.close();
        directory.close();
    }

}

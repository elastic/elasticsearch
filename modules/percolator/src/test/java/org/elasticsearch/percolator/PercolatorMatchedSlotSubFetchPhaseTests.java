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
package org.elasticsearch.percolator;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.stream.IntStream;

public class PercolatorMatchedSlotSubFetchPhaseTests extends ESTestCase {

    public void testHitsExecute() throws Exception {
        try (Directory directory = newDirectory()) {
            // Need a one doc index:
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                Document document = new Document();
                indexWriter.addDocument(document);
            }

            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = new IndexSearcher(reader);

                // A match:
                {
                    SearchHit[] hits = new SearchHit[]{new SearchHit(0)};
                    PercolateQuery.QueryStore queryStore = ctx -> docId -> new TermQuery(new Term("field", "value"));
                    MemoryIndex memoryIndex = new MemoryIndex();
                    memoryIndex.addField("field", "value", new WhitespaceAnalyzer());
                    memoryIndex.addField(new NumericDocValuesField(SeqNoFieldMapper.PRIMARY_TERM_NAME, 0), null);
                    PercolateQuery percolateQuery =  new PercolateQuery("_name", queryStore, Collections.emptyList(),
                        new MatchAllDocsQuery(), memoryIndex.createSearcher(), null, new MatchNoDocsQuery());

                    PercolatorMatchedSlotSubFetchPhase.innerHitsExecute(percolateQuery, indexSearcher, hits);
                    assertNotNull(hits[0].field(PercolatorMatchedSlotSubFetchPhase.FIELD_NAME_PREFIX));
                    assertEquals(0, (int) hits[0].field(PercolatorMatchedSlotSubFetchPhase.FIELD_NAME_PREFIX).getValue());
                }

                // No match:
                {
                    SearchHit[] hits = new SearchHit[]{new SearchHit(0)};
                    PercolateQuery.QueryStore queryStore = ctx -> docId -> new TermQuery(new Term("field", "value"));
                    MemoryIndex memoryIndex = new MemoryIndex();
                    memoryIndex.addField("field", "value1", new WhitespaceAnalyzer());
                    memoryIndex.addField(new NumericDocValuesField(SeqNoFieldMapper.PRIMARY_TERM_NAME, 0), null);
                    PercolateQuery percolateQuery =  new PercolateQuery("_name", queryStore, Collections.emptyList(),
                        new MatchAllDocsQuery(), memoryIndex.createSearcher(), null, new MatchNoDocsQuery());

                    PercolatorMatchedSlotSubFetchPhase.innerHitsExecute(percolateQuery, indexSearcher, hits);
                    assertNull(hits[0].field(PercolatorMatchedSlotSubFetchPhase.FIELD_NAME_PREFIX));
                }

                // No query:
                {
                    SearchHit[] hits = new SearchHit[]{new SearchHit(0)};
                    PercolateQuery.QueryStore queryStore = ctx -> docId -> null;
                    MemoryIndex memoryIndex = new MemoryIndex();
                    memoryIndex.addField("field", "value", new WhitespaceAnalyzer());
                    memoryIndex.addField(new NumericDocValuesField(SeqNoFieldMapper.PRIMARY_TERM_NAME, 0), null);
                    PercolateQuery percolateQuery =  new PercolateQuery("_name", queryStore, Collections.emptyList(),
                        new MatchAllDocsQuery(), memoryIndex.createSearcher(), null, new MatchNoDocsQuery());

                    PercolatorMatchedSlotSubFetchPhase.innerHitsExecute(percolateQuery, indexSearcher, hits);
                    assertNull(hits[0].field(PercolatorMatchedSlotSubFetchPhase.FIELD_NAME_PREFIX));
                }
            }
        }
    }

    public void testConvertTopDocsToSlots() {
        ScoreDoc[] scoreDocs = new ScoreDoc[randomInt(128)];
        for (int i = 0; i < scoreDocs.length; i++) {
            scoreDocs[i] = new ScoreDoc(i, 1f);
        }

        TopDocs topDocs = new TopDocs(new TotalHits(scoreDocs.length, TotalHits.Relation.EQUAL_TO), scoreDocs);
        IntStream stream = PercolatorMatchedSlotSubFetchPhase.convertTopDocsToSlots(topDocs, null);

        int[] result = stream.toArray();
        assertEquals(scoreDocs.length, result.length);
        for (int i = 0; i < scoreDocs.length; i++) {
            assertEquals(scoreDocs[i].doc, result[i]);
        }
    }

    public void testConvertTopDocsToSlots_nestedDocs() {
        ScoreDoc[] scoreDocs = new ScoreDoc[5];
        scoreDocs[0] = new ScoreDoc(2, 1f);
        scoreDocs[1] = new ScoreDoc(5, 1f);
        scoreDocs[2] = new ScoreDoc(8, 1f);
        scoreDocs[3] = new ScoreDoc(11, 1f);
        scoreDocs[4] = new ScoreDoc(14, 1f);
        TopDocs topDocs = new TopDocs(new TotalHits(scoreDocs.length, TotalHits.Relation.EQUAL_TO), scoreDocs);

        FixedBitSet bitSet = new FixedBitSet(15);
        bitSet.set(2);
        bitSet.set(5);
        bitSet.set(8);
        bitSet.set(11);
        bitSet.set(14);

        int[] rootDocsBySlot = PercolatorMatchedSlotSubFetchPhase.buildRootDocsSlots(bitSet);
        int[] result = PercolatorMatchedSlotSubFetchPhase.convertTopDocsToSlots(topDocs, rootDocsBySlot).toArray();
        assertEquals(scoreDocs.length, result.length);
        assertEquals(0, result[0]);
        assertEquals(1, result[1]);
        assertEquals(2, result[2]);
        assertEquals(3, result[3]);
        assertEquals(4, result[4]);
    }

}

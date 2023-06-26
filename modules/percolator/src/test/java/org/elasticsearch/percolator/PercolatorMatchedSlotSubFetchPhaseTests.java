/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.percolator;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.FetchContext;
import org.elasticsearch.search.fetch.FetchSubPhase.HitContext;
import org.elasticsearch.search.fetch.FetchSubPhaseProcessor;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.Map;
import java.util.stream.IntStream;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PercolatorMatchedSlotSubFetchPhaseTests extends ESTestCase {

    public void testHitsExecute() throws Exception {
        try (Directory directory = newDirectory()) {
            // Need a one doc index:
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                Document document = new Document();
                indexWriter.addDocument(document);
            }

            PercolatorMatchedSlotSubFetchPhase phase = new PercolatorMatchedSlotSubFetchPhase();

            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                LeafReaderContext context = reader.leaves().get(0);
                // A match:
                {
                    HitContext hit = new HitContext(new SearchHit(0), context, 0, Map.of(), Source.empty(null));
                    PercolateQuery.QueryStore queryStore = ctx -> docId -> new TermQuery(new Term("field", "value"));
                    MemoryIndex memoryIndex = new MemoryIndex();
                    memoryIndex.addField("field", "value", new WhitespaceAnalyzer());
                    memoryIndex.addField(new NumericDocValuesField(SeqNoFieldMapper.PRIMARY_TERM_NAME, 0), null);
                    PercolateQuery percolateQuery = new PercolateQuery(
                        "_name",
                        queryStore,
                        Collections.emptyList(),
                        new MatchAllDocsQuery(),
                        memoryIndex.createSearcher(),
                        null,
                        new MatchNoDocsQuery()
                    );

                    FetchContext sc = mock(FetchContext.class);
                    when(sc.query()).thenReturn(percolateQuery);
                    SearchExecutionContext sec = mock(SearchExecutionContext.class);
                    when(sc.getSearchExecutionContext()).thenReturn(sec);
                    when(sec.indexVersionCreated()).thenReturn(IndexVersion.CURRENT);

                    FetchSubPhaseProcessor processor = phase.getProcessor(sc);
                    assertNotNull(processor);
                    processor.process(hit);

                    assertNotNull(hit.hit().field(PercolatorMatchedSlotSubFetchPhase.FIELD_NAME_PREFIX));
                    assertEquals(0, (int) hit.hit().field(PercolatorMatchedSlotSubFetchPhase.FIELD_NAME_PREFIX).getValue());
                }

                // No match:
                {
                    HitContext hit = new HitContext(new SearchHit(0), context, 0, Map.of(), Source.empty(null));
                    PercolateQuery.QueryStore queryStore = ctx -> docId -> new TermQuery(new Term("field", "value"));
                    MemoryIndex memoryIndex = new MemoryIndex();
                    memoryIndex.addField("field", "value1", new WhitespaceAnalyzer());
                    memoryIndex.addField(new NumericDocValuesField(SeqNoFieldMapper.PRIMARY_TERM_NAME, 0), null);
                    PercolateQuery percolateQuery = new PercolateQuery(
                        "_name",
                        queryStore,
                        Collections.emptyList(),
                        new MatchAllDocsQuery(),
                        memoryIndex.createSearcher(),
                        null,
                        new MatchNoDocsQuery()
                    );

                    FetchContext sc = mock(FetchContext.class);
                    when(sc.query()).thenReturn(percolateQuery);
                    SearchExecutionContext sec = mock(SearchExecutionContext.class);
                    when(sc.getSearchExecutionContext()).thenReturn(sec);
                    when(sec.indexVersionCreated()).thenReturn(IndexVersion.CURRENT);

                    FetchSubPhaseProcessor processor = phase.getProcessor(sc);
                    assertNotNull(processor);
                    processor.process(hit);

                    assertNull(hit.hit().field(PercolatorMatchedSlotSubFetchPhase.FIELD_NAME_PREFIX));
                }

                // No query:
                {
                    HitContext hit = new HitContext(new SearchHit(0), context, 0, Map.of(), Source.empty(null));
                    PercolateQuery.QueryStore queryStore = ctx -> docId -> null;
                    MemoryIndex memoryIndex = new MemoryIndex();
                    memoryIndex.addField("field", "value", new WhitespaceAnalyzer());
                    memoryIndex.addField(new NumericDocValuesField(SeqNoFieldMapper.PRIMARY_TERM_NAME, 0), null);
                    PercolateQuery percolateQuery = new PercolateQuery(
                        "_name",
                        queryStore,
                        Collections.emptyList(),
                        new MatchAllDocsQuery(),
                        memoryIndex.createSearcher(),
                        null,
                        new MatchNoDocsQuery()
                    );

                    FetchContext sc = mock(FetchContext.class);
                    when(sc.query()).thenReturn(percolateQuery);
                    SearchExecutionContext sec = mock(SearchExecutionContext.class);
                    when(sc.getSearchExecutionContext()).thenReturn(sec);
                    when(sec.indexVersionCreated()).thenReturn(IndexVersion.CURRENT);

                    FetchSubPhaseProcessor processor = phase.getProcessor(sc);
                    assertNotNull(processor);
                    processor.process(hit);

                    assertNull(hit.hit().field(PercolatorMatchedSlotSubFetchPhase.FIELD_NAME_PREFIX));
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

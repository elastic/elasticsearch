/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.geo.ShapeRelation;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class SeqNoFieldMapperTests extends MapperServiceTestCase {
    private static final int MAX_SEQ_NO = 10_000;

    public void testTermQuery() throws IOException {
        testCase((ft, searcher) -> {
            int seqNo = randomInt(MAX_SEQ_NO);
            TopFieldDocs docs = docs(searcher, ft.termQuery(seqNo, null), 10);
            assertThat(docs.totalHits, equalTo(new TotalHits(1, TotalHits.Relation.EQUAL_TO)));
            assertThat(((FieldDoc) docs.scoreDocs[0]).fields[0], equalTo((long) seqNo));
        });
    }

    public void testExactQuery() throws IOException {
        testCase((ft, searcher) -> {
            int seqNo = randomInt(MAX_SEQ_NO);
            TopFieldDocs docs = docs(searcher, ft.exactQuery(seqNo), 10);
            assertThat(docs.totalHits, equalTo(new TotalHits(1, TotalHits.Relation.EQUAL_TO)));
            assertThat(((FieldDoc) docs.scoreDocs[0]).fields[0], equalTo((long) seqNo));
        });
    }

    public void testRangeQuery() throws IOException {
        testCase((ft, searcher) -> {
            int min = randomInt(MAX_SEQ_NO - 1);
            int max = randomIntBetween(min, MAX_SEQ_NO);
            TopFieldDocs docs = docs(
                searcher,
                ft.rangeQuery(min, max, true, true, ShapeRelation.INTERSECTS, null, null, null),
                max - min + 100
            );
            assertThat(docs.totalHits, equalTo(new TotalHits(max - min + 1, TotalHits.Relation.EQUAL_TO)));
            for (int i = 0; i < docs.scoreDocs.length; i++) {
                assertThat(((FieldDoc) docs.scoreDocs[i]).fields[0], equalTo((long) (min + i)));
            }
        });
    }

    public void testNativeRangeQuery() throws IOException {
        testCase((ft, searcher) -> {
            int min = randomInt(MAX_SEQ_NO - 1);
            int max = randomIntBetween(min, MAX_SEQ_NO);
            TopFieldDocs docs = docs(searcher, ft.rangeQuery(min, max), max - min + 100);
            assertThat(docs.totalHits, equalTo(new TotalHits(max - min + 1, TotalHits.Relation.EQUAL_TO)));
            for (int i = 0; i < docs.scoreDocs.length; i++) {
                assertThat(((FieldDoc) docs.scoreDocs[i]).fields[0], equalTo((long) (min + i)));
            }
        });
    }

    private void testCase(CheckedBiConsumer<SeqNoFieldMapper.SeqNoFieldType, IndexSearcher, IOException> c) throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {}));
        try (Directory dir = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), dir)) {
            for (int seqNo = 0; seqNo <= MAX_SEQ_NO; seqNo++) {
                ParsedDocument parsed = mapperService.documentParser().parseDocument(source("{}"), mapperService.mappingLookup());
                parsed.updateSeqID(seqNo, 0);
                iw.addDocuments(parsed.docs());
            }
            try (IndexReader reader = iw.getReader()) {
                c.accept(
                    (SeqNoFieldMapper.SeqNoFieldType) mapperService.mappingLookup().getFieldType(SeqNoFieldMapper.NAME),
                    new IndexSearcher(reader)
                );
            }
        }
    }

    private TopFieldDocs docs(IndexSearcher searcher, Query query, int n) throws IOException {
        return searcher.search(query, n, new Sort(new SortField(SeqNoFieldMapper.NAME, SortField.Type.LONG)));
    }
}

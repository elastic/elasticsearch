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
import org.elasticsearch.Version;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.in;

public class SeqNoFieldMapperTests extends MapperServiceTestCase {
    private static final int MAX_SEQ_NO = 10_000;

    public void testTermQuery() throws IOException {
        testCase((ft, context) -> {
            int seqNo = randomInt(MAX_SEQ_NO);
            TopFieldDocs docs = docs(context, ft.termQuery(seqNo, context), 10);
            assertThat(docs.totalHits, equalTo(new TotalHits(1, TotalHits.Relation.EQUAL_TO)));
            assertThat(((FieldDoc) docs.scoreDocs[0]).fields[0], equalTo((long) seqNo));
        });
    }

    public void testExactQuery() throws IOException {
        testCase((ft, context) -> {
            int seqNo = randomInt(MAX_SEQ_NO);
            TopFieldDocs docs = docs(context, ft.exactQuery(context.indexVersionCreated(), seqNo), 10);
            assertThat(docs.totalHits, equalTo(new TotalHits(1, TotalHits.Relation.EQUAL_TO)));
            assertThat(((FieldDoc) docs.scoreDocs[0]).fields[0], equalTo((long) seqNo));
        });
    }

    public void testTermsQuery() throws IOException {
        testCase((ft, context) -> {
            int size = between(2, (int) (MAX_SEQ_NO * 0.05));
            Set<Integer> seqNos = new HashSet<>();
            while (seqNos.size() < size) {
                seqNos.add(randomInt(MAX_SEQ_NO));
            }
            TopFieldDocs docs = docs(context, ft.termsQuery(seqNos, context), size + 100);
            assertThat(docs.totalHits, equalTo(new TotalHits(size, TotalHits.Relation.EQUAL_TO)));
            for (int i = 0; i < size; i++) {
                assertThat(((Long) ((FieldDoc) docs.scoreDocs[i]).fields[0]).intValue(), in(seqNos));
            }
        });
    }

    public void testRangeQuery() throws IOException {
        testCase((ft, context) -> {
            int min = randomInt(MAX_SEQ_NO - 1);
            int max = randomIntBetween(min, MAX_SEQ_NO);
            TopFieldDocs docs = docs(
                context,
                ft.rangeQuery(min, max, true, true, ShapeRelation.INTERSECTS, null, null, context),
                max - min + 100
            );
            assertThat(docs.totalHits, equalTo(new TotalHits(max - min + 1, TotalHits.Relation.EQUAL_TO)));
            for (int i = 0; i < docs.scoreDocs.length; i++) {
                assertThat(((FieldDoc) docs.scoreDocs[i]).fields[0], equalTo((long) (min + i)));
            }
        });
    }

    public void testNativeRangeQuery() throws IOException {
        testCase((ft, context) -> {
            int min = randomInt(MAX_SEQ_NO - 1);
            int max = randomIntBetween(min, MAX_SEQ_NO);
            TopFieldDocs docs = docs(context, ft.rangeQuery(context.indexVersionCreated(), min, max), max - min + 100);
            assertThat(docs.totalHits, equalTo(new TotalHits(max - min + 1, TotalHits.Relation.EQUAL_TO)));
            for (int i = 0; i < docs.scoreDocs.length; i++) {
                assertThat(((FieldDoc) docs.scoreDocs[i]).fields[0], equalTo((long) (min + i)));
            }
        });
    }

    private void testCase(CheckedBiConsumer<SeqNoFieldMapper.SeqNoFieldType, SearchExecutionContext, IOException> c) throws IOException {
        MapperService mapperService = createMapperService(randomBoolean() ? Version.CURRENT : Version.V_8_3_0, mapping(b -> {}));
        try (Directory dir = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), dir)) {
            for (int seqNo = 0; seqNo <= MAX_SEQ_NO; seqNo++) {
                ParsedDocument parsed = mapperService.documentParser().parseDocument(source("{}"), mapperService.mappingLookup());
                parsed.updateSeqID(seqNo, 0);
                iw.addDocuments(parsed.docs());
            }
            try (IndexReader reader = iw.getReader()) {
                c.accept(
                    (SeqNoFieldMapper.SeqNoFieldType) mapperService.mappingLookup().getFieldType(SeqNoFieldMapper.NAME),
                    createSearchExecutionContext(mapperService, new IndexSearcher(reader))
                );
            }
        }
    }

    private TopFieldDocs docs(SearchExecutionContext context, Query query, int n) throws IOException {
        return context.searcher().search(query, n, new Sort(new SortField(SeqNoFieldMapper.NAME, SortField.Type.LONG)));
    }
}

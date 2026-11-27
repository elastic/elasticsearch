/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.query;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.codec.tsdb.PrefixedPartitionsWriter;
import org.elasticsearch.index.codec.tsdb.es95.ES95TSDBDocValuesFormatFactory;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class TsidPrefixRangeQueryTests extends ESTestCase {

    public void testMatchesPrefixRange() throws IOException {
        final List<byte[]> tsids = randomTsids();
        try (Directory directory = newDirectory(); IndexWriter writer = new IndexWriter(directory, config(true))) {
            for (int i = 0; i < tsids.size(); i++) {
                final Document doc = new Document();
                doc.add(new SortedDocValuesField(TimeSeriesIdFieldMapper.NAME, new BytesRef(tsids.get(i))));
                doc.add(new StoredField("id", i));
                writer.addDocument(doc);
                if (rarely()) {
                    writer.flush();
                }
            }
            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                final IndexSearcher searcher = newSearcher(reader, false);
                for (int iteration = 0; iteration < 20; iteration++) {
                    final int prefixBits = between(1, PrefixedPartitionsWriter.PARTITION_PREFIX_BITS);
                    final int lower = between(0, (1 << prefixBits) - 1);
                    final TsidPrefixRange range = new TsidPrefixRange(prefixBits, lower, between(lower + 1, 1 << prefixBits));
                    final Set<Integer> expected = new HashSet<>();
                    for (int i = 0; i < tsids.size(); i++) {
                        final int prefix = prefix(tsids.get(i), prefixBits);
                        if (prefix >= range.lower() && prefix < range.upper()) {
                            expected.add(i);
                        }
                    }
                    final TsidPrefixRangeQuery query = new TsidPrefixRangeQuery(range);
                    final ScoreDoc[] hits = searcher.search(query, tsids.size() + 1).scoreDocs;
                    final Set<Integer> actual = new HashSet<>();
                    for (ScoreDoc hit : hits) {
                        actual.add(searcher.storedFields().document(hit.doc).getField("id").numericValue().intValue());
                    }
                    assertThat("range " + range, actual, equalTo(expected));

                }
            }
        }
    }

    public void testNeedsFineEnoughPartitions() throws IOException {
        for (boolean writePartitions : new boolean[] { false, true }) {
            try (Directory directory = newDirectory(); IndexWriter writer = new IndexWriter(directory, config(writePartitions))) {
                final Document doc = new Document();
                doc.add(new SortedDocValuesField(TimeSeriesIdFieldMapper.NAME, new BytesRef(new byte[] { 1, 2, 3, 4 })));
                writer.addDocument(doc);
                try (DirectoryReader reader = DirectoryReader.open(writer)) {
                    final int prefixBits = writePartitions ? PrefixedPartitionsWriter.PARTITION_PREFIX_BITS + 1 : 8;
                    final TsidPrefixRangeQuery query = new TsidPrefixRangeQuery(new TsidPrefixRange(prefixBits, 0, 1));
                    expectThrows(IllegalStateException.class, () -> newSearcher(reader, false).count(query));
                }
            }
        }
    }

    public void testFindsOnlyRequiredClauses() {
        final TsidPrefixRange range = new TsidPrefixRange(8, 3, 9);
        final TsidPrefixRangeQuery query = new TsidPrefixRangeQuery(range);
        assertThat(TsidPrefixRangeQuery.find(query), equalTo(range));
        assertThat(TsidPrefixRangeQuery.find(new ConstantScoreQuery(new BoostQuery(query, 2f))), equalTo(range));
        for (BooleanClause.Occur occur : BooleanClause.Occur.values()) {
            final Query bool = new BooleanQuery.Builder().add(MatchAllDocsQuery.INSTANCE, BooleanClause.Occur.FILTER)
                .add(query, occur)
                .build();
            final boolean required = occur == BooleanClause.Occur.MUST || occur == BooleanClause.Occur.FILTER;
            assertThat(occur.toString(), TsidPrefixRangeQuery.find(bool), equalTo(required ? range : null));
        }
        assertNull(TsidPrefixRangeQuery.find(MatchAllDocsQuery.INSTANCE));
    }

    public void testEqualsAndHashCode() {
        final TsidPrefixRangeQuery query = new TsidPrefixRangeQuery(new TsidPrefixRange(8, 3, 9));
        assertThat(query, equalTo(new TsidPrefixRangeQuery(new TsidPrefixRange(8, 3, 9))));
        assertThat(query.hashCode(), equalTo(new TsidPrefixRangeQuery(new TsidPrefixRange(8, 3, 9)).hashCode()));
        assertNotEquals(query, new TsidPrefixRangeQuery(new TsidPrefixRange(8, 3, 10)));
    }

    public void testInvalidRange() {
        expectThrows(IllegalArgumentException.class, () -> new TsidPrefixRange(0, 0, 1));
        expectThrows(IllegalArgumentException.class, () -> new TsidPrefixRange(31, 0, 1));
        expectThrows(IllegalArgumentException.class, () -> new TsidPrefixRange(8, 3, 3));
        expectThrows(IllegalArgumentException.class, () -> new TsidPrefixRange(8, -1, 3));
        expectThrows(IllegalArgumentException.class, () -> new TsidPrefixRange(8, 0, 257));
    }

    private static IndexWriterConfig config(boolean writePartitions) {
        return new IndexWriterConfig().setCodec(
            TestUtil.alwaysDocValuesFormat(ES95TSDBDocValuesFormatFactory.create(randomBoolean(), randomBoolean(), writePartitions, null))
        ).setIndexSort(new Sort(new SortField(TimeSeriesIdFieldMapper.NAME, SortField.Type.STRING)));
    }

    private static List<byte[]> randomTsids() {
        final byte[] firstBytes = new byte[between(1, 4)];
        for (int i = 0; i < firstBytes.length; i++) {
            firstBytes[i] = randomByte();
        }
        final List<byte[]> tsids = new ArrayList<>();
        for (int s = between(1, 300); s > 0; s--) {
            final byte[] tsid = randomByteArrayOfLength(between(4, 8));
            tsid[0] = firstBytes[between(0, firstBytes.length - 1)];
            for (int d = between(1, 20); d > 0; d--) {
                tsids.add(tsid);
            }
        }
        return tsids;
    }

    private static int prefix(byte[] tsid, int prefixBits) {
        long value = 0;
        for (int i = 0; i < Integer.BYTES; i++) {
            value = (value << Byte.SIZE) | (tsid[i] & 0xFF);
        }
        return (int) (value >>> (Integer.SIZE - prefixBits));
    }
}

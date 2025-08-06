/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.read;

import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.index.codec.tsdb.es819.ES819TSDBDocValuesFormat;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.elasticsearch.indices.CrankyCircuitBreakerService;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.equalTo;

public class TSIDOrdinalsBuilderTests extends ComputeTestCase {

    public void testReader() throws IOException {
        testRead(blockFactory());
    }

    public void testReadWithCranky() throws IOException {
        var factory = crankyBlockFactory();
        try {
            testRead(factory);
            // If we made it this far cranky didn't fail us!
        } catch (CircuitBreakingException e) {
            logger.info("cranky", e);
            assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
        assertThat(factory.breaker().getUsed(), equalTo(0L));
    }

    private void testRead(BlockFactory factory) throws IOException {
        BytesRef[] values = new BytesRef[] { new BytesRef("a"), new BytesRef("b"), new BytesRef("c"), new BytesRef("d") };

        int count = 1000;
        try (Directory directory = newDirectory()) {
            try (IndexWriter indexWriter = createIndexWriter(directory)) {
                for (int i = 0; i < count; i++) {
                    BytesRef v = values[i % values.length];
                    indexWriter.addDocument(List.of(new SortedDocValuesField("_tsid", v)));
                }
            }
            Map<String, Integer> counts = new HashMap<>();
            try (IndexReader reader = DirectoryReader.open(directory)) {
                for (LeafReaderContext ctx : reader.leaves()) {
                    SortedDocValues docValues = ctx.reader().getSortedDocValues("_tsid");
                    try (TSIDOrdinalsBuilder builder = new TSIDOrdinalsBuilder(factory, docValues, ctx.reader().numDocs())) {
                        for (int i = 0; i < ctx.reader().maxDoc(); i++) {
                            assertThat(docValues.advanceExact(i), equalTo(true));
                            int ord = docValues.ordValue();
                            builder.appendOrd(ord);
                        }
                        try (BytesRefBlock build = builder.build()) {
                            for (int i = 0; i < build.getPositionCount(); i++) {
                                String key = build.getBytesRef(i, new BytesRef()).utf8ToString();
                                counts.merge(key, 1, Integer::sum);
                            }
                        }
                    }
                }
            }
            int expectedCount = count / values.length;
            assertMap(
                counts,
                matchesMap().entry("a", expectedCount).entry("b", expectedCount).entry("c", expectedCount).entry("d", expectedCount)
            );
        }
    }

    public void testHighCardinality() throws IOException {
        int count = 1_000;
        try (Directory directory = newDirectory()) {
            try (IndexWriter indexWriter = createIndexWriter(directory)) {
                for (int i = 0; i < count; i++) {
                    BytesRef tsid = new BytesRef(String.format(Locale.ROOT, "%04d", i));
                    indexWriter.addDocument(List.of(new SortedDocValuesField("_tsid", tsid)));
                }
                indexWriter.forceMerge(1);
            }
            try (IndexReader reader = DirectoryReader.open(directory)) {
                assertThat(reader.leaves().size(), equalTo(1));
                LeafReader leafReader = reader.leaves().get(0).reader();
                SortedDocValues docValues = leafReader.getSortedDocValues("_tsid");
                int offset = 850;
                try (TSIDOrdinalsBuilder builder = new TSIDOrdinalsBuilder(blockFactory(), docValues, count - offset)) {
                    for (int i = offset; i < leafReader.maxDoc(); i++) {
                        assertThat(docValues.advanceExact(i), equalTo(true));
                        int ord = docValues.ordValue();
                        builder.appendOrd(ord);
                    }
                    try (BytesRefBlock build = builder.build()) {
                        assertThat(build.getPositionCount(), equalTo(count - offset));
                        for (int i = 0; i < build.getPositionCount(); i++) {
                            String key = build.getBytesRef(i, new BytesRef()).utf8ToString();
                            assertThat(key, equalTo(String.format(Locale.ROOT, "%04d", offset + i)));
                        }
                    }
                }
            }
        }
    }

    static IndexWriter createIndexWriter(Directory directory) throws IOException {
        IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
        iwc.setLeafSorter(DataStream.TIMESERIES_LEAF_READERS_SORTER);
        iwc.setIndexSort(new Sort(new SortField(TimeSeriesIdFieldMapper.NAME, SortField.Type.STRING, false)));
        iwc.setCodec(TestUtil.alwaysDocValuesFormat(new ES819TSDBDocValuesFormat()));
        return new IndexWriter(directory, iwc);
    }

}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.bucket.composite;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortedSetSortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.index.mapper.IpFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.search.aggregations.AggregatorTestCase;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Correctness gate for {@link SegmentOrdinalValuesSource} over random multi-segment data. A composite paginated to
 * exhaustion in small pages must yield exactly the same buckets (key + doc_count), in the same order, as the same
 * composite returned in a single large page. The single-page run never fills its queue, so it neither paginates with an
 * after-key nor dynamically prunes - it is the straightforward reference path. The paginated run exercises the bounded
 * queue, per-segment slot remapping, after-key resumption, and dynamic pruning, so any divergence pins a bug in those.
 * The matrix covers the segment source as a leading and non-leading source, over indexed keyword, doc-values-only
 * (non-indexed) keyword, and IP fields, across asc/desc {@code _key}, optional missing buckets, and small page sizes.
 */
public class SegmentOrdinalCompositeEqualityTests extends AggregatorTestCase {

    // Larger than the number of distinct composite keys any test below can produce, so a single page holds them all.
    private static final int SINGLE_PAGE_SIZE = 10_000;

    private enum FieldKind {
        KEYWORD_INDEXED,
        KEYWORD_DOC_VALUES,
        IP,
        LONG
    }

    private record FieldSpec(String name, FieldKind kind) {
        MappedFieldType fieldType() {
            return switch (kind) {
                case KEYWORD_INDEXED -> new KeywordFieldMapper.KeywordFieldType(name);
                case KEYWORD_DOC_VALUES -> new KeywordFieldMapper.KeywordFieldType(name, false, true, Map.of());
                case IP -> new IpFieldMapper.IpFieldType(name);
                case LONG -> new NumberFieldMapper.NumberFieldType(name, NumberFieldMapper.NumberType.LONG);
            };
        }
    }

    public void testSingleIndexedKeyword() throws Exception {
        runRandomized(List.of(new FieldSpec("kw", FieldKind.KEYWORD_INDEXED)));
    }

    public void testSingleDocValuesOnlyKeyword() throws Exception {
        // No inverted index => no dynamic pruning; verifies the plain segment-ordinal remap path.
        runRandomized(List.of(new FieldSpec("kw", FieldKind.KEYWORD_DOC_VALUES)));
    }

    public void testSingleIp() throws Exception {
        // IP has ordinals (sorted doc values) but no terms => segment source without dynamic pruning.
        runRandomized(List.of(new FieldSpec("ip", FieldKind.IP)));
    }

    public void testMultiKeyword() throws Exception {
        // Segment source is both leading (kw1, prunes) and non-leading (kw2, no prune).
        runRandomized(List.of(new FieldSpec("kw1", FieldKind.KEYWORD_INDEXED), new FieldSpec("kw2", FieldKind.KEYWORD_INDEXED)));
    }

    public void testKeywordLeadingThenIp() throws Exception {
        runRandomized(List.of(new FieldSpec("kw", FieldKind.KEYWORD_INDEXED), new FieldSpec("ip", FieldKind.IP)));
    }

    public void testLongLeadingThenKeyword() throws Exception {
        // Leading source is numeric (not a segment source), so the keyword segment source is exercised non-leading.
        runRandomized(List.of(new FieldSpec("lng", FieldKind.LONG), new FieldSpec("kw", FieldKind.KEYWORD_INDEXED)));
    }

    public void testIndexSortedKeyword() throws Exception {
        // The index is sorted ascending on the keyword, congruent with the composite leading source, so the composite
        // takes the index-sort early-termination path (processLeafFromQuery).
        final MappedFieldType[] fieldTypes = { new KeywordFieldMapper.KeywordFieldType("kw") };
        final List<FieldSpec> sources = List.of(new FieldSpec("kw", FieldKind.KEYWORD_INDEXED));
        for (int iter = 0; iter < 25; iter++) {
            final int numSegments = randomIntBetween(1, 4);
            final int cardinality = randomIntBetween(1, 20);
            final int size = randomIntBetween(1, 12);
            final Sort indexSort = new Sort(new SortedSetSortField("kw", false));
            assertPaginationConsistent(sources, fieldTypes, indexSort, numSegments, cardinality, true, false, size);
        }
    }

    private void runRandomized(List<FieldSpec> sources) throws IOException {
        final MappedFieldType[] fieldTypes = sources.stream().map(FieldSpec::fieldType).toArray(MappedFieldType[]::new);
        for (int iter = 0; iter < 25; iter++) {
            final int numSegments = randomIntBetween(1, 5);
            final int cardinality = randomIntBetween(1, 30);
            final boolean asc = randomBoolean();
            final boolean missing = randomBoolean();
            final int size = randomIntBetween(1, 20);
            assertPaginationConsistent(sources, fieldTypes, null, numSegments, cardinality, asc, missing, size);
        }
    }

    private void assertPaginationConsistent(
        List<FieldSpec> sources,
        MappedFieldType[] fieldTypes,
        Sort indexSort,
        int numSegments,
        int cardinality,
        boolean asc,
        boolean missing,
        int size
    ) throws IOException {
        try (Directory dir = newDirectory()) {
            final IndexWriterConfig cfg = newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE);
            if (indexSort != null) {
                cfg.setIndexSort(indexSort);
            }
            try (RandomIndexWriter w = new RandomIndexWriter(random(), dir, cfg)) {
                for (int s = 0; s < numSegments; s++) {
                    final int docsInSeg = randomIntBetween(1, 30);
                    for (int d = 0; d < docsInSeg; d++) {
                        final Document doc = new Document();
                        for (FieldSpec src : sources) {
                            if (missing && rarely()) {
                                continue; // omit this source's value to exercise the missing bucket
                            }
                            addRandomValue(doc, src, cardinality);
                        }
                        w.addDocument(doc);
                    }
                    w.commit(); // force a new segment per batch
                }
                try (DirectoryReader reader = w.getReader()) {
                    final List<String> paged = collectAllPages(reader, fieldTypes, sources, asc, missing, size);
                    final List<String> singlePage = collectAllPages(reader, fieldTypes, sources, asc, missing, SINGLE_PAGE_SIZE);
                    assertEquals(
                        "sources="
                            + sources
                            + " numSegments="
                            + numSegments
                            + " cardinality="
                            + cardinality
                            + " asc="
                            + asc
                            + " missing="
                            + missing
                            + " size="
                            + size,
                        singlePage,
                        paged
                    );
                }
            }
        }
    }

    private void addRandomValue(Document doc, FieldSpec src, int cardinality) {
        switch (src.kind()) {
            case KEYWORD_INDEXED -> {
                final BytesRef term = new BytesRef(String.format(Locale.ROOT, "%05d", randomInt(cardinality - 1)));
                doc.add(new SortedSetDocValuesField(src.name(), term));
                doc.add(new StringField(src.name(), term, Field.Store.NO));
            }
            case KEYWORD_DOC_VALUES -> {
                final BytesRef term = new BytesRef(String.format(Locale.ROOT, "%05d", randomInt(cardinality - 1)));
                doc.add(new SortedSetDocValuesField(src.name(), term)); // doc values only, no inverted index
            }
            case IP -> {
                final InetAddress ip = InetAddresses.forString("10.0.0." + (randomInt(cardinality - 1) % 256));
                doc.add(new SortedSetDocValuesField(src.name(), new BytesRef(InetAddressPoint.encode(ip))));
                doc.add(new InetAddressPoint(src.name(), ip));
            }
            case LONG -> {
                final long value = randomInt(cardinality - 1);
                doc.add(new SortedNumericDocValuesField(src.name(), value));
                doc.add(new LongPoint(src.name(), value));
            }
        }
    }

    private List<String> collectAllPages(
        DirectoryReader reader,
        MappedFieldType[] fieldTypes,
        List<FieldSpec> sources,
        boolean asc,
        boolean missing,
        int size
    ) throws IOException {
        final List<String> out = new ArrayList<>();
        Map<String, Object> after = null;
        for (int guard = 0; guard < 100_000; guard++) {
            final List<CompositeValuesSourceBuilder<?>> sourceBuilders = new ArrayList<>();
            for (FieldSpec src : sources) {
                sourceBuilders.add(
                    new TermsValuesSourceBuilder(src.name()).field(src.name()).order(asc ? "asc" : "desc").missingBucket(missing)
                );
            }
            final CompositeAggregationBuilder builder = new CompositeAggregationBuilder("comp", sourceBuilders).size(size);
            if (after != null) {
                builder.aggregateAfter(after);
            }
            final InternalComposite result = searchAndReduce(reader, new AggTestConfig(builder, fieldTypes));
            if (result.getBuckets().isEmpty()) {
                break;
            }
            for (InternalComposite.InternalBucket bucket : result.getBuckets()) {
                out.add(bucket.getKeyAsString() + "#" + bucket.getDocCount());
            }
            after = result.afterKey();
            if (after == null) {
                break;
            }
        }
        return out;
    }
}

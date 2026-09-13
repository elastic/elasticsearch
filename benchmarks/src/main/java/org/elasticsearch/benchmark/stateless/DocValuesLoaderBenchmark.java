/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.stateless;

import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.benchmark.index.mapper.MapperServiceFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.codec.Elasticsearch93Lucene104Codec;
import org.elasticsearch.index.fieldvisitor.StoredFieldLoader;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.SourceFieldMetrics;
import org.elasticsearch.index.mapper.SourceLoader;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Param;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Random;

/**
 * Benchmark for {@link SourceLoader.SyntheticFieldLoader.DocValuesLoader} against a
 * stateless-simulated Directory, exercising the prefetch path in TSDB doc values codecs.
 *
 * <p>Builds an index with configurable numeric (long) and keyword fields, then loads
 * synthetic source for a batch of scattered doc IDs. When {@code indexSort=true}, a sort
 * on {@code @timestamp} activates the ES95 TSDB doc values format whose iterators
 * implement {@code Prefetchable}.
 *
 * <p>Sweeps via {@link Param}:
 * <ul>
 *   <li>{@code numericFields} — number of long fields</li>
 *   <li>{@code keywordFields} — number of keyword fields</li>
 *   <li>{@code numDocs} — index size</li>
 *   <li>{@code batchSize} — doc IDs loaded per query invocation</li>
 *   <li>{@code sparsity} — fraction of docs with a value for numeric fields (1.0 = dense)</li>
 *   <li>{@code indexSort} — whether to add an index sort (activates ES95 TSDB DV format)</li>
 * </ul>
 *
 * <h2>Usage</h2>
 * <pre>{@code
 * ./gradlew -p benchmarks run --args '
 *   DocValuesLoaderBenchmark
 *   -p cacheState=COLD -p firstByteLatencyMs=100
 *   -p indexSort=true,false
 * '
 * }</pre>
 */
@Fork(value = 1, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
public class DocValuesLoaderBenchmark extends AbstractStatelessQueryBenchmark {

    private static final long SEED = 0xDEADBEEFL;
    private static final String TIMESTAMP_FIELD = "@timestamp";

    @Param({ "10" })
    public int numericFields;

    @Param({ "7" })
    public int keywordFields;

    /** Length of each randomly-generated keyword value. Longer values reduce prefix compression and increase DV file size. */
    @Param({ "20" })
    public int keywordValueLen;

    @Param({ "5" })
    public int binaryFields;

    /**
     * Length of each randomly-generated binary value.
     * Binary values are stored as {@code binary} doc-values fields, exercising the
     * {@link org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesProducer} binary prefetch path.
     */
    @Param({ "20" })
    public int binaryValueLen;

    @Param({ "100000" })
    public int numDocs;

    @Param({ "64" })
    public int batchSize;

    @Param({ "1.0", "0.5" })
    public double sparsity;

    @Param({ "true", "false" })
    public boolean indexSort;

    private SourceLoader sourceLoader;
    private int[] docIdBatch;

    @Override
    protected Settings extraNodeSettings() {
        return Settings.builder().putList("node.roles", "search").build();
    }

    @Override
    protected IndexWriterConfig indexWriterConfig() {
        DocValuesFormat dvFormat = indexSort ? DocValuesFormat.forName("ES95TSDB") : DocValuesFormat.forName("Lucene90");
        IndexWriterConfig iwc = new IndexWriterConfig();
        iwc.setCodec(new Elasticsearch93Lucene104Codec() {
            @Override
            public DocValuesFormat getDocValuesFormatForField(String field) {
                return dvFormat;
            }
        });
        iwc.setUseCompoundFile(false);
        if (indexSort) {
            iwc.setIndexSort(new Sort(new SortedNumericSortField(TIMESTAMP_FIELD, SortField.Type.LONG, true)));
        }
        return iwc;
    }

    private static final char[] KW_ALPHABET = "abcdefghijklmnopqrstuvwxyz0123456789".toCharArray();

    @Override
    protected String indexCacheKey() {
        return "dvloader-n"
            + numericFields
            + "-k"
            + keywordFields
            + "-kl"
            + keywordValueLen
            + "-b"
            + binaryFields
            + "-bl"
            + binaryValueLen
            + "-d"
            + numDocs
            + "-s"
            + sparsity
            + "-sort"
            + indexSort;
    }

    @Override
    protected void buildIndex(IndexWriter writer) throws IOException {
        Random rng = new Random(SEED);
        char[] kwBuf = new char[keywordValueLen];
        // Binary fields are written in the IntegratedCount format: VInt(count=1) + VInt(len) + bytes.
        // The header is written once and then preserved; only the payload bytes are randomized per document.
        int binHeaderLen = vIntLen(1) + vIntLen(binaryValueLen);
        byte[] binBuf = new byte[binHeaderLen + binaryValueLen];
        byte[] binPayload = new byte[binaryValueLen];
        int pos = writeVInt(binBuf, 0, 1);
        writeVInt(binBuf, pos, binaryValueLen);
        for (int i = 0; i < numDocs; i++) {
            Document doc = new Document();
            long timestamp = 1_700_000_000_000L + i * 1000L;
            doc.add(new SortedNumericDocValuesField(TIMESTAMP_FIELD, timestamp));

            for (int f = 0; f < numericFields; f++) {
                if (rng.nextDouble() < sparsity) {
                    doc.add(new SortedNumericDocValuesField("metric_" + f, rng.nextLong()));
                }
            }

            for (int f = 0; f < keywordFields; f++) {
                for (int c = 0; c < keywordValueLen; c++) {
                    kwBuf[c] = KW_ALPHABET[rng.nextInt(KW_ALPHABET.length)];
                }
                doc.add(new SortedSetDocValuesField("dim_" + f, new BytesRef(new String(kwBuf))));
            }

            for (int f = 0; f < binaryFields; f++) {
                rng.nextBytes(binPayload);
                System.arraycopy(binPayload, 0, binBuf, binHeaderLen, binaryValueLen);
                doc.add(new BinaryDocValuesField("bin_" + f, new BytesRef(binBuf)));
            }

            writer.addDocument(doc);
        }
        writer.forceMerge(1);
    }

    @Override
    protected void prepareQuery() throws IOException {
        String mappings = buildMappings();
        Settings indexSettings = Settings.builder().put("index.mapping.source.mode", "synthetic").build();
        MapperService mapperService = MapperServiceFactory.create(mappings, Collections.emptyList(), indexSettings);
        sourceLoader = mapperService.mappingLookup().newSourceLoader(null, SourceFieldMetrics.NOOP, null);

        Random rng = new Random(SEED ^ 0xCAFE);
        docIdBatch = new int[batchSize];
        for (int i = 0; i < batchSize; i++) {
            docIdBatch[i] = rng.nextInt(numDocs);
        }
        Arrays.sort(docIdBatch);
    }

    @Override
    protected Object runQuery(IndexSearcher searcher) throws IOException {
        LeafReaderContext ctx = searcher.getIndexReader().leaves().get(0);
        SourceLoader.Leaf leaf = sourceLoader.leaf(ctx, docIdBatch);
        var storedFieldLoader = StoredFieldLoader.empty().getLoader(ctx, null);

        int loaded = 0;
        for (int docId : docIdBatch) {
            storedFieldLoader.advanceTo(docId);
            leaf.source(storedFieldLoader, docId);
            loaded++;
        }
        return loaded;
    }

    private String buildMappings() {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"_doc\":{\"properties\":{");
        sb.append("\"").append(TIMESTAMP_FIELD).append("\":{\"type\":\"date\"}");

        for (int f = 0; f < numericFields; f++) {
            sb.append(",\"metric_").append(f).append("\":{\"type\":\"long\"}");
        }
        for (int f = 0; f < keywordFields; f++) {
            sb.append(",\"dim_").append(f).append("\":{\"type\":\"keyword\"}");
        }
        for (int f = 0; f < binaryFields; f++) {
            sb.append(",\"bin_").append(f).append("\":{\"type\":\"binary\"}");
        }

        sb.append("}}}");
        return sb.toString();
    }

    private static int vIntLen(int value) {
        int len = 1;
        while ((value & ~0x7F) != 0) {
            value >>>= 7;
            len++;
        }
        return len;
    }

    private static int writeVInt(byte[] buf, int pos, int value) {
        while ((value & ~0x7F) != 0) {
            buf[pos++] = (byte) ((value & 0x7F) | 0x80);
            value >>>= 7;
        }
        buf[pos++] = (byte) value;
        return pos;
    }
}

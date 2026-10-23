/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.stateless;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.apache.lucene.codecs.perfield.PerFieldDocValuesFormat;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Sort;
import org.elasticsearch.benchmark.index.mapper.MapperServiceFactory;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.IndexSortConfig;
import org.elasticsearch.index.codec.DocValuesBatchPrefetcher;
import org.elasticsearch.index.codec.ElasticsearchStoredFieldsFormat;
import org.elasticsearch.index.codec.PerFieldMapperCodec;
import org.elasticsearch.index.codec.tsdb.es95.ES95TSDBDocValuesFormat;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fieldvisitor.LeafStoredFieldLoader;
import org.elasticsearch.index.fieldvisitor.StoredFieldLoader;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.SourceFieldMetrics;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.xcontent.XContentType;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Param;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;

/**
 * Loads the synthetic {@code _source} of a batch, as the fetch phase does, from a stateless-simulated directory, with and
 * without doc-values prefetch. Documents are parsed by the mapper and written with the codec of their index mode:
 * <ul>
 *   <li>{@code es819}: a {@code logsdb} index;</li>
 *   <li>{@code es95}: the same documents in the ES95 format;</li>
 *   <li>{@code columnar}: a {@code columnar} index, ColumNAR strings and ES819 numbers.</li>
 * </ul>
 * {@code prefetchBudget} is the document and column pairs a window holds, {@code 0} disabling prefetch.
 *
 * <h2>Usage</h2>
 * <pre>{@code
 * ./gradlew -p benchmarks run --args '
 *   DocValuesLoaderBenchmark
 *   -p cacheState=COLD -p firstByteLatencyMs=100 -p rangeSize=1mb
 * '
 * }</pre>
 */
@Fork(value = 1, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
public class DocValuesLoaderBenchmark extends AbstractStatelessQueryBenchmark {

    private static final long SEED = 0xDEADBEEFL;

    @Param({ "es819", "es95", "columnar" })
    public String format;

    @Param({ "10" })
    public int numericFields;

    @Param({ "10" })
    public int keywordFields;

    /** How many distinct values a keyword field takes; {@code 0} makes every value unique. */
    @Param({ "1000" })
    public int keywordCardinality;

    @Param({ "200000" })
    public int numDocs;

    /** Segments the index is left with. */
    @Param({ "1" })
    public int segments;

    @Param({ "16", "256" })
    public int batchSize;

    /** Fraction of documents that hold each field; {@code 1.0} is dense. */
    @Param({ "1.0", "0.3" })
    public double density;

    @Param({ "0", "1024" })
    public int prefetchBudget;

    /** Whether the batch is spread evenly over the segment, so that no two of its documents are close, rather than random. */
    @Param({ "false" })
    public boolean spreadBatch;

    /**
     * Cache range and region size; empty keeps the default of 16mb, a range in which this index is read whole when the reader
     * opens, which leaves a cold batch nothing to download.
     */
    @Param({ "1mb" })
    public String rangeSize;

    private MapperService mapperService;
    private SourceLoader sourceLoader;
    private int[] batch;

    @Override
    protected Settings extraNodeSettings() {
        Settings.Builder settings = Settings.builder().putList("node.roles", "search");
        if (rangeSize.isEmpty() == false) {
            settings.put(SharedBlobCacheService.SHARED_CACHE_RANGE_SIZE_SETTING.getKey(), rangeSize);
            settings.put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), rangeSize);
        }
        return settings.build();
    }

    private MapperService mapperService() {
        if (mapperService == null) {
            Settings settings = Settings.builder().put("index.mode", format.equals("columnar") ? "columnar" : "logsdb").build();
            mapperService = MapperServiceFactory.create(mappings(), Collections.emptyList(), settings);
        }
        return mapperService;
    }

    @Override
    protected IndexWriterConfig indexWriterConfig() {
        MapperService mapperService = mapperService();
        Codec codec = new PerFieldMapperCodec(
            Lucene104Codec.Mode.BEST_SPEED,
            ElasticsearchStoredFieldsFormat.Mode.LUCENE,
            ElasticsearchStoredFieldsFormat.Mode.LUCENE,
            mapperService,
            BigArrays.NON_RECYCLING_INSTANCE,
            null
        );
        if (format.equals("es95")) {
            Codec perField = codec;
            DocValuesFormat es95 = new ES95TSDBDocValuesFormat();
            codec = new FilterCodec(perField.getName(), perField) {
                private final DocValuesFormat docValues = new PerFieldDocValuesFormat() {
                    @Override
                    public DocValuesFormat getDocValuesFormatForField(String field) {
                        DocValuesFormat chosen = ((PerFieldMapperCodec) perField).getDocValuesFormatForField(field);
                        // ES95 where the mapping chose a TSDB format.
                        return chosen.getName().startsWith("ES819") ? es95 : chosen;
                    }
                };

                @Override
                public DocValuesFormat docValuesFormat() {
                    return docValues;
                }
            };
        }
        IndexWriterConfig config = new IndexWriterConfig().setCodec(codec).setUseCompoundFile(false);
        config.setMergePolicy(segments > 1 ? NoMergePolicy.INSTANCE : config.getMergePolicy());
        Sort sort = new IndexSortConfig(mapperService.getIndexSettings()).buildIndexSort(
            mapperService::fieldType,
            (ft, s) -> ft.fielddataBuilder(FieldDataContext.noRuntimeFields("index", "")).build(null, null)
        );
        if (sort != null) {
            config.setIndexSort(sort);
        }
        return config;
    }

    @Override
    protected String indexCacheKey() {
        return "dvprefetch-"
            + format
            + "-n"
            + numericFields
            + "-k"
            + keywordFields
            + "-c"
            + keywordCardinality
            + "-d"
            + numDocs
            + "-s"
            + segments
            + "-p"
            + density;
    }

    @Override
    protected void buildIndex(IndexWriter writer) throws IOException {
        Random random = new Random(SEED);
        for (int i = 0; i < numDocs; i++) {
            StringBuilder source = new StringBuilder("{\"@timestamp\":").append(1_700_000_000_000L + i * 1000L);
            source.append(",\"host\":{\"name\":\"host-").append(random.nextInt(50)).append("\"}");
            for (int f = 0; f < numericFields; f++) {
                if (random.nextDouble() < density) {
                    source.append(",\"metric_").append(f).append("\":").append(random.nextInt(1_000_000));
                }
            }
            for (int f = 0; f < keywordFields; f++) {
                if (random.nextDouble() < density) {
                    long value = keywordCardinality == 0 ? random.nextLong() : random.nextInt(keywordCardinality);
                    source.append(",\"dim_").append(f).append("\":\"value-").append(f).append('-').append(value).append('"');
                }
            }
            source.append('}');
            List<LuceneDocument> docs = mapperService().documentMapper()
                .parse(new SourceToParse(Integer.toString(i), new BytesArray(source.toString()), XContentType.JSON))
                .docs();
            writer.addDocuments(docs);
            if (segments > 1 && (i + 1) % (numDocs / segments) == 0) {
                writer.flush();
            }
        }
        if (segments == 1) {
            writer.forceMerge(1);
        }
    }

    @Override
    protected void prepareQuery() throws IOException {
        sourceLoader = mapperService().mappingLookup().newSourceLoader(null, SourceFieldMetrics.NOOP, null);
        Random random = new Random(SEED ^ 0xCAFE);
        batch = spreadBatch
            ? IntStream.range(0, batchSize).map(i -> (int) ((2L * i + 1) * numDocs / (2L * batchSize))).toArray()
            : random.ints(0, numDocs).distinct().limit(batchSize).sorted().toArray();
    }

    @Override
    protected Object runQuery(IndexSearcher searcher) throws IOException {
        List<LeafReaderContext> leaves = new ArrayList<>();
        List<int[]> docsInLeaves = new ArrayList<>();
        List<LeafReaderContext> all = searcher.getIndexReader().leaves();
        for (int i = 0; i < batch.length;) {
            LeafReaderContext leaf = all.get(ReaderUtil.subIndex(batch[i], all));
            int end = i;
            while (end < batch.length && batch[end] < leaf.docBase + leaf.reader().maxDoc()) {
                end++;
            }
            leaves.add(leaf);
            docsInLeaves.add(Arrays.stream(batch, i, end).map(doc -> doc - leaf.docBase).toArray());
            i = end;
        }
        // A fresh loader each time, as each fetch gets one.
        SourceLoader loader = mapperService.mappingLookup().newSourceLoader(null, SourceFieldMetrics.NOOP, null);
        if (prefetchBudget > 0) {
            loader.announce(new DocValuesBatchPrefetcher(prefetchBudget), leaves, docsInLeaves);
        }
        int loaded = 0;
        for (int l = 0; l < leaves.size(); l++) {
            LeafReaderContext leaf = leaves.get(l);
            int[] docs = docsInLeaves.get(l);
            SourceLoader.Leaf sourceLeaf = loader.leaf(leaf, docs);
            LeafStoredFieldLoader storedFields = StoredFieldLoader.create(false, loader.requiredStoredFields()).getLoader(leaf, docs);
            for (int doc : docs) {
                storedFields.advanceTo(doc);
                loaded += sourceLeaf.source(storedFields, doc).internalSourceRef().length();
            }
        }
        return loaded;
    }

    private String mappings() {
        StringBuilder mappings = new StringBuilder("{\"_doc\":{\"properties\":{");
        mappings.append("\"@timestamp\":{\"type\":\"date\"},\"host\":{\"properties\":{\"name\":{\"type\":\"keyword\"}}}");
        for (int f = 0; f < numericFields; f++) {
            mappings.append(",\"metric_").append(f).append("\":{\"type\":\"long\"}");
        }
        for (int f = 0; f < keywordFields; f++) {
            mappings.append(",\"dim_").append(f).append("\":{\"type\":\"keyword\"}");
        }
        return mappings.append("}}}").toString();
    }
}

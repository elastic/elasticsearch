/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.knn;

import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Command line arguments for the KNN index tester.
 * This class encapsulates all the parameters required to run the KNN index tests.
 */
record CmdLineArgs(
    List<Path> docVectors,
    Path queryVectors,
    int numDocs,
    int numQueries,
    KnnIndexTester.IndexType indexType,
    int ivfClusterSize,
    int hnswM,
    int hnswEfConstruction,
    int indexThreads,
    boolean reindex,
    boolean forceMerge,
    long seed,
    VectorSimilarityFunction vectorSpace,
    int quantizeBits,
    VectorEncoding vectorEncoding,
    int dimensions,
    boolean earlyTermination,
    KnnIndexTester.MergePolicyType mergePolicy,
    double writerBufferSizeInMb,
    int writerMaxBufferedDocs,
    int forceMergeMaxNumSegments,
    boolean onDiskRescore,
    List<SearchParameters> searchParams
) {

    static final ParseField DOC_VECTORS_FIELD = new ParseField("doc_vectors");
    static final ParseField QUERY_VECTORS_FIELD = new ParseField("query_vectors");
    static final ParseField NUM_DOCS_FIELD = new ParseField("num_docs");
    static final ParseField NUM_QUERIES_FIELD = new ParseField("num_queries");
    static final ParseField INDEX_TYPE_FIELD = new ParseField("index_type");
    static final ParseField NUM_CANDIDATES_FIELD = new ParseField("num_candidates");
    static final ParseField K_FIELD = new ParseField("k");
    // static final ParseField N_PROBE_FIELD = new ParseField("n_probe");
    static final ParseField VISIT_PERCENTAGE_FIELD = new ParseField("visit_percentage");
    static final ParseField IVF_CLUSTER_SIZE_FIELD = new ParseField("ivf_cluster_size");
    static final ParseField OVER_SAMPLING_FACTOR_FIELD = new ParseField("over_sampling_factor");
    static final ParseField HNSW_M_FIELD = new ParseField("hnsw_m");
    static final ParseField HNSW_EF_CONSTRUCTION_FIELD = new ParseField("hnsw_ef_construction");
    static final ParseField NUM_SEARCHERS_FIELD = new ParseField("num_searchers");
    static final ParseField SEARCH_THREADS_FIELD = new ParseField("search_threads");
    static final ParseField INDEX_THREADS_FIELD = new ParseField("index_threads");
    static final ParseField REINDEX_FIELD = new ParseField("reindex");
    static final ParseField FORCE_MERGE_FIELD = new ParseField("force_merge");
    static final ParseField FORCE_MERGE_MAX_NUM_SEGMENTS_FIELD = new ParseField("force_merge_max_num_segments");
    static final ParseField VECTOR_SPACE_FIELD = new ParseField("vector_space");
    static final ParseField QUANTIZE_BITS_FIELD = new ParseField("quantize_bits");
    static final ParseField VECTOR_ENCODING_FIELD = new ParseField("vector_encoding");
    static final ParseField DIMENSIONS_FIELD = new ParseField("dimensions");
    static final ParseField EARLY_TERMINATION_FIELD = new ParseField("early_termination");
    static final ParseField FILTER_SELECTIVITY_FIELD = new ParseField("filter_selectivity");
    static final ParseField SEED_FIELD = new ParseField("seed");
    static final ParseField MERGE_POLICY_FIELD = new ParseField("merge_policy");
    static final ParseField WRITER_BUFFER_MB_FIELD = new ParseField("writer_buffer_mb");
    static final ParseField WRITER_BUFFER_DOCS_FIELD = new ParseField("writer_buffer_docs");
    static final ParseField ON_DISK_RESCORE_FIELD = new ParseField("on_disk_rescore");
    static final ParseField FILTER_CACHED = new ParseField("filter_cache");
    static final ParseField SEARCH_PARAMS = new ParseField("search_params");

    /** By default, in ES the default writer buffer size is 10% of the heap space
     * (see {@code IndexingMemoryController.INDEX_BUFFER_SIZE_SETTING}).
     * We configure the Java heap size for this tool in {@code build.gradle}; currently we default to 16GB, so in that case
     * the buffer size would be 1.6GB.
     */
    static final double DEFAULT_WRITER_BUFFER_MB = (JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() / (1024.0 * 1024.0)) * 0.1;

    static CmdLineArgs fromXContent(XContentParser parser) throws IOException {
        Builder builder = PARSER.apply(parser, null);
        return builder.build();
    }

    static final ObjectParser<CmdLineArgs.Builder, Void> PARSER = new ObjectParser<>("cmd_line_args", false, Builder::new);

    static {
        PARSER.declareStringArray(Builder::setDocVectors, DOC_VECTORS_FIELD);
        PARSER.declareString(Builder::setQueryVectors, QUERY_VECTORS_FIELD);
        PARSER.declareInt(Builder::setNumDocs, NUM_DOCS_FIELD);
        PARSER.declareInt(Builder::setNumQueries, NUM_QUERIES_FIELD);
        PARSER.declareString(Builder::setIndexType, INDEX_TYPE_FIELD);
        PARSER.declareInt(Builder::setNumCandidates, NUM_CANDIDATES_FIELD);
        PARSER.declareInt(Builder::setK, K_FIELD);
        // PARSER.declareIntArray(Builder::setNProbe, N_PROBE_FIELD);
        PARSER.declareDoubleArray(Builder::setVisitPercentages, VISIT_PERCENTAGE_FIELD);
        PARSER.declareInt(Builder::setIvfClusterSize, IVF_CLUSTER_SIZE_FIELD);
        PARSER.declareFloat(Builder::setOverSamplingFactor, OVER_SAMPLING_FACTOR_FIELD);
        PARSER.declareInt(Builder::setHnswM, HNSW_M_FIELD);
        PARSER.declareInt(Builder::setHnswEfConstruction, HNSW_EF_CONSTRUCTION_FIELD);
        PARSER.declareInt(Builder::setSearchThreads, SEARCH_THREADS_FIELD);
        PARSER.declareInt(Builder::setNumSearchers, NUM_SEARCHERS_FIELD);
        PARSER.declareInt(Builder::setIndexThreads, INDEX_THREADS_FIELD);
        PARSER.declareBoolean(Builder::setReindex, REINDEX_FIELD);
        PARSER.declareBoolean(Builder::setForceMerge, FORCE_MERGE_FIELD);
        PARSER.declareString(Builder::setVectorSpace, VECTOR_SPACE_FIELD);
        PARSER.declareInt(Builder::setQuantizeBits, QUANTIZE_BITS_FIELD);
        PARSER.declareString(Builder::setVectorEncoding, VECTOR_ENCODING_FIELD);
        PARSER.declareInt(Builder::setDimensions, DIMENSIONS_FIELD);
        PARSER.declareBoolean(Builder::setEarlyTermination, EARLY_TERMINATION_FIELD);
        PARSER.declareFloat(Builder::setFilterSelectivity, FILTER_SELECTIVITY_FIELD);
        PARSER.declareLong(Builder::setSeed, SEED_FIELD);
        PARSER.declareString(Builder::setMergePolicy, MERGE_POLICY_FIELD);
        PARSER.declareDouble(Builder::setWriterBufferMb, WRITER_BUFFER_MB_FIELD);
        PARSER.declareInt(Builder::setWriterMaxBufferedDocs, WRITER_BUFFER_DOCS_FIELD);
        PARSER.declareInt(Builder::setForceMergeMaxNumSegments, FORCE_MERGE_MAX_NUM_SEGMENTS_FIELD);
        PARSER.declareBoolean(Builder::setOnDiskRescore, ON_DISK_RESCORE_FIELD);
        PARSER.declareBoolean(Builder::setFilterCached, FILTER_CACHED);
        PARSER.declareObjectArray(Builder::setSearchParams, (p, c) -> SearchParameters.fromXContent(p), SEARCH_PARAMS);
    }

    public int numberOfSearchRuns() {
        return searchParams.size();
    }

    public static String exampleFormatForHelp() {
        var b = new CmdLineArgs.Builder().setDimensions(64)
            .setDocVectors(List.of("/doc/vectors/path"))
            .setQueryVectors("/query/vectors/path")
            .setSearchParams(
                List.of(
                    SearchParameters.builder().setEarlyTermination(true).setTopK(100),
                    SearchParameters.builder().setEarlyTermination(false).setTopK(10)
                )
            );

        return Strings.toString(b, true, false);
    }

    static class Builder implements ToXContentObject {
        private List<Path> docVectors;
        private Path queryVectors;
        private int numDocs = 1000;
        private int numQueries = 100;
        private KnnIndexTester.IndexType indexType = KnnIndexTester.IndexType.HNSW;
        private int numCandidates = 1000;
        private int k = 10;
        private double[] visitPercentages = new double[] { 1.0 };
        private int ivfClusterSize = 1000;
        private float overSamplingFactor = 0;
        private int hnswM = 16;
        private int hnswEfConstruction = 200;
        private int searchThreads = 1;
        private int numSearchers = 1;
        private int indexThreads = 1;
        private boolean reindex = false;
        private boolean forceMerge = false;
        private int forceMergeMaxNumSegments = 1;
        private VectorSimilarityFunction vectorSpace = VectorSimilarityFunction.EUCLIDEAN;
        private int quantizeBits = 8;
        private VectorEncoding vectorEncoding = VectorEncoding.FLOAT32;
        private int dimensions;
        private boolean earlyTermination;
        private float filterSelectivity = 1f;
        private long seed = 1751900822751L;
        private KnnIndexTester.MergePolicyType mergePolicy = null;
        private double writerBufferSizeInMb = DEFAULT_WRITER_BUFFER_MB;
        private boolean onDiskRescore = false;
        private boolean filterCached = true;
        private List<SearchParameters.Builder> searchParams = null;

        /**
         * Elasticsearch does not set this explicitly, and in Lucene this setting is
         * disabled by default (writer flushes by RAM usage).
         */
        private int writerMaxBufferedDocs = IndexWriterConfig.DISABLE_AUTO_FLUSH;

        public Builder setDocVectors(List<String> docVectors) {
            if (docVectors == null || docVectors.isEmpty()) {
                throw new IllegalArgumentException("Document vectors path must be provided");
            }
            // Convert list of strings to list of Paths
            this.docVectors = docVectors.stream().map(PathUtils::get).toList();
            return this;
        }

        public Builder setQueryVectors(String queryVectors) {
            this.queryVectors = PathUtils.get(queryVectors);
            return this;
        }

        public Builder setNumDocs(int numDocs) {
            this.numDocs = numDocs;
            return this;
        }

        public Builder setNumQueries(int numQueries) {
            this.numQueries = numQueries;
            return this;
        }

        public Builder setIndexType(String indexType) {
            this.indexType = KnnIndexTester.IndexType.valueOf(indexType.toUpperCase(Locale.ROOT));
            return this;
        }

        public Builder setNumCandidates(int numCandidates) {
            this.numCandidates = numCandidates;
            return this;
        }

        public Builder setK(int k) {
            this.k = k;
            return this;
        }

        public Builder setVisitPercentages(List<Double> visitPercentages) {
            this.visitPercentages = visitPercentages.stream().mapToDouble(Double::doubleValue).toArray();
            return this;
        }

        public Builder setIvfClusterSize(int ivfClusterSize) {
            this.ivfClusterSize = ivfClusterSize;
            return this;
        }

        public Builder setOverSamplingFactor(float overSamplingFactor) {
            this.overSamplingFactor = overSamplingFactor;
            return this;
        }

        public Builder setHnswM(int hnswM) {
            this.hnswM = hnswM;
            return this;
        }

        public Builder setHnswEfConstruction(int hnswEfConstruction) {
            this.hnswEfConstruction = hnswEfConstruction;
            return this;
        }

        public Builder setSearchThreads(int searchThreads) {
            this.searchThreads = searchThreads;
            return this;
        }

        public Builder setNumSearchers(int numSearchers) {
            this.numSearchers = numSearchers;
            return this;
        }

        public Builder setIndexThreads(int indexThreads) {
            this.indexThreads = indexThreads;
            return this;
        }

        public Builder setReindex(boolean reindex) {
            this.reindex = reindex;
            return this;
        }

        public Builder setForceMerge(boolean forceMerge) {
            this.forceMerge = forceMerge;
            return this;
        }

        public Builder setVectorSpace(String vectorSpace) {
            this.vectorSpace = VectorSimilarityFunction.valueOf(vectorSpace.toUpperCase(Locale.ROOT));
            return this;
        }

        public Builder setQuantizeBits(int quantizeBits) {
            this.quantizeBits = quantizeBits;
            return this;
        }

        public Builder setVectorEncoding(String vectorEncoding) {
            this.vectorEncoding = VectorEncoding.valueOf(vectorEncoding.toUpperCase(Locale.ROOT));
            return this;
        }

        public Builder setDimensions(int dimensions) {
            this.dimensions = dimensions;
            return this;
        }

        public Builder setEarlyTermination(Boolean patience) {
            this.earlyTermination = patience;
            return this;
        }

        public Builder setFilterSelectivity(float filterSelectivity) {
            this.filterSelectivity = filterSelectivity;
            return this;
        }

        public Builder setSeed(long seed) {
            this.seed = seed;
            return this;
        }

        public Builder setMergePolicy(String mergePolicy) {
            this.mergePolicy = KnnIndexTester.MergePolicyType.valueOf(mergePolicy.toUpperCase(Locale.ROOT));
            return this;
        }

        public Builder setWriterBufferMb(double writerBufferSizeInMb) {
            this.writerBufferSizeInMb = writerBufferSizeInMb;
            return this;
        }

        public Builder setWriterMaxBufferedDocs(int writerMaxBufferedDocs) {
            this.writerMaxBufferedDocs = writerMaxBufferedDocs;
            return this;
        }

        public Builder setForceMergeMaxNumSegments(int forceMergeMaxNumSegments) {
            this.forceMergeMaxNumSegments = forceMergeMaxNumSegments;
            return this;
        }

        public Builder setOnDiskRescore(boolean onDiskRescore) {
            this.onDiskRescore = onDiskRescore;
            return this;
        }

        public Builder setFilterCached(boolean filterCached) {
            this.filterCached = filterCached;
            return this;
        }

        public Builder setSearchParams(List<SearchParameters.Builder> searchParams) {
            this.searchParams = searchParams;
            return this;
        }

        public CmdLineArgs build() {
            if (docVectors == null) {
                throw new IllegalArgumentException("Document vectors path must be provided");
            }
            if (dimensions <= 0 && dimensions != -1) {
                throw new IllegalArgumentException(
                    "dimensions must be a positive integer or -1 for when dimension is available in the vector file"
                );
            }

            var baseSearchParams = new SearchParameters(
                numCandidates,
                k,
                visitPercentages[0],
                overSamplingFactor,
                searchThreads,
                numSearchers,
                filterSelectivity,
                filterCached,
                earlyTermination
            );

            if (visitPercentages.length > 1 && (searchParams != null && searchParams.isEmpty() == false)) {
                throw new IllegalArgumentException(
                    Strings.format(
                        "The %1$s option is incompatible with setting multiple values of %2$s. Use %1$s to control %2$s",
                        SEARCH_PARAMS,
                        VISIT_PERCENTAGE_FIELD
                    )
                );
            }

            List<SearchParameters> searchRuns = new ArrayList<>();

            if (searchParams == null || searchParams.isEmpty()) {
                // single base case
                searchRuns.add(baseSearchParams);

                // Convert any extra values in the list of visit percentages to the
                // search params format. This is for backwards compatibility where multiple
                // values of visit percentage would equate to multiple searches
                for (int i = 1; i < visitPercentages.length; i++) {
                    searchRuns.add(SearchParameters.builder().setVisitPercentage(visitPercentages[i]).buildWithDefaults(baseSearchParams));
                }
            } else {
                for (var so : searchParams) {
                    searchRuns.add(so.buildWithDefaults(baseSearchParams));
                }
            }

            return new CmdLineArgs(
                docVectors,
                queryVectors,
                numDocs,
                numQueries,
                indexType,
                ivfClusterSize,
                hnswM,
                hnswEfConstruction,
                indexThreads,
                reindex,
                forceMerge,
                seed,
                vectorSpace,
                quantizeBits,
                vectorEncoding,
                dimensions,
                earlyTermination,
                mergePolicy,
                writerBufferSizeInMb,
                writerMaxBufferedDocs,
                forceMergeMaxNumSegments,
                onDiskRescore,
                searchRuns
            );
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (docVectors != null) {
                List<String> docVectorsStrings = docVectors.stream().map(Path::toString).toList();
                builder.field(DOC_VECTORS_FIELD.getPreferredName(), docVectorsStrings);
            }
            if (queryVectors != null) {
                builder.field(QUERY_VECTORS_FIELD.getPreferredName(), queryVectors.toString());
            }
            builder.field(NUM_DOCS_FIELD.getPreferredName(), numDocs);
            builder.field(NUM_QUERIES_FIELD.getPreferredName(), numQueries);
            builder.field(INDEX_TYPE_FIELD.getPreferredName(), indexType.name().toLowerCase(Locale.ROOT));
            // builder.field(N_PROBE_FIELD.getPreferredName(), nProbes);
            builder.field(IVF_CLUSTER_SIZE_FIELD.getPreferredName(), ivfClusterSize);
            builder.field(HNSW_M_FIELD.getPreferredName(), hnswM);
            builder.field(HNSW_EF_CONSTRUCTION_FIELD.getPreferredName(), hnswEfConstruction);
            builder.field(INDEX_THREADS_FIELD.getPreferredName(), indexThreads);
            builder.field(REINDEX_FIELD.getPreferredName(), reindex);
            builder.field(FORCE_MERGE_FIELD.getPreferredName(), forceMerge);
            builder.field(VECTOR_SPACE_FIELD.getPreferredName(), vectorSpace.name().toLowerCase(Locale.ROOT));
            builder.field(QUANTIZE_BITS_FIELD.getPreferredName(), quantizeBits);
            builder.field(VECTOR_ENCODING_FIELD.getPreferredName(), vectorEncoding.name().toLowerCase(Locale.ROOT));
            builder.field(DIMENSIONS_FIELD.getPreferredName(), dimensions);
            builder.field(EARLY_TERMINATION_FIELD.getPreferredName(), earlyTermination);
            builder.field(SEED_FIELD.getPreferredName(), seed);
            builder.field(WRITER_BUFFER_MB_FIELD.getPreferredName(), writerBufferSizeInMb);
            builder.field(WRITER_BUFFER_DOCS_FIELD.getPreferredName(), writerMaxBufferedDocs);
            builder.field(FORCE_MERGE_MAX_NUM_SEGMENTS_FIELD.getPreferredName(), forceMergeMaxNumSegments);
            builder.field(ON_DISK_RESCORE_FIELD.getPreferredName(), onDiskRescore);
            if (mergePolicy != null) {
                builder.field(MERGE_POLICY_FIELD.getPreferredName(), mergePolicy.name().toLowerCase(Locale.ROOT));
            }
            if (searchParams != null) {
                builder.field(SEARCH_PARAMS.getPreferredName(), searchParams);
            }
            return builder.endObject();
        }

    }
}

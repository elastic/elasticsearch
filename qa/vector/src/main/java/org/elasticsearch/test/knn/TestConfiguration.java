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
import org.apache.lucene.index.VectorSimilarityFunction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskBBQVectorsFormat;
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
record TestConfiguration(
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
    VectorSimilarityFunction vectorSpace,
    Integer quantizeBits,
    KnnIndexTester.VectorEncoding vectorEncoding,
    int dimensions,
    KnnIndexTester.MergePolicyType mergePolicy,
    double writerBufferSizeInMb,
    int writerMaxBufferedDocs,
    int forceMergeMaxNumSegments,
    boolean onDiskRescore,
    List<SearchParameters> searchParams,
    int numMergeWorkers,
    boolean doPrecondition,
    int preconditioningBlockDims,
    int secondaryClusterSize
) {

    static final ParseField DOC_VECTORS_FIELD = new ParseField("doc_vectors");
    static final ParseField QUERY_VECTORS_FIELD = new ParseField("query_vectors");
    static final ParseField NUM_DOCS_FIELD = new ParseField("num_docs");
    static final ParseField NUM_QUERIES_FIELD = new ParseField("num_queries");
    static final ParseField INDEX_TYPE_FIELD = new ParseField("index_type");
    static final ParseField NUM_CANDIDATES_FIELD = new ParseField("num_candidates");
    static final ParseField K_FIELD = new ParseField("k");
    static final ParseField VISIT_PERCENTAGE_FIELD = new ParseField("visit_percentage");
    static final ParseField IVF_CLUSTER_SIZE_FIELD = new ParseField("ivf_cluster_size");
    static final ParseField SECONDARY_CLUSTER_SIZE = new ParseField("secondary_cluster_size");
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
    static final ParseField MERGE_WORKERS_FIELD = new ParseField("merge_workers");
    static final ParseField WRITER_BUFFER_MB_FIELD = new ParseField("writer_buffer_mb");
    static final ParseField WRITER_BUFFER_DOCS_FIELD = new ParseField("writer_buffer_docs");
    static final ParseField ON_DISK_RESCORE_FIELD = new ParseField("on_disk_rescore");
    static final ParseField DO_PRECONDITION = new ParseField("precondition");
    static final ParseField PRECONDITIONING_BLOCK_DIMS = new ParseField("preconditioning_block_dims");
    static final ParseField FILTER_CACHED = new ParseField("filter_cache");
    static final ParseField SEARCH_PARAMS = new ParseField("search_params");

    /** By default, in ES the default writer buffer size is 10% of the heap space
     * (see {@code IndexingMemoryController.INDEX_BUFFER_SIZE_SETTING}).
     * We configure the Java heap size for this tool in {@code build.gradle}; currently we default to 16GB, so in that case
     * the buffer size would be 1.6GB.
     */
    static final double DEFAULT_WRITER_BUFFER_MB = (JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() / (1024.0 * 1024.0)) * 0.1;

    static TestConfiguration fromXContent(XContentParser parser) throws IOException {
        Builder builder = PARSER.apply(parser, null);
        return builder.build();
    }

    static final ObjectParser<TestConfiguration.Builder, Void> PARSER = new ObjectParser<>("test_configuration", false, Builder::new);

    static {
        PARSER.declareStringArray(Builder::setDocVectors, DOC_VECTORS_FIELD);
        PARSER.declareString(Builder::setQueryVectors, QUERY_VECTORS_FIELD);
        PARSER.declareInt(Builder::setNumDocs, NUM_DOCS_FIELD);
        PARSER.declareInt(Builder::setNumQueries, NUM_QUERIES_FIELD);
        PARSER.declareString(Builder::setIndexType, INDEX_TYPE_FIELD);
        PARSER.declareIntArray(Builder::setNumCandidates, NUM_CANDIDATES_FIELD);
        PARSER.declareIntArray(Builder::setK, K_FIELD);
        PARSER.declareDoubleArray(Builder::setVisitPercentages, VISIT_PERCENTAGE_FIELD);
        PARSER.declareInt(Builder::setIvfClusterSize, IVF_CLUSTER_SIZE_FIELD);
        PARSER.declareFloatArray(Builder::setOverSamplingFactor, OVER_SAMPLING_FACTOR_FIELD);
        PARSER.declareInt(Builder::setHnswM, HNSW_M_FIELD);
        PARSER.declareInt(Builder::setHnswEfConstruction, HNSW_EF_CONSTRUCTION_FIELD);
        PARSER.declareIntArray(Builder::setSearchThreads, SEARCH_THREADS_FIELD);
        PARSER.declareIntArray(Builder::setNumSearchers, NUM_SEARCHERS_FIELD);
        PARSER.declareInt(Builder::setIndexThreads, INDEX_THREADS_FIELD);
        PARSER.declareBoolean(Builder::setReindex, REINDEX_FIELD);
        PARSER.declareBoolean(Builder::setForceMerge, FORCE_MERGE_FIELD);
        PARSER.declareString(Builder::setVectorSpace, VECTOR_SPACE_FIELD);
        PARSER.declareField(
            Builder::setQuantizeBits,
            p -> p.currentToken() == XContentParser.Token.VALUE_NULL ? null : p.intValue(),
            QUANTIZE_BITS_FIELD,
            ObjectParser.ValueType.INT_OR_NULL
        );
        PARSER.declareString(Builder::setVectorEncoding, VECTOR_ENCODING_FIELD);
        PARSER.declareInt(Builder::setDimensions, DIMENSIONS_FIELD);
        PARSER.declareFieldArray(
            Builder::setEarlyTermination,
            (p, c) -> p.booleanValue(),
            EARLY_TERMINATION_FIELD,
            ObjectParser.ValueType.VALUE_ARRAY
        );
        PARSER.declareFloatArray(Builder::setFilterSelectivity, FILTER_SELECTIVITY_FIELD);
        PARSER.declareLongArray(Builder::setSeed, SEED_FIELD);
        PARSER.declareString(Builder::setMergePolicy, MERGE_POLICY_FIELD);
        PARSER.declareDouble(Builder::setWriterBufferMb, WRITER_BUFFER_MB_FIELD);
        PARSER.declareInt(Builder::setWriterMaxBufferedDocs, WRITER_BUFFER_DOCS_FIELD);
        PARSER.declareInt(Builder::setForceMergeMaxNumSegments, FORCE_MERGE_MAX_NUM_SEGMENTS_FIELD);
        PARSER.declareBoolean(Builder::setOnDiskRescore, ON_DISK_RESCORE_FIELD);
        PARSER.declareBoolean(Builder::setDoPrecondition, DO_PRECONDITION);
        PARSER.declareInt(Builder::setPreconditioningBlockDims, PRECONDITIONING_BLOCK_DIMS);
        PARSER.declareFieldArray(Builder::setFilterCached, (p, c) -> p.booleanValue(), FILTER_CACHED, ObjectParser.ValueType.VALUE_ARRAY);
        PARSER.declareObjectArray(Builder::setSearchParams, (p, c) -> SearchParameters.fromXContent(p), SEARCH_PARAMS);
        PARSER.declareInt(Builder::setMergeWorkers, MERGE_WORKERS_FIELD);
        PARSER.declareInt(Builder::setSecondaryClusterSize, SECONDARY_CLUSTER_SIZE);
    }

    public int numberOfSearchRuns() {
        return searchParams.size();
    }

    public static String exampleFormatForHelp() {
        var b = new TestConfiguration.Builder().setDimensions(64)
            .setDocVectors(List.of("/doc/vectors/path"))
            .setQueryVectors("/query/vectors/path")
            .setNumCandidates(List.of(100, 1000))
            .setEarlyTermination(List.of(Boolean.TRUE, Boolean.FALSE));

        return Strings.toString(b, true, false);
    }

    public static String formattedParameterHelp() {
        List<ParameterHelp> params = List.of(
            new ParameterHelp("doc_vectors", "array[string]", "Required. Paths to document vectors files used for indexing."),
            new ParameterHelp("query_vectors", "string", "Optional. Path to query vectors file; omit to skip searches."),
            new ParameterHelp("num_docs", "int", "Number of documents to index."),
            new ParameterHelp("num_queries", "int", "Number of queries to run from the query vectors file."),
            new ParameterHelp("index_type", "string", "Index type: hnsw, flat, ivf, or gpu_hnsw."),
            new ParameterHelp("ivf_cluster_size", "int", "IVF: number of clusters."),
            new ParameterHelp("secondary_cluster_size", "int", "IVF: centroids per parent cluster; -1 uses the format default."),
            new ParameterHelp("hnsw_m", "int", "HNSW: M parameter (graph degree)."),
            new ParameterHelp("hnsw_ef_construction", "int", "HNSW: efConstruction parameter."),
            new ParameterHelp("index_threads", "int", "Number of threads used for indexing."),
            new ParameterHelp("reindex", "boolean", "Whether to build a new index from the document vectors."),
            new ParameterHelp("force_merge", "boolean", "Whether to force-merge the index after indexing."),
            new ParameterHelp("force_merge_max_num_segments", "int", "Force-merge target number of segments."),
            new ParameterHelp("vector_space", "string", "Similarity: euclidean, dot_product, or cosine."),
            new ParameterHelp("quantize_bits", "int", "Quantization bits; valid values depend on index_type."),
            new ParameterHelp("vector_encoding", "string", "Vector encoding: byte, float32, or bfloat16."),
            new ParameterHelp("dimensions", "int", "Vector dimensions; -1 uses dimensions from the vector file."),
            new ParameterHelp("merge_policy", "string", "Merge policy: tiered, log_byte, log_doc, or no."),
            new ParameterHelp("merge_workers", "int", "Number of merge worker threads for vector formats."),
            new ParameterHelp("writer_buffer_mb", "double", "Index writer RAM buffer size in MB."),
            new ParameterHelp("writer_buffer_docs", "int", "Max buffered docs before flush; -1 disables auto flush by docs."),
            new ParameterHelp("on_disk_rescore", "boolean", "Search: enable on-disk rescore for search."),
            new ParameterHelp("precondition", "boolean", "IVF: apply preconditioning prior to indexing."),
            new ParameterHelp("preconditioning_block_dims", "int", "IVF: block dimensions used for preconditioning."),
            new ParameterHelp("num_candidates", "array[int]", "HNSW: number of candidates (efSearch) to consider per query."),
            new ParameterHelp("k", "array[int]", "Search: top K results to return."),
            new ParameterHelp("visit_percentage", "array[double]", "IVF: percentage of IVF index to visit (0.0-100.0)."),
            new ParameterHelp("over_sampling_factor", "array[float]", "Search: oversampling factor for approximate search."),
            new ParameterHelp("search_threads", "array[int]", "Search: threads per searcher."),
            new ParameterHelp("num_searchers", "array[int]", "Search: number of parallel searchers."),
            new ParameterHelp("filter_selectivity", "array[float]", "Search: filter selectivity (0.0-1.0)."),
            new ParameterHelp("filter_cache", "array[boolean]", "Search: whether filters are cached."),
            new ParameterHelp("early_termination", "array[boolean]", "Search: allow early termination when possible."),
            new ParameterHelp("seed", "array[long]", "Search: random seed used random filters."),
            new ParameterHelp(
                "search_params",
                "array[object]",
                "Explicit per-search settings; each object may include search fields like num_candidates, k, and visit_percentage."
            )
        );

        int nameWidth = "parameter".length();
        int typeWidth = "type".length();
        for (ParameterHelp param : params) {
            nameWidth = Math.max(nameWidth, param.name.length());
            typeWidth = Math.max(typeWidth, param.type.length());
        }

        StringBuilder sb = new StringBuilder();
        sb.append("Configuration parameters:");
        sb.append("\n");
        sb.append(formatParamRow("parameter", "type", "description", nameWidth, typeWidth));
        sb.append("\n");
        sb.append("-".repeat(nameWidth)).append("  ").append("-".repeat(typeWidth)).append("  ").append("-".repeat("description".length()));
        sb.append("\n");
        for (ParameterHelp param : params) {
            sb.append(formatParamRow(param.name, param.type, param.description, nameWidth, typeWidth));
            sb.append("\n");
        }
        sb.append("\n");
        sb.append(
            "Notes: array parameters are combined as a cartesian product to define multiple search runs. "
                + "If you use search_params, do not provide multiple values for array parameters."
        );
        return sb.toString();
    }

    private static String formatParamRow(String name, String type, String description, int nameWidth, int typeWidth) {
        return String.format(Locale.ROOT, "%-" + nameWidth + "s  %-" + typeWidth + "s  %s", name, type, description);
    }

    private record ParameterHelp(String name, String type, String description) {}

    static class Builder implements ToXContentObject {
        private List<Path> docVectors;
        private Path queryVectors;
        private int numDocs = 1000;
        private int numQueries = 100;
        private KnnIndexTester.IndexType indexType = KnnIndexTester.IndexType.HNSW;
        private List<Integer> numCandidates = List.of(1000);
        private List<Integer> k = List.of(10);
        private List<Double> visitPercentages = List.of(1.0);
        private int ivfClusterSize = ESNextDiskBBQVectorsFormat.DEFAULT_VECTORS_PER_CLUSTER;
        private List<Float> overSamplingFactor = List.of(0f);
        private int hnswM = 16;
        private int hnswEfConstruction = 200;
        private List<Integer> searchThreads = List.of(1);
        private List<Integer> numSearchers = List.of(1);
        private int indexThreads = 1;
        private boolean reindex = false;
        private boolean forceMerge = false;
        private int forceMergeMaxNumSegments = 1;
        private VectorSimilarityFunction vectorSpace = VectorSimilarityFunction.EUCLIDEAN;
        private Integer quantizeBits = null;
        private KnnIndexTester.VectorEncoding vectorEncoding = KnnIndexTester.VectorEncoding.FLOAT32;
        private int dimensions;
        private List<Boolean> earlyTermination = List.of(Boolean.FALSE);
        private List<Float> filterSelectivity = List.of(1f);
        private List<Long> seed = List.of(1751900822751L);
        private KnnIndexTester.MergePolicyType mergePolicy = null;
        private double writerBufferSizeInMb = DEFAULT_WRITER_BUFFER_MB;
        private boolean onDiskRescore = false;
        private boolean doPrecondition = false;
        private int preconditioningBlockDims = 64;
        private List<Boolean> filterCached = List.of(Boolean.TRUE);
        private List<SearchParameters.Builder> searchParams = null;
        private int numMergeWorkers = 1;
        private int secondaryClusterSize = -1;

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

        public Builder setMergeWorkers(int numMergeWorkers) {
            this.numMergeWorkers = numMergeWorkers;
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

        public Builder setNumCandidates(List<Integer> numCandidates) {
            this.numCandidates = numCandidates;
            return this;
        }

        public Builder setK(List<Integer> k) {
            this.k = k;
            return this;
        }

        public Builder setVisitPercentages(List<Double> visitPercentages) {
            this.visitPercentages = visitPercentages;
            return this;
        }

        public Builder setIvfClusterSize(int ivfClusterSize) {
            this.ivfClusterSize = ivfClusterSize;
            return this;
        }

        public Builder setOverSamplingFactor(List<Float> overSamplingFactor) {
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

        public Builder setSearchThreads(List<Integer> searchThreads) {
            this.searchThreads = searchThreads;
            return this;
        }

        public Builder setNumSearchers(List<Integer> numSearchers) {
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

        public Builder setQuantizeBits(Integer quantizeBits) {
            this.quantizeBits = quantizeBits;
            return this;
        }

        public Builder setVectorEncoding(String vectorEncoding) {
            this.vectorEncoding = KnnIndexTester.VectorEncoding.valueOf(vectorEncoding.toUpperCase(Locale.ROOT));
            return this;
        }

        public Builder setDimensions(int dimensions) {
            this.dimensions = dimensions;
            return this;
        }

        public Builder setEarlyTermination(List<Boolean> patience) {
            this.earlyTermination = patience;
            return this;
        }

        public Builder setFilterSelectivity(List<Float> filterSelectivity) {
            this.filterSelectivity = filterSelectivity;
            return this;
        }

        public Builder setSeed(List<Long> seed) {
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

        public Builder setDoPrecondition(boolean doPrecondition) {
            this.doPrecondition = doPrecondition;
            return this;
        }

        public Builder setPreconditioningBlockDims(int preconditioningBlockDims) {
            this.preconditioningBlockDims = preconditioningBlockDims;
            return this;
        }

        public Builder setFilterCached(List<Boolean> filterCached) {
            this.filterCached = filterCached;
            return this;
        }

        public Builder setSearchParams(List<SearchParameters.Builder> searchParams) {
            this.searchParams = searchParams;
            return this;
        }

        public Builder setSecondaryClusterSize(int secondaryClusterSize) {
            this.secondaryClusterSize = secondaryClusterSize;
            return this;
        }

        public TestConfiguration build() {
            if (docVectors == null) {
                throw new IllegalArgumentException("Document vectors path must be provided");
            }
            if (dimensions <= 0 && dimensions != -1) {
                throw new IllegalArgumentException(
                    "dimensions must be a positive integer or -1 for when dimension is available in the vector file"
                );
            }

            // length of the longest array parameter
            int longestParam = longestParameter();
            if (longestParam > 1 && (searchParams != null && searchParams.isEmpty() == false)) {
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
                searchRuns.addAll(allCombinations());
            } else {
                // Using the search_param format.
                // Create a search run for each search param object
                var baseSearchParams = new SearchParameters(
                    numCandidates.getFirst(),
                    k.getFirst(),
                    visitPercentages.getFirst(),
                    overSamplingFactor.getFirst(),
                    searchThreads.getFirst(),
                    numSearchers.getFirst(),
                    filterSelectivity.getFirst(),
                    filterCached.getFirst(),
                    earlyTermination.getFirst(),
                    seed.getFirst()
                );

                for (var so : searchParams) {
                    searchRuns.add(so.buildWithDefaults(baseSearchParams));
                }
            }

            return new TestConfiguration(
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
                vectorSpace,
                quantizeBits,
                vectorEncoding,
                dimensions,
                mergePolicy,
                writerBufferSizeInMb,
                writerMaxBufferedDocs,
                forceMergeMaxNumSegments,
                onDiskRescore,
                searchRuns,
                numMergeWorkers,
                doPrecondition,
                preconditioningBlockDims,
                secondaryClusterSize
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
            builder.field(NUM_CANDIDATES_FIELD.getPreferredName(), numCandidates);
            builder.field(K_FIELD.getPreferredName(), k);
            builder.field(VISIT_PERCENTAGE_FIELD.getPreferredName(), visitPercentages);
            builder.field(IVF_CLUSTER_SIZE_FIELD.getPreferredName(), ivfClusterSize);
            builder.field(OVER_SAMPLING_FACTOR_FIELD.getPreferredName(), overSamplingFactor);
            builder.field(HNSW_M_FIELD.getPreferredName(), hnswM);
            builder.field(HNSW_EF_CONSTRUCTION_FIELD.getPreferredName(), hnswEfConstruction);
            builder.field(SEARCH_THREADS_FIELD.getPreferredName(), searchThreads);
            builder.field(NUM_SEARCHERS_FIELD.getPreferredName(), numSearchers);
            builder.field(INDEX_THREADS_FIELD.getPreferredName(), indexThreads);
            builder.field(REINDEX_FIELD.getPreferredName(), reindex);
            builder.field(FORCE_MERGE_FIELD.getPreferredName(), forceMerge);
            builder.field(VECTOR_SPACE_FIELD.getPreferredName(), vectorSpace.name().toLowerCase(Locale.ROOT));
            if (quantizeBits != null) {
                builder.field(QUANTIZE_BITS_FIELD.getPreferredName(), quantizeBits);
            }
            builder.field(VECTOR_ENCODING_FIELD.getPreferredName(), vectorEncoding.name().toLowerCase(Locale.ROOT));
            builder.field(DIMENSIONS_FIELD.getPreferredName(), dimensions);
            builder.field(EARLY_TERMINATION_FIELD.getPreferredName(), earlyTermination);
            builder.field(FILTER_SELECTIVITY_FIELD.getPreferredName(), filterSelectivity);
            builder.field(SEED_FIELD.getPreferredName(), seed);
            builder.field(WRITER_BUFFER_MB_FIELD.getPreferredName(), writerBufferSizeInMb);
            builder.field(WRITER_BUFFER_DOCS_FIELD.getPreferredName(), writerMaxBufferedDocs);
            builder.field(FORCE_MERGE_MAX_NUM_SEGMENTS_FIELD.getPreferredName(), forceMergeMaxNumSegments);
            builder.field(ON_DISK_RESCORE_FIELD.getPreferredName(), onDiskRescore);
            builder.field(FILTER_CACHED.getPreferredName(), filterCached);
            if (mergePolicy != null) {
                builder.field(MERGE_POLICY_FIELD.getPreferredName(), mergePolicy.name().toLowerCase(Locale.ROOT));
            }
            if (searchParams != null) {
                builder.field(SEARCH_PARAMS.getPreferredName(), searchParams);
            }
            return builder.endObject();
        }

        int longestParameter() {
            var lengths = List.of(
                numCandidates.size(),
                k.size(),
                visitPercentages.size(),
                overSamplingFactor.size(),
                searchThreads.size(),
                numSearchers.size(),
                filterSelectivity.size(),
                filterCached.size(),
                earlyTermination.size(),
                seed.size()
            );
            return lengths.stream().max(Integer::compareTo).get();
        }

        List<SearchParameters> allCombinations() {
            return cartesianProduct(
                List.of(
                    numCandidates,
                    k,
                    visitPercentages,
                    overSamplingFactor,
                    searchThreads,
                    numSearchers,
                    filterSelectivity,
                    filterCached,
                    earlyTermination,
                    seed
                )
            ).stream()
                .map(
                    params -> new SearchParameters(
                        (Integer) params.get(0),
                        (Integer) params.get(1),
                        (Double) params.get(2),
                        (Float) params.get(3),
                        (Integer) params.get(4),
                        (Integer) params.get(5),
                        (Float) params.get(6),
                        (Boolean) params.get(7),
                        (Boolean) params.get(8),
                        (Long) params.get(9)
                    )
                )
                .toList();
        }

        private static List<List<Object>> cartesianProduct(List<List<?>> lists) {
            List<List<Object>> result = new ArrayList<>();
            result.add(new ArrayList<>());

            for (List<?> list : lists) {
                List<List<Object>> temp = new ArrayList<>();
                for (List<Object> res : result) {
                    for (Object item : list) {
                        List<Object> newRes = new ArrayList<>(res);
                        newRes.add(item);
                        temp.add(newRes);
                    }
                }
                result = temp;
            }
            return result;
        }
    }
}

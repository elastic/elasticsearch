/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.knn;

import com.sun.management.ThreadMXBean;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.lucene103.Lucene103Codec;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.NamedThreadFactory;
import org.elasticsearch.cli.ProcessInfo;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.gpu.codec.ES92GpuHnswSQVectorsFormat;
import org.elasticsearch.gpu.codec.ES92GpuHnswVectorsFormat;
import org.elasticsearch.index.codec.vectors.diskbbq.ES920DiskBBQVectorsFormat;
import org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskBBQVectorsFormat;
import org.elasticsearch.index.codec.vectors.es93.ES93BinaryQuantizedVectorsFormat;
import org.elasticsearch.index.codec.vectors.es93.ES93HnswBinaryQuantizedVectorsFormat;
import org.elasticsearch.index.codec.vectors.es93.ES93HnswScalarQuantizedVectorsFormat;
import org.elasticsearch.index.codec.vectors.es93.ES93HnswVectorsFormat;
import org.elasticsearch.index.codec.vectors.es93.ES93ScalarQuantizedVectorsFormat;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.logging.Level;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.MAX_DIMS_COUNT;

/**
 * A utility class to create and test KNN indices using Lucene.
 * It supports various index types (HNSW, FLAT, IVF) and configurations.
 */
public class KnnIndexTester {
    static final Logger logger;

    static {
        LogConfigurator.loadLog4jPlugins();

        // necessary otherwise the es.logger.level system configuration in build.gradle is ignored
        ProcessInfo pinfo = ProcessInfo.fromSystem();
        Map<String, String> sysprops = pinfo.sysprops();
        String loggerLevel = sysprops.getOrDefault("es.logger.level", Level.INFO.name());
        Settings settings = Settings.builder().put("logger.level", loggerLevel).build();
        LogConfigurator.configureWithoutConfig(settings);

        logger = LogManager.getLogger(KnnIndexTester.class);
    }

    static final String INDEX_DIR = "target/knn_index";

    enum IndexType {
        HNSW,
        FLAT,
        IVF,
        GPU_HNSW
    }

    enum VectorEncoding {
        BYTE(org.apache.lucene.index.VectorEncoding.BYTE),
        FLOAT32(org.apache.lucene.index.VectorEncoding.FLOAT32),
        BFLOAT16(org.apache.lucene.index.VectorEncoding.FLOAT32);

        private final org.apache.lucene.index.VectorEncoding luceneEncoding;

        VectorEncoding(org.apache.lucene.index.VectorEncoding luceneEncoding) {
            this.luceneEncoding = luceneEncoding;
        }

        public org.apache.lucene.index.VectorEncoding luceneEncoding() {
            return luceneEncoding;
        }
    }

    enum MergePolicyType {
        TIERED,
        LOG_BYTE,
        NO,
        LOG_DOC
    }

    private static String formatIndexPath(TestConfiguration args) {
        List<String> suffix = new ArrayList<>();
        if (args.indexType() == IndexType.FLAT) {
            suffix.add("flat");
        } else if (args.indexType() == IndexType.GPU_HNSW) {
            suffix.add("gpu_hnsw");
        } else if (args.indexType() == IndexType.IVF) {
            suffix.add("ivf");
            suffix.add(Integer.toString(args.ivfClusterSize()));
            suffix.add(
                Integer.toString(
                    args.secondaryClusterSize() == -1
                        ? ES920DiskBBQVectorsFormat.DEFAULT_CENTROIDS_PER_PARENT_CLUSTER
                        : args.secondaryClusterSize()
                )
            );
            suffix.add(Integer.toString(args.quantizeBits()));
        } else {
            suffix.add(Integer.toString(args.hnswM()));
            suffix.add(Integer.toString(args.hnswEfConstruction()));
            if (args.quantizeBits() < 32) {
                suffix.add(Integer.toString(args.quantizeBits()));
            }
        }
        return INDEX_DIR + "/" + args.docVectors().get(0).getFileName() + "-" + String.join("-", suffix) + ".index";
    }

    static Codec createCodec(TestConfiguration args, @Nullable ExecutorService exec) {
        final KnnVectorsFormat format;
        int quantizeBits = args.quantizeBits();
        DenseVectorFieldMapper.ElementType elementType = switch (args.vectorEncoding()) {
            case BYTE -> DenseVectorFieldMapper.ElementType.BYTE;
            case FLOAT32 -> DenseVectorFieldMapper.ElementType.FLOAT;
            case BFLOAT16 -> DenseVectorFieldMapper.ElementType.BFLOAT16;
        };
        if (args.indexType() == IndexType.IVF) {
            ESNextDiskBBQVectorsFormat.QuantEncoding encoding = switch (quantizeBits) {
                case (1) -> ESNextDiskBBQVectorsFormat.QuantEncoding.ONE_BIT_4BIT_QUERY;
                case (2) -> ESNextDiskBBQVectorsFormat.QuantEncoding.TWO_BIT_4BIT_QUERY;
                case (4) -> ESNextDiskBBQVectorsFormat.QuantEncoding.FOUR_BIT_SYMMETRIC;
                default -> throw new IllegalArgumentException(
                    "IVF index type only supports 1, 2 or 4 bits quantization, but got: " + quantizeBits
                );
            };
            format = new ESNextDiskBBQVectorsFormat(
                encoding,
                args.ivfClusterSize(),
                args.secondaryClusterSize() == -1
                    ? ES920DiskBBQVectorsFormat.DEFAULT_CENTROIDS_PER_PARENT_CLUSTER
                    : args.secondaryClusterSize(),
                elementType,
                args.onDiskRescore(),
                exec,
                exec != null ? args.numMergeWorkers() : 1,
                args.doPrecondition(),
                args.preconditioningBlockDims()
            );
        } else if (args.indexType() == IndexType.GPU_HNSW) {
            if (quantizeBits == 32) {
                format = new ES92GpuHnswVectorsFormat();
            } else if (quantizeBits == 7) {
                format = new ES92GpuHnswSQVectorsFormat();
            } else {
                throw new IllegalArgumentException("GPU HNSW index type only supports 7 or 32 bits quantization, but got: " + quantizeBits);
            }
        } else {
            if (quantizeBits == 1) {
                if (args.indexType() == IndexType.FLAT) {
                    format = new ES93BinaryQuantizedVectorsFormat(elementType, false);
                } else {
                    format = new ES93HnswBinaryQuantizedVectorsFormat(
                        args.hnswM(),
                        args.hnswEfConstruction(),
                        elementType,
                        false,
                        exec != null ? args.numMergeWorkers() : 1,
                        exec
                    );
                }
            } else if (quantizeBits < 32) {
                if (args.indexType() == IndexType.FLAT) {
                    format = new ES93ScalarQuantizedVectorsFormat(elementType, null, quantizeBits, true, false);
                } else {
                    format = new ES93HnswScalarQuantizedVectorsFormat(
                        args.hnswM(),
                        args.hnswEfConstruction(),
                        elementType,
                        null,
                        quantizeBits,
                        true,
                        false,
                        exec != null ? args.numMergeWorkers() : 1,
                        exec
                    );
                }
            } else {
                format = new ES93HnswVectorsFormat(
                    args.hnswM(),
                    args.hnswEfConstruction(),
                    elementType,
                    exec != null ? args.numMergeWorkers() : 1,
                    exec
                );
            }
        }
        return new Lucene103Codec() {
            @Override
            public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
                return new KnnVectorsFormat(format.getName()) {
                    @Override
                    public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
                        return format.fieldsWriter(state);
                    }

                    @Override
                    public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
                        return format.fieldsReader(state);
                    }

                    @Override
                    public int getMaxDimensions(String fieldName) {
                        return MAX_DIMS_COUNT;
                    }

                    @Override
                    public String toString() {
                        return format.toString();
                    }
                };
            }
        };
    }

    private record ParsedArgs(boolean help, String configPath, int warmUpIterations) {

    }

    private static ParsedArgs parseArgs(String[] args) {
        boolean help = false;
        String configFile = null;
        int warmUpIterations = 1;

        if (args.length > 2) {
            return null; // invalid options
        }

        for (var arg : args) {
            if (arg.equals("-h") || arg.equals("--help")) {
                help = true;
            } else if (arg.startsWith("--warmUp=")) {
                warmUpIterations = Integer.parseInt(arg.substring("--warmUp=".length()));
            } else {
                configFile = arg;
            }
        }

        if (configFile == null) {
            return null; // config file required
        }

        return new ParsedArgs(help, configFile, warmUpIterations);
    }

    /**
     * Main method to run the KNN index tester.
     * It parses command line arguments, creates the index, and runs searches if specified.
     *
     * @param args Command line arguments
     * @throws Exception If an error occurs during index creation or search
     */
    public static void main(String[] args) throws Exception {
        var parsedArgs = parseArgs(args);
        if (parsedArgs == null || parsedArgs.help()) {
            // printout an example configuration formatted file and indicate that it is required
            System.out.println("Usage: java -cp <your-classpath> org.elasticsearch.test.knn.KnnIndexTester <config-file> [--warmUp]");
            System.out.println("Where <config-file> is a JSON file containing one or more configurations for the KNN index tester.");
            System.out.println("--warmUp is the number of warm up iterations");
            System.out.println();
            System.out.println("Run multiple searches with different configurations by adding extra values to the array parameters.");
            System.out.println("Every combination of each parameter will be run.");
            System.out.println();
            System.out.println(TestConfiguration.formattedParameterHelp());
            System.out.println();
            System.out.println(
                "This example configuration runs 4 searches with different combinations of num_candidates and early_termination:"
            );
            System.out.println(TestConfiguration.exampleFormatForHelp());
            return;
        }

        Path jsonConfigPath = PathUtils.get(parsedArgs.configPath());
        if (Files.exists(jsonConfigPath) == false) {
            throw new IllegalArgumentException("JSON config file does not exist: " + jsonConfigPath);
        }

        logger.info("Using configuration file: " + jsonConfigPath);
        // Parse the JSON config file to get command line arguments
        // This assumes that the JSON file is the correct format
        List<TestConfiguration> testConfigurationList = new ArrayList<>();
        try (
            InputStream jsonStream = Files.newInputStream(jsonConfigPath);
            XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, jsonStream)
        ) {
            // check if the parser is at the start of an object if so, we only have one set of arguments
            if (parser.currentToken() == null && parser.nextToken() == XContentParser.Token.START_OBJECT) {
                testConfigurationList.add(TestConfiguration.fromXContent(parser));
            } else if (parser.currentToken() == XContentParser.Token.START_ARRAY) {
                // if the parser is at the start of an array, we have multiple sets of arguments
                while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                    testConfigurationList.add(TestConfiguration.fromXContent(parser));
                }
            } else {
                throw new IllegalArgumentException("Invalid JSON format in config file: " + jsonConfigPath);
            }
        }
        FormattedResults formattedResults = new FormattedResults();

        for (TestConfiguration testConfiguration : testConfigurationList) {
            String indexPathName = formatIndexPath(testConfiguration);
            String indexType = testConfiguration.indexType().name().toLowerCase(Locale.ROOT);
            Results indexResults = new Results(indexPathName, indexType, testConfiguration.numDocs());
            Results[] results = new Results[testConfiguration.numberOfSearchRuns()];
            for (int i = 0; i < results.length; i++) {
                results[i] = new Results(indexPathName, indexType, testConfiguration.numDocs());
            }
            logger.info("Running with Java: " + Runtime.version());
            logger.info("Running KNN index tester with arguments: " + testConfiguration);
            final ExecutorService exec;
            if (testConfiguration.numMergeWorkers() > 1) {
                exec = Executors.newFixedThreadPool(testConfiguration.numMergeWorkers(), new NamedThreadFactory("vector-merge"));
            } else {
                exec = null;
            }
            try {
                Codec codec = createCodec(testConfiguration, exec);
                Path indexPath = PathUtils.get(indexPathName);
                MergePolicy mergePolicy = getMergePolicy(testConfiguration);
                if (testConfiguration.reindex() || testConfiguration.forceMerge()) {
                    KnnIndexer knnIndexer = new KnnIndexer(
                        testConfiguration.docVectors(),
                        indexPath,
                        codec,
                        testConfiguration.indexThreads(),
                        testConfiguration.vectorEncoding().luceneEncoding,
                        testConfiguration.dimensions(),
                        testConfiguration.vectorSpace(),
                        testConfiguration.numDocs(),
                        mergePolicy,
                        testConfiguration.writerBufferSizeInMb(),
                        testConfiguration.writerMaxBufferedDocs()
                    );
                    if (testConfiguration.reindex() == false && Files.exists(indexPath) == false) {
                        throw new IllegalArgumentException("Index path does not exist: " + indexPath);
                    }
                    if (testConfiguration.reindex()) {
                        knnIndexer.createIndex(indexResults);
                    }
                    if (testConfiguration.forceMerge()) {
                        knnIndexer.forceMerge(indexResults, testConfiguration.forceMergeMaxNumSegments());
                    }
                }
                numSegments(indexPath, indexResults);
                if (testConfiguration.queryVectors() != null && testConfiguration.numQueries() > 0) {
                    if (parsedArgs.warmUpIterations() > 0) {
                        logger.info("Running the searches for " + parsedArgs.warmUpIterations() + " warm up iterations");
                    }
                    // Warm up
                    for (int warmUpCount = 0; warmUpCount < parsedArgs.warmUpIterations(); warmUpCount++) {
                        for (int i = 0; i < results.length; i++) {
                            var ignoreResults = new Results(indexPathName, indexType, testConfiguration.numDocs());
                            KnnSearcher knnSearcher = new KnnSearcher(indexPath, testConfiguration);
                            knnSearcher.runSearch(ignoreResults, testConfiguration.searchParams().get(i));
                        }
                    }

                    for (int i = 0; i < results.length; i++) {
                        KnnSearcher knnSearcher = new KnnSearcher(indexPath, testConfiguration);
                        knnSearcher.runSearch(results[i], testConfiguration.searchParams().get(i));
                    }
                }
                formattedResults.queryResults.addAll(List.of(results));
                formattedResults.indexResults.add(indexResults);
            } finally {
                if (exec != null) {
                    exec.shutdown();
                }
            }
        }
        logger.info("Results: \n" + formattedResults);
    }

    private static MergePolicy getMergePolicy(TestConfiguration args) {
        return switch (args.mergePolicy()) {
            case null -> null;
            case TIERED -> new TieredMergePolicy();
            case LOG_BYTE -> new LogByteSizeMergePolicy();
            case NO -> NoMergePolicy.INSTANCE;
            case LOG_DOC -> new LogDocMergePolicy();
        };
    }

    static void numSegments(Path indexPath, Results result) {
        try (FSDirectory dir = FSDirectory.open(indexPath); IndexReader reader = DirectoryReader.open(dir)) {
            result.numSegments = reader.leaves().size();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to get segment count for index at " + indexPath, e);
        }
    }

    static class FormattedResults {
        List<Results> indexResults = new ArrayList<>();
        List<Results> queryResults = new ArrayList<>();

        @Override
        public String toString() {
            if (indexResults.isEmpty() && queryResults.isEmpty()) {
                return "No results available.";
            }

            String[] indexingHeaders = {
                "index_name",
                "index_type",
                "num_docs",
                "doc_add_time(ms)",
                "total_index_time(ms)",
                "force_merge_time(ms)",
                "num_segments" };

            // Define column headers
            String[] searchHeaders = {
                "index_name",
                "index_type",
                "visit_percentage(%)",
                "latency(ms)",
                "net_cpu_time(ms)",
                "avg_cpu_count",
                "QPS",
                "recall",
                "visited",
                "filter_selectivity",
                "filter_cached",
                "oversampling_factor",
                "num_candidates",
                "early_termination" };

            // Calculate appropriate column widths based on headers and data

            StringBuilder sb = new StringBuilder();

            String[][] indexResultsArray = new String[indexResults.size()][];
            for (int i = 0; i < indexResults.size(); i++) {
                Results indexResult = indexResults.get(i);
                indexResultsArray[i] = new String[] {
                    indexResult.indexName,
                    indexResult.indexType,
                    Integer.toString(indexResult.numDocs),
                    Long.toString(indexResult.docAddTimeMS),
                    Long.toString(indexResult.indexTimeMS),
                    Long.toString(indexResult.forceMergeTimeMS),
                    Integer.toString(indexResult.numSegments) };
            }
            printBlock(sb, indexingHeaders, indexResultsArray);
            String[][] queryResultsArray = new String[queryResults.size()][];
            for (int i = 0; i < queryResults.size(); i++) {
                Results queryResult = queryResults.get(i);
                queryResultsArray[i] = new String[] {
                    queryResult.indexName,
                    queryResult.indexType,
                    String.format(Locale.ROOT, "%.3f", queryResult.visitPercentage),
                    String.format(Locale.ROOT, "%.2f", queryResult.avgLatency),
                    String.format(Locale.ROOT, "%.2f", queryResult.netCpuTimeMS),
                    String.format(Locale.ROOT, "%.2f", queryResult.avgCpuCount),
                    String.format(Locale.ROOT, "%.2f", queryResult.qps),
                    String.format(Locale.ROOT, "%.2f", queryResult.avgRecall),
                    String.format(Locale.ROOT, "%.2f", queryResult.averageVisited),
                    String.format(Locale.ROOT, "%.2f", queryResult.filterSelectivity),
                    Boolean.toString(queryResult.filterCached),
                    String.format(Locale.ROOT, "%.2f", queryResult.overSamplingFactor),
                    String.format(Locale.ROOT, "%d", queryResult.numCandidates),
                    Boolean.toString(queryResult.earlyTermination) };
            }

            printBlock(sb, searchHeaders, queryResultsArray);

            return sb.toString();
        }

        private void printBlock(StringBuilder sb, String[] headers, String[][] rows) {
            int[] widths = calculateColumnWidths(headers, rows);
            sb.append("\n");
            sb.append(formatRow(headers, widths));
            sb.append("\n");

            // Add separator line
            for (int width : widths) {
                sb.append("-".repeat(width)).append("  ");
            }
            sb.append("\n");

            for (String[] row : rows) {
                sb.append(formatRow(row, widths));
                sb.append("\n");
            }
        }

        // Helper method to format a single row with proper column widths
        private String formatRow(String[] values, int[] widths) {
            StringBuilder row = new StringBuilder();
            for (int i = 0; i < values.length; i++) {
                // Left-align text column (index_type), right-align numeric columns
                String format = (i == 0) ? "%-" + widths[i] + "s" : "%" + widths[i] + "s";
                row.append(Strings.format(format, values[i]));

                // Add separation between columns
                if (i < values.length - 1) {
                    row.append("  ");
                }
            }
            return row.toString();
        }

        // Calculate appropriate column widths based on headers and data
        private int[] calculateColumnWidths(String[] headers, String[]... data) {
            int[] widths = new int[headers.length];

            // Initialize widths with header lengths
            for (int i = 0; i < headers.length; i++) {
                widths[i] = headers[i].length();
            }

            // Update widths based on data
            for (String[] values : data) {
                for (int i = 0; i < values.length; i++) {
                    widths[i] = Math.max(widths[i], values[i].length());
                }
            }

            return widths;
        }
    }

    static class Results {
        final String indexType, indexName;
        public long docAddTimeMS;
        int numDocs;
        float filterSelectivity;
        long indexTimeMS;
        long forceMergeTimeMS;
        int numSegments;
        double visitPercentage;
        double avgLatency;
        double qps;
        double avgRecall;
        double averageVisited;
        double netCpuTimeMS;
        double avgCpuCount;
        boolean filterCached;
        double overSamplingFactor;
        boolean earlyTermination;
        int numCandidates;

        Results(String indexName, String indexType, int numDocs) {
            this.indexName = indexName;
            this.indexType = indexType;
            this.numDocs = numDocs;
        }
    }

    static final class ThreadDetails {
        private static final ThreadMXBean threadBean = (ThreadMXBean) ManagementFactory.getThreadMXBean();
        public final long[] threadIDs;
        public final long[] cpuTimesNS;
        public final ThreadInfo[] threadInfos;
        public final long ns;

        ThreadDetails() {
            ns = System.nanoTime();
            threadIDs = threadBean.getAllThreadIds();
            cpuTimesNS = threadBean.getThreadCpuTime(threadIDs);
            threadInfos = threadBean.getThreadInfo(threadIDs);
        }
    }
}

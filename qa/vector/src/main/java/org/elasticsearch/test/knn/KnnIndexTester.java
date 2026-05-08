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
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.search.Sort;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.NamedThreadFactory;
import org.elasticsearch.cli.ProcessInfo;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.gpu.codec.ES92GpuHnswSQVectorsFormat;
import org.elasticsearch.gpu.codec.ES92GpuHnswVectorsFormat;
import org.elasticsearch.index.codec.vectors.diskbbq.ES920DiskBBQVectorsFormat;
import org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskBBQVectorsFormat;
import org.elasticsearch.index.codec.vectors.es93.ES93BinaryQuantizedVectorsFormat;
import org.elasticsearch.index.codec.vectors.es93.ES93FlatVectorFormat;
import org.elasticsearch.index.codec.vectors.es93.ES93HnswBinaryQuantizedVectorsFormat;
import org.elasticsearch.index.codec.vectors.es93.ES93HnswVectorsFormat;
import org.elasticsearch.index.codec.vectors.es94.ES94HnswScalarQuantizedVectorsFormat;
import org.elasticsearch.index.codec.vectors.es94.ES94ScalarQuantizedVectorsFormat;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.logging.Level;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.test.knn.data.DataGenerator;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;

import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.MAX_DIMS_COUNT;

/**
 * A utility class to create and test KNN indices using Lucene.
 * It supports various index types (HNSW, FLAT, IVF) and configurations.
 */
public class KnnIndexTester {
    public static final Logger logger;

    static {
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
        FLAT,
        HNSW,
        IVF,
        GPU_HNSW
    }

    public enum VectorEncoding {
        BYTE(org.apache.lucene.index.VectorEncoding.BYTE, DenseVectorFieldMapper.ElementType.BYTE),
        FLOAT32(org.apache.lucene.index.VectorEncoding.FLOAT32, DenseVectorFieldMapper.ElementType.FLOAT),
        BFLOAT16(org.apache.lucene.index.VectorEncoding.FLOAT32, DenseVectorFieldMapper.ElementType.BFLOAT16);

        private final org.apache.lucene.index.VectorEncoding luceneEncoding;
        private final DenseVectorFieldMapper.ElementType elementType;

        VectorEncoding(org.apache.lucene.index.VectorEncoding luceneEncoding, DenseVectorFieldMapper.ElementType elementType) {
            this.luceneEncoding = luceneEncoding;
            this.elementType = elementType;
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

    /**
     * Factory that creates a directory for a given index path.
     */
    @FunctionalInterface
    interface DirectoryFactory {
        Directory create(Path indexPath) throws IOException;
    }

    record DirectoryTypeConfig(DirectoryFactory factory, boolean shared, boolean preWarm, BiConsumer<Directory, String> diagnosticLogger) {
        private static final BiConsumer<Directory, String> NOOP = (a, b) -> {};

        DirectoryTypeConfig(DirectoryFactory factory, boolean shared, boolean preWarm) {
            this(factory, shared, preWarm, NOOP);
        }
    }

    private static final Map<String, DirectoryTypeConfig> directoryTypeRegistry = new ConcurrentHashMap<>(3);

    static {
        directoryTypeRegistry.put("default", new DirectoryTypeConfig(KnnIndexer::getDirectory, false, false));
        directoryTypeRegistry.put("frozen", new DirectoryTypeConfig(KnnIndexer::openFrozenDirectory, false, true));
        directoryTypeRegistry.put(
            "stateless",
            new DirectoryTypeConfig(KnnIndexer::openStatelessDirectory, true, true, KnnIndexer::logStatelessCacheStats)
        );
    }

    static DirectoryTypeConfig getDirectoryTypeConfig(String name) {
        DirectoryTypeConfig config = directoryTypeRegistry.get(name);
        if (config == null) {
            throw new IllegalArgumentException("Unknown directory_type: '" + name + "'. Known types: " + directoryTypeRegistry.keySet());
        }
        return config;
    }

    private static String formatIndexPath(TestConfiguration args) {
        List<String> suffix = new ArrayList<>();
        switch (args.indexType()) {
            case FLAT -> suffix.add("flat");
            case GPU_HNSW -> suffix.add("gpu_hnsw");
            case IVF -> {
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
            }
            case HNSW -> {
                suffix.add(Integer.toString(args.hnswM()));
                suffix.add(Integer.toString(args.hnswEfConstruction()));
                if (args.quantizeBits() != null) {
                    suffix.add(Integer.toString(args.quantizeBits()));
                }
            }
        }

        return INDEX_DIR + "/" + args.docVectors().getFirst().getFileName() + "-" + String.join("-", suffix) + ".index";
    }

    static Codec createCodec(TestConfiguration args, @Nullable ExecutorService exec) {
        final KnnVectorsFormat format;
        Integer quantizeBits = args.quantizeBits();
        DenseVectorFieldMapper.ElementType elementType = args.vectorEncoding().elementType;
        int mergeWorkers = exec != null ? args.numMergeWorkers() : 1;

        format = switch (args.indexType()) {
            case IVF -> {
                var encoding = ESNextDiskBBQVectorsFormat.QuantEncoding.fromBits(quantizeBits.byteValue());
                // Use flatVectorThreshold from config, or default to -1 (dynamic) if not specified
                int flatVectorThreshold = args.flatVectorThreshold() >= 0 ? args.flatVectorThreshold() : -1;
                yield new ESNextDiskBBQVectorsFormat(
                    encoding,
                    args.ivfClusterSize(),
                    args.secondaryClusterSize() == -1
                        ? ES920DiskBBQVectorsFormat.DEFAULT_CENTROIDS_PER_PARENT_CLUSTER
                        : args.secondaryClusterSize(),
                    elementType,
                    args.onDiskRescore(),
                    exec,
                    mergeWorkers,
                    args.doPrecondition(),
                    args.preconditioningBlockDims(),
                    flatVectorThreshold,
                    args.datasetConfig().isSliced() ? KnnIndexer.PARTITION_ID_FIELD : null
                );
            }
            case GPU_HNSW -> switch (quantizeBits) {
                case null -> new ES92GpuHnswVectorsFormat();
                case 7 -> new ES92GpuHnswSQVectorsFormat();
                default -> throw new IllegalArgumentException(
                    "GPU HNSW index type only supports 7 bits quantization, but got: " + quantizeBits
                );
            };
            case HNSW -> switch (quantizeBits) {
                case null -> new ES93HnswVectorsFormat(
                    args.hnswM(),
                    args.hnswEfConstruction(),
                    elementType,
                    mergeWorkers,
                    exec,
                    args.flatVectorThreshold()
                );
                case 1 -> new ES93HnswBinaryQuantizedVectorsFormat(
                    args.hnswM(),
                    args.hnswEfConstruction(),
                    elementType,
                    false,
                    mergeWorkers,
                    exec,
                    args.flatVectorThreshold()
                );
                default -> new ES94HnswScalarQuantizedVectorsFormat(
                    args.hnswM(),
                    args.hnswEfConstruction(),
                    elementType,
                    quantizeBits,
                    false,
                    mergeWorkers,
                    exec,
                    args.flatVectorThreshold()
                );
            };
            case FLAT -> switch (quantizeBits) {
                case null -> new ES93FlatVectorFormat(elementType);
                case 1 -> new ES93BinaryQuantizedVectorsFormat(elementType, false);
                default -> new ES94ScalarQuantizedVectorsFormat(elementType, quantizeBits, false);
            };
        };

        logger.info("Using format {}", format.getName());

        return new Lucene104Codec() {
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
            System.out.println("Available datasets:");
            try {
                System.out.println(TestConfiguration.listDatasets());
            } catch (Exception e) {
                System.out.println("Failed to list datasets: " + e.getMessage());
            }
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
        String rawConfigJson = Files.readString(jsonConfigPath);
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
        LocalDateTime runStart = LocalDateTime.now();

        for (TestConfiguration testConfiguration : testConfigurationList) {
            // check this here so IVF/GPUHNSW can guarantee quantizeBits is set properly
            checkQuantizeBits(testConfiguration);
            String indexPathName = formatIndexPath(testConfiguration);
            String indexType = testConfiguration.indexType().name().toLowerCase(Locale.ROOT);
            Results indexResults = new Results(indexPathName, indexType, testConfiguration.numDocs());
            Results[] results = new Results[testConfiguration.numberOfSearchRuns()];
            Arrays.setAll(results, i -> new Results(indexPathName, indexType, testConfiguration.numDocs()));
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
                DirectoryTypeConfig dirConfig = getDirectoryTypeConfig(testConfiguration.directoryType());

                runTestConfiguration(
                    testConfiguration,
                    indexPath,
                    codec,
                    mergePolicy,
                    dirConfig,
                    indexResults,
                    results,
                    parsedArgs,
                    indexPathName,
                    indexType
                );
                formattedResults.queryResults.addAll(List.of(results));
                formattedResults.indexResults.add(indexResults);
            } finally {
                if (exec != null) {
                    exec.shutdown();
                }
            }
        }
        logger.info("Results: \n" + formattedResults);
        GitInfo gitInfo = captureGitInfo();
        Path dumpFile = writeResultsDump(jsonConfigPath, rawConfigJson, formattedResults, gitInfo, runStart);
        Path csvFile = appendResultsCsv(jsonConfigPath, testConfigurationList, formattedResults, gitInfo, runStart);
        List<String> outputPaths = new ArrayList<>();
        if (dumpFile != null) outputPaths.add(dumpFile.toString());
        if (csvFile != null) outputPaths.add(csvFile.toString());
        if (outputPaths.isEmpty() == false) {
            logger.info("Output files written:\n  {}", String.join("\n  ", outputPaths));
        }
    }

    /**
     * Bundles the vector reader, document factory and total doc count
     * needed to create an index. Created via {@link DataGenerator#createIndexingSetup()}.
     */
    public record IndexingSetup(IndexVectorReader reader, KnnIndexer.DocumentFactory factory, int totalDocs) implements Closeable {

        @Override
        public void close() throws IOException {
            reader.close();
        }
    }

    /**
     * Runs indexing, merge, and search phases using the given directory configuration.
     * When {@code dirConfig.shared()} is true, a single directory instance is used for all
     * phases. Otherwise, separate directories are used for write and read.
     */
    private static void runTestConfiguration(
        TestConfiguration testConfiguration,
        Path indexPath,
        Codec codec,
        MergePolicy mergePolicy,
        DirectoryTypeConfig dirConfig,
        Results indexResults,
        Results[] results,
        ParsedArgs parsedArgs,
        String indexPathName,
        String indexType
    ) throws Exception {
        Directory sharedDir = dirConfig.shared() ? dirConfig.factory().create(indexPath) : null;
        try {
            DataGenerator dataGenerator = testConfiguration.datasetConfig().createDataGenerator(testConfiguration);

            if (testConfiguration.reindex() || testConfiguration.forceMerge()) {
                KnnIndexer knnIndexer = new KnnIndexer(
                    testConfiguration.docVectors(),
                    indexPath,
                    codec,
                    testConfiguration.indexThreads(),
                    testConfiguration.vectorEncoding().luceneEncoding,
                    testConfiguration.dimensions(),
                    testConfiguration.vectorSpace(),
                    testConfiguration.normalizeVectors(),
                    testConfiguration.numDocs(),
                    mergePolicy,
                    testConfiguration.writerBufferSizeInMb(),
                    testConfiguration.writerMaxBufferedDocs()
                );
                if (testConfiguration.reindex()) {
                    Directory writeDir = sharedDir != null ? sharedDir : dirConfig.factory().create(indexPath);
                    try (var setup = dataGenerator.createIndexingSetup()) {
                        knnIndexer.createIndex(
                            indexResults,
                            writeDir,
                            setup.reader(),
                            setup.factory(),
                            setup.totalDocs(),
                            dataGenerator.getIndexSort()
                        );
                    } finally {
                        if (writeDir != sharedDir) {
                            writeDir.close();
                        }
                    }
                } else if (Files.exists(indexPath) == false) {
                    throw new IllegalArgumentException("Index path does not exist: " + indexPath);
                }
                if (testConfiguration.forceMerge()) {
                    forceMerge(knnIndexer, indexResults, sharedDir, testConfiguration, dataGenerator.getIndexSort());
                }
            }
            numSegments(indexPath, indexResults, sharedDir);

            boolean hasQueries = testConfiguration.numQueries() > 0 && dataGenerator.numQueries() > 0;
            if (hasQueries) {
                Directory readDir = sharedDir != null ? sharedDir : dirConfig.factory().create(indexPath);
                try {
                    if (dirConfig.preWarm()) {
                        logDiagnostics(dirConfig, readDir, "Before prewarm");
                        KnnSearcher.preWarmDirectory(readDir);
                        logDiagnostics(dirConfig, readDir, "After prewarm");
                    }
                    runSearches(testConfiguration, indexPath, readDir, results, parsedArgs, indexPathName, indexType, dataGenerator);
                    logDiagnostics(dirConfig, readDir, "After search");
                } finally {
                    if (readDir != sharedDir) {
                        readDir.close();
                    }
                }
            }
        } finally {
            if (sharedDir != null) {
                sharedDir.close();
            }
        }
    }

    static void forceMerge(
        KnnIndexer knnIndexer,
        Results indexResults,
        Directory sharedDir,
        TestConfiguration testConfiguration,
        Sort indexSort
    ) throws Exception {
        if (sharedDir != null) {
            knnIndexer.forceMerge(indexResults, testConfiguration.forceMergeMaxNumSegments(), sharedDir, indexSort);
        } else {
            knnIndexer.forceMerge(indexResults, testConfiguration.forceMergeMaxNumSegments(), indexSort);
        }
    }

    private static void logDiagnostics(DirectoryTypeConfig dirConfig, Directory dir, String label) {
        dirConfig.diagnosticLogger().accept(dir, label);
    }

    static void numSegments(Path indexPath, Results indexResults, Directory sharedDir) throws IOException {
        if (sharedDir != null) {
            numSegments(sharedDir, indexResults);
        } else {
            numSegments(indexPath, indexResults);
        }
    }

    private static void runSearches(
        TestConfiguration testConfiguration,
        Path indexPath,
        Directory dir,
        Results[] results,
        ParsedArgs parsedArgs,
        String indexPathName,
        String indexType,
        DataGenerator dataGenerator
    ) throws Exception {
        if (parsedArgs.warmUpIterations() > 0) {
            logger.info("Running the searches for " + parsedArgs.warmUpIterations() + " warm up iterations");
        }
        for (int warmUpCount = 0; warmUpCount < parsedArgs.warmUpIterations(); warmUpCount++) {
            for (int i = 0; i < results.length; i++) {
                var ignoreResults = new Results(indexPathName, indexType, testConfiguration.numDocs());
                KnnSearcher knnSearcher = new KnnSearcher(indexPath, testConfiguration);
                var setup = dataGenerator.createSearchSetup(knnSearcher, testConfiguration.searchParams().get(i));
                knnSearcher.search(ignoreResults, testConfiguration.searchParams().get(i), dir, setup);
            }
        }
        for (int i = 0; i < results.length; i++) {
            KnnSearcher knnSearcher = new KnnSearcher(indexPath, testConfiguration);
            var setup = dataGenerator.createSearchSetup(knnSearcher, testConfiguration.searchParams().get(i));
            knnSearcher.search(results[i], testConfiguration.searchParams().get(i), dir, setup);
        }
    }

    private static void checkQuantizeBits(TestConfiguration args) {
        switch (args.indexType()) {
            case IVF:
                if (args.quantizeBits() == null || !Set.of(1, 2, 4, 7).contains(args.quantizeBits())) {
                    throw new IllegalArgumentException(
                        "IVF index type only supports 1, 2, 4 or 7 bits quantization, but got: " + args.quantizeBits()
                    );
                }
                break;
            case GPU_HNSW: {
                if (args.quantizeBits() != null && args.quantizeBits() != 7) {
                    throw new IllegalArgumentException(
                        "GPU HNSW index type only supports 7 bits quantization, but got: " + args.quantizeBits()
                    );
                }
            }
        }
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

    static void numSegments(Path indexPath, Results result) throws IOException {
        try (Directory dir = KnnIndexer.getDirectory(indexPath); IndexReader reader = DirectoryReader.open(dir)) {
            result.numSegments = reader.leaves().size();
        } catch (IOException e) {
            throw new IOException("Failed to get segment count for index at " + indexPath, e);
        }
    }

    static void numSegments(Directory dir, Results result) throws IOException {
        try (IndexReader reader = DirectoryReader.open(dir)) {
            result.numSegments = reader.leaves().size();
        } catch (IOException e) {
            throw new IOException("Failed to get segment count for dir: " + dir, e);
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

            // Only include partition recall columns if any result has partition data
            boolean hasPartitionRecall = queryResults.stream()
                .anyMatch(r -> r.perPartitionRecall != null && r.perPartitionRecall.isEmpty() == false);

            List<String> searchHeaderList = CollectionUtils.arrayAsArrayList(
                "index_name",
                "index_type",
                "num_segments",
                "visit_percentage(%)",
                "actual_visit(%)",
                "latency(ms)",
                "net_cpu_time(ms)",
                "avg_cpu_count",
                "QPS",
                "recall",
                "top_k",
                "visited",
                "filter_selectivity",
                "filter_cached",
                "oversampling_factor",
                "num_candidates",
                "early_termination"
            );
            if (hasPartitionRecall) {
                searchHeaderList.add("partition_recall_min");
                searchHeaderList.add("partition_recall_max");
                searchHeaderList.add("partition_recall_avg");
            }
            String[] searchHeaders = searchHeaderList.toArray(String[]::new);

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
                List<String> row = CollectionUtils.arrayAsArrayList(
                    queryResult.indexName,
                    queryResult.indexType,
                    Integer.toString(queryResult.numSegments),
                    String.format(Locale.ROOT, "%.3f", queryResult.visitPercentage),
                    String.format(Locale.ROOT, "%.3f", queryResult.actualVisitPercentage),
                    String.format(Locale.ROOT, "%.2f", queryResult.avgLatency),
                    String.format(Locale.ROOT, "%.2f", queryResult.netCpuTimeMS),
                    String.format(Locale.ROOT, "%.2f", queryResult.avgCpuCount),
                    String.format(Locale.ROOT, "%.2f", queryResult.qps),
                    String.format(Locale.ROOT, "%.2f", queryResult.avgRecall),
                    String.format(Locale.ROOT, "%d", queryResult.topK),
                    String.format(Locale.ROOT, "%.2f", queryResult.averageVisited),
                    String.format(Locale.ROOT, "%.2f", queryResult.filterSelectivity),
                    Boolean.toString(queryResult.filterCached),
                    String.format(Locale.ROOT, "%.2f", queryResult.overSamplingFactor),
                    String.format(Locale.ROOT, "%d", queryResult.numCandidates),
                    Boolean.toString(queryResult.earlyTermination)
                );
                if (hasPartitionRecall) {
                    String partitionMin = "";
                    String partitionMax = "";
                    String partitionAvg = "";
                    if (queryResult.perPartitionRecall != null && queryResult.perPartitionRecall.isEmpty() == false) {
                        var stats = queryResult.perPartitionRecall.values().stream().mapToDouble(Float::doubleValue).summaryStatistics();
                        partitionMin = String.format(Locale.ROOT, "%.4f", stats.getMin());
                        partitionMax = String.format(Locale.ROOT, "%.4f", stats.getMax());
                        partitionAvg = String.format(Locale.ROOT, "%.4f", stats.getAverage());
                    }
                    row.add(partitionMin);
                    row.add(partitionMax);
                    row.add(partitionAvg);
                }
                queryResultsArray[i] = row.toArray(String[]::new);
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
        int totalIndexVectors;
        double visitPercentage;
        double actualVisitPercentage;
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
        int topK;
        Map<String, Float> perPartitionRecall;

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

    private record GitInfo(String branch, String commit) {}

    private static GitInfo captureGitInfo() {
        try {
            String branch = runProcess("git", "branch", "--show-current").trim();
            String commit = runProcess("git", "log", "-1", "--format=%h %s").trim();
            return new GitInfo(branch, commit);
        } catch (Exception e) {
            logger.debug("Could not capture git info: {}", e.getMessage());
            return new GitInfo("", "");
        }
    }

    private static String runProcess(String... command) throws IOException, InterruptedException {
        Process p = new ProcessBuilder(command).redirectErrorStream(true).start();
        String output = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        p.waitFor();
        return output;
    }

    private static Path writeResultsDump(
        Path configFilePath,
        String rawConfigJson,
        FormattedResults formattedResults,
        GitInfo gitInfo,
        LocalDateTime timestamp
    ) {
        try {
            Path outDir = PathUtils.get("target/knn_results");
            Files.createDirectories(outDir);
            String configFileName = configFilePath.getFileName().toString();
            int dotIdx = configFileName.lastIndexOf('.');
            String configBaseName = dotIdx > 0 ? configFileName.substring(0, dotIdx) : configFileName;
            String fileTs = timestamp.format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss"));
            Path outFile = outDir.resolve(fileTs + "_" + configBaseName + ".txt");

            StringBuilder sb = new StringBuilder();
            sb.append("=".repeat(80)).append("\n");
            sb.append("  KNN Index Tester Results\n");
            sb.append("=".repeat(80)).append("\n");
            sb.append("Timestamp:   ").append(timestamp.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)).append("\n");
            sb.append("Config file: ").append(configFilePath.toAbsolutePath()).append("\n\n");

            sb.append("--- System Information ").append("-".repeat(58)).append("\n");
            sb.append("OS:          ")
                .append(System.getProperty("os.name"))
                .append(" ")
                .append(System.getProperty("os.version"))
                .append(" (")
                .append(System.getProperty("os.arch"))
                .append(")\n");
            sb.append("JVM:         ")
                .append(System.getProperty("java.vm.name"))
                .append(" ")
                .append(System.getProperty("java.version"))
                .append(" (")
                .append(System.getProperty("java.vendor"))
                .append(")\n");
            sb.append("Processors:  ").append(Runtime.getRuntime().availableProcessors()).append("\n");
            sb.append("Heap max:    ").append(JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() / (1024 * 1024)).append(" MB\n\n");

            sb.append("--- Git Information ").append("-".repeat(61)).append("\n");
            if (gitInfo.branch().isEmpty() && gitInfo.commit().isEmpty()) {
                sb.append("(unavailable)\n\n");
            } else {
                sb.append("Branch: ").append(gitInfo.branch()).append("\n");
                sb.append("Commit: ").append(gitInfo.commit()).append("\n\n");
            }

            sb.append("--- Configuration ").append("-".repeat(62)).append("\n");
            sb.append(rawConfigJson).append("\n\n");

            sb.append("--- Results ").append("-".repeat(68)).append("\n");
            sb.append(formattedResults);
            sb.append("\n").append("=".repeat(80)).append("\n");

            Files.writeString(outFile, sb.toString());
            return outFile.toAbsolutePath();
        } catch (IOException e) {
            logger.warn("Failed to write results dump: {}", e.getMessage());
            return null;
        }
    }

    private static final String[] CSV_HEADERS = {
        "timestamp",
        "config_file",
        "git_branch",
        "git_commit",
        "os",
        "jvm_version",
        "processors",
        "heap_max_mb",
        "index_name",
        "index_type",
        "num_docs",
        "num_segments",
        "quantize_bits",
        "vector_encoding",
        "vector_space",
        "hnsw_m",
        "hnsw_ef_construction",
        "ivf_cluster_size",
        "secondary_cluster_size",
        "merge_policy",
        "on_disk_rescore",
        "precondition",
        "flat_vector_threshold",
        "top_k",
        "num_candidates",
        "visit_percentage",
        "over_sampling_factor",
        "early_termination",
        "filter_selectivity",
        "filter_cached",
        "search_threads",
        "num_searchers",
        "doc_add_time_ms",
        "index_time_ms",
        "force_merge_time_ms",
        "recall",
        "qps",
        "avg_latency_ms",
        "net_cpu_time_ms",
        "avg_cpu_count",
        "visited",
        "visit_pct_configured",
        "visit_pct_actual",
        "partition_recall_min",
        "partition_recall_max",
        "partition_recall_avg" };

    private static Path appendResultsCsv(
        Path configFilePath,
        List<TestConfiguration> configs,
        FormattedResults formattedResults,
        GitInfo gitInfo,
        LocalDateTime timestamp
    ) {
        try {
            Path outDir = PathUtils.get("target/knn_results");
            Files.createDirectories(outDir);
            Path csvFile = outDir.resolve("results.csv");
            boolean writeHeader = Files.exists(csvFile) == false || Files.size(csvFile) == 0;

            if (writeHeader == false) {
                String existingHeader;
                try (var lines = Files.lines(csvFile, StandardCharsets.UTF_8)) {
                    existingHeader = lines.findFirst().orElse("");
                }
                if (existingHeader.equals(buildCsvRow(CSV_HEADERS)) == false) {
                    String archiveTs = timestamp.format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss"));
                    Path archiveFile = outDir.resolve("results_archived_" + archiveTs + ".csv");
                    Files.move(csvFile, archiveFile);
                    logger.info("CSV headers changed — archived existing results to: {}", archiveFile.getFileName());
                    writeHeader = true;
                }
            }

            String ts = timestamp.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            String configPath = configFilePath.toAbsolutePath().toString();
            String os = System.getProperty("os.name") + " " + System.getProperty("os.version") + " (" + System.getProperty("os.arch") + ")";
            String jvmVersion = System.getProperty("java.vm.name") + " " + System.getProperty("java.version");
            String processors = Integer.toString(Runtime.getRuntime().availableProcessors());
            String heapMaxMb = Long.toString(JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() / (1024 * 1024));

            try (
                BufferedWriter writer = Files.newBufferedWriter(
                    csvFile,
                    StandardCharsets.UTF_8,
                    StandardOpenOption.APPEND,
                    StandardOpenOption.CREATE
                )
            ) {
                if (writeHeader) {
                    writer.write(buildCsvRow(CSV_HEADERS));
                    writer.newLine();
                }
                int queryResultIdx = 0;
                for (int configIdx = 0; configIdx < configs.size(); configIdx++) {
                    TestConfiguration config = configs.get(configIdx);
                    Results indexResult = formattedResults.indexResults.get(configIdx);
                    for (int searchIdx = 0; searchIdx < config.numberOfSearchRuns(); searchIdx++) {
                        Results qr = formattedResults.queryResults.get(queryResultIdx++);
                        SearchParameters sp = config.searchParams().get(searchIdx);

                        String partMin = "", partMax = "", partAvg = "";
                        if (qr.perPartitionRecall != null && qr.perPartitionRecall.isEmpty() == false) {
                            var stats = qr.perPartitionRecall.values().stream().mapToDouble(Float::doubleValue).summaryStatistics();
                            partMin = String.format(Locale.ROOT, "%.4f", stats.getMin());
                            partMax = String.format(Locale.ROOT, "%.4f", stats.getMax());
                            partAvg = String.format(Locale.ROOT, "%.4f", stats.getAverage());
                        }

                        String[] row = {
                            ts,
                            configPath,
                            gitInfo.branch(),
                            gitInfo.commit(),
                            os,
                            jvmVersion,
                            processors,
                            heapMaxMb,
                            qr.indexName,
                            qr.indexType,
                            Integer.toString(qr.numDocs),
                            Integer.toString(qr.numSegments),
                            config.quantizeBits() != null ? Integer.toString(config.quantizeBits()) : "",
                            config.vectorEncoding().name().toLowerCase(Locale.ROOT),
                            config.vectorSpace() != null ? config.vectorSpace().name().toLowerCase(Locale.ROOT) : "",
                            Integer.toString(config.hnswM()),
                            Integer.toString(config.hnswEfConstruction()),
                            Integer.toString(config.ivfClusterSize()),
                            Integer.toString(config.secondaryClusterSize()),
                            config.mergePolicy() != null ? config.mergePolicy().name().toLowerCase(Locale.ROOT) : "",
                            Boolean.toString(config.onDiskRescore()),
                            Boolean.toString(config.doPrecondition()),
                            Integer.toString(config.flatVectorThreshold()),
                            Integer.toString(sp.topK()),
                            Integer.toString(sp.numCandidates()),
                            String.format(Locale.ROOT, "%.4f", sp.visitPercentage()),
                            String.format(Locale.ROOT, "%.4f", sp.overSamplingFactor()),
                            Boolean.toString(sp.earlyTermination()),
                            String.format(Locale.ROOT, "%.4f", sp.filterSelectivity()),
                            Boolean.toString(sp.filterCached()),
                            Integer.toString(sp.searchThreads()),
                            Integer.toString(sp.numSearchers()),
                            Long.toString(indexResult.docAddTimeMS),
                            Long.toString(indexResult.indexTimeMS),
                            Long.toString(indexResult.forceMergeTimeMS),
                            String.format(Locale.ROOT, "%.4f", qr.avgRecall),
                            String.format(Locale.ROOT, "%.2f", qr.qps),
                            String.format(Locale.ROOT, "%.2f", qr.avgLatency),
                            String.format(Locale.ROOT, "%.2f", qr.netCpuTimeMS),
                            String.format(Locale.ROOT, "%.2f", qr.avgCpuCount),
                            String.format(Locale.ROOT, "%.2f", qr.averageVisited),
                            String.format(Locale.ROOT, "%.4f", qr.visitPercentage),
                            String.format(Locale.ROOT, "%.4f", qr.actualVisitPercentage),
                            partMin,
                            partMax,
                            partAvg };
                        writer.write(buildCsvRow(row));
                        writer.newLine();
                    }
                }
            }
            return csvFile.toAbsolutePath();
        } catch (IOException e) {
            logger.warn("Failed to append results to CSV: {}", e.getMessage());
            return null;
        }
    }

    private static String buildCsvRow(String[] values) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < values.length; i++) {
            if (i > 0) sb.append(',');
            sb.append(csvQuote(values[i]));
        }
        return sb.toString();
    }

    private static String csvQuote(String value) {
        if (value.contains(",") || value.contains("\"") || value.contains("\n") || value.contains("\r")) {
            return "\"" + value.replace("\"", "\"\"") + "\"";
        }
        return value;
    }
}

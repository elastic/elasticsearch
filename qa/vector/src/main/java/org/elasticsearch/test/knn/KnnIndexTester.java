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
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
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
import org.elasticsearch.cli.ProcessInfo;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.gpu.codec.ES92GpuHnswSQVectorsFormat;
import org.elasticsearch.gpu.codec.ES92GpuHnswVectorsFormat;
import org.elasticsearch.index.codec.vectors.ES813Int8FlatVectorFormat;
import org.elasticsearch.index.codec.vectors.ES814HnswScalarQuantizedVectorsFormat;
import org.elasticsearch.index.codec.vectors.diskbbq.ES920DiskBBQVectorsFormat;
import org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskBBQVectorsFormat;
import org.elasticsearch.index.codec.vectors.es818.ES818BinaryQuantizedVectorsFormat;
import org.elasticsearch.index.codec.vectors.es818.ES818HnswBinaryQuantizedVectorsFormat;
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
import java.lang.management.ThreadInfo;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

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

    enum MergePolicyType {
        TIERED,
        LOG_BYTE,
        NO,
        LOG_DOC
    }

    private static String formatIndexPath(CmdLineArgs args) {
        List<String> suffix = new ArrayList<>();
        if (args.indexType() == IndexType.FLAT) {
            suffix.add("flat");
        } else if (args.indexType() == IndexType.GPU_HNSW) {
            suffix.add("gpu_hnsw");
        } else if (args.indexType() == IndexType.IVF) {
            suffix.add("ivf");
            suffix.add(Integer.toString(args.ivfClusterSize()));
        } else {
            suffix.add(Integer.toString(args.hnswM()));
            suffix.add(Integer.toString(args.hnswEfConstruction()));
            if (args.quantizeBits() < 32) {
                suffix.add(Integer.toString(args.quantizeBits()));
            }
        }
        return INDEX_DIR + "/" + args.docVectors().get(0).getFileName() + "-" + String.join("-", suffix) + ".index";
    }

    static Codec createCodec(CmdLineArgs args) {
        final KnnVectorsFormat format;
        int quantizeBits = args.quantizeBits();
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
                ES920DiskBBQVectorsFormat.DEFAULT_CENTROIDS_PER_PARENT_CLUSTER,
                DenseVectorFieldMapper.ElementType.FLOAT,
                args.onDiskRescore()
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
                    format = new ES818BinaryQuantizedVectorsFormat();
                } else {
                    format = new ES818HnswBinaryQuantizedVectorsFormat(args.hnswM(), args.hnswEfConstruction(), 1, null);
                }
            } else if (quantizeBits < 32) {
                if (args.indexType() == IndexType.FLAT) {
                    format = new ES813Int8FlatVectorFormat(null, quantizeBits, true);
                } else {
                    format = new ES814HnswScalarQuantizedVectorsFormat(args.hnswM(), args.hnswEfConstruction(), null, quantizeBits, true);
                }
            } else {
                format = new Lucene99HnswVectorsFormat(args.hnswM(), args.hnswEfConstruction(), 1, null);
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

    /**
     * Main method to run the KNN index tester.
     * It parses command line arguments, creates the index, and runs searches if specified.
     *
     * @param args Command line arguments
     * @throws Exception If an error occurs during index creation or search
     */
    public static void main(String[] args) throws Exception {
        if (args.length != 1 || args[0].equals("--help") || args[0].equals("-h")) {
            // printout an example configuration formatted file and indicate that it is required
            System.out.println("Usage: java -cp <your-classpath> org.elasticsearch.test.knn.KnnIndexTester <config-file>");
            System.out.println("Where <config-file> is a JSON file containing one or more configurations for the KNN index tester.");
            System.out.println("An example configuration object: ");
            System.out.println(
                Strings.toString(
                    new CmdLineArgs.Builder().setDimensions(64)
                        .setDocVectors(List.of("/doc/vectors/path"))
                        .setQueryVectors("/query/vectors/path")
                        .build(),
                    true,
                    true
                )
            );
            return;
        }
        String jsonConfig = args[0];
        // Parse command line arguments
        Path jsonConfigPath = PathUtils.get(jsonConfig);
        if (Files.exists(jsonConfigPath) == false) {
            throw new IllegalArgumentException("JSON config file does not exist: " + jsonConfigPath);
        }
        // Parse the JSON config file to get command line arguments
        // This assumes that CmdLineArgs.fromXContent is implemented to parse the JSON file
        List<CmdLineArgs> cmdLineArgsList = new ArrayList<>();
        try (
            InputStream jsonStream = Files.newInputStream(jsonConfigPath);
            XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, jsonStream)
        ) {
            // check if the parser is at the start of an object if so, we only have one set of arguments
            if (parser.currentToken() == null && parser.nextToken() == XContentParser.Token.START_OBJECT) {
                cmdLineArgsList.add(CmdLineArgs.fromXContent(parser));
            } else if (parser.currentToken() == XContentParser.Token.START_ARRAY) {
                // if the parser is at the start of an array, we have multiple sets of arguments
                while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                    cmdLineArgsList.add(CmdLineArgs.fromXContent(parser));
                }
            } else {
                throw new IllegalArgumentException("Invalid JSON format in config file: " + jsonConfigPath);
            }
        }
        FormattedResults formattedResults = new FormattedResults();

        for (CmdLineArgs cmdLineArgs : cmdLineArgsList) {
            double[] visitPercentages = cmdLineArgs.indexType().equals(IndexType.IVF) && cmdLineArgs.numQueries() > 0
                ? cmdLineArgs.visitPercentages()
                : new double[] { 0 };
            String indexType = cmdLineArgs.indexType().name().toLowerCase(Locale.ROOT);
            Results indexResults = new Results(
                cmdLineArgs.docVectors().get(0).getFileName().toString(),
                indexType,
                cmdLineArgs.numDocs(),
                cmdLineArgs.filterSelectivity()
            );
            Results[] results = new Results[visitPercentages.length];
            for (int i = 0; i < visitPercentages.length; i++) {
                results[i] = new Results(
                    cmdLineArgs.docVectors().get(0).getFileName().toString(),
                    indexType,
                    cmdLineArgs.numDocs(),
                    cmdLineArgs.filterSelectivity()
                );
            }
            logger.info("Running with Java: " + Runtime.version());
            logger.info("Running KNN index tester with arguments: " + cmdLineArgs);
            Codec codec = createCodec(cmdLineArgs);
            Path indexPath = PathUtils.get(formatIndexPath(cmdLineArgs));
            MergePolicy mergePolicy = getMergePolicy(cmdLineArgs);
            if (cmdLineArgs.reindex() || cmdLineArgs.forceMerge()) {
                KnnIndexer knnIndexer = new KnnIndexer(
                    cmdLineArgs.docVectors(),
                    indexPath,
                    codec,
                    cmdLineArgs.indexThreads(),
                    cmdLineArgs.vectorEncoding(),
                    cmdLineArgs.dimensions(),
                    cmdLineArgs.vectorSpace(),
                    cmdLineArgs.numDocs(),
                    mergePolicy,
                    cmdLineArgs.writerBufferSizeInMb(),
                    cmdLineArgs.writerMaxBufferedDocs()
                );
                if (cmdLineArgs.reindex() == false && Files.exists(indexPath) == false) {
                    throw new IllegalArgumentException("Index path does not exist: " + indexPath);
                }
                if (cmdLineArgs.reindex()) {
                    knnIndexer.createIndex(indexResults);
                }
                if (cmdLineArgs.forceMerge()) {
                    knnIndexer.forceMerge(indexResults, cmdLineArgs.forceMergeMaxNumSegments());
                }
            }
            numSegments(indexPath, indexResults);
            if (cmdLineArgs.queryVectors() != null && cmdLineArgs.numQueries() > 0) {
                for (int i = 0; i < results.length; i++) {
                    KnnSearcher knnSearcher = new KnnSearcher(indexPath, cmdLineArgs, visitPercentages[i]);
                    knnSearcher.runSearch(results[i], cmdLineArgs.earlyTermination());
                }
            }
            formattedResults.queryResults.addAll(List.of(results));
            formattedResults.indexResults.add(indexResults);
        }
        logger.info("Results: \n" + formattedResults);
    }

    private static MergePolicy getMergePolicy(CmdLineArgs args) {
        MergePolicy mergePolicy = null;
        if (args.mergePolicy() != null) {
            if (args.mergePolicy() == MergePolicyType.TIERED) {
                mergePolicy = new TieredMergePolicy();
            } else if (args.mergePolicy() == MergePolicyType.LOG_BYTE) {
                mergePolicy = new LogByteSizeMergePolicy();
            } else if (args.mergePolicy() == MergePolicyType.NO) {
                mergePolicy = NoMergePolicy.INSTANCE;
            } else if (args.mergePolicy() == MergePolicyType.LOG_DOC) {
                mergePolicy = new LogDocMergePolicy();
            } else {
                throw new IllegalArgumentException("Invalid merge policy: " + args.mergePolicy());
            }
        }
        return mergePolicy;
    }

    static void numSegments(Path indexPath, KnnIndexTester.Results result) {
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
                "oversampling_factor" };

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
                    String.format(Locale.ROOT, "%.2f", queryResult.overSamplingFactor) };
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
        final float filterSelectivity;
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

        Results(String indexName, String indexType, int numDocs, float filterSelectivity) {
            this.indexName = indexName;
            this.indexType = indexType;
            this.numDocs = numDocs;
            this.filterSelectivity = filterSelectivity;
        }
    }

    static final class ThreadDetails {
        private static final ThreadMXBean threadBean = (ThreadMXBean) java.lang.management.ManagementFactory.getThreadMXBean();
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

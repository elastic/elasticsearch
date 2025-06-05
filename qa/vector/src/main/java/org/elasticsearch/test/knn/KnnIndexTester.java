/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.knn;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene101.Lucene101Codec;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.index.codec.vectors.ES813Int8FlatVectorFormat;
import org.elasticsearch.index.codec.vectors.ES814HnswScalarQuantizedVectorsFormat;
import org.elasticsearch.index.codec.vectors.IVFVectorsFormat;
import org.elasticsearch.index.codec.vectors.es818.ES818BinaryQuantizedVectorsFormat;
import org.elasticsearch.index.codec.vectors.es818.ES818HnswBinaryQuantizedVectorsFormat;
import org.elasticsearch.logging.Level;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * A utility class to create and test KNN indices using Lucene.
 * It supports various index types (HNSW, FLAT, IVF) and configurations.
 */
public class KnnIndexTester {
    static final Level LOG_LEVEL = Level.DEBUG;

    static final SysOutLogger logger = new SysOutLogger();

    static {
        LogConfigurator.loadLog4jPlugins();
        LogConfigurator.configureESLogging(); // native access requires logging to be initialized
    }

    static final String INDEX_DIR = "target/knn_index";

    enum IndexType {
        HNSW,
        FLAT,
        IVF
    }

    private static String formatIndexPath(CmdLineArgs args) {
        List<String> suffix = new ArrayList<>();
        if (args.indexType() == IndexType.FLAT) {
            suffix.add("flat");
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
        return INDEX_DIR + "/" + args.docVectors().getFileName() + "-" + String.join("-", suffix) + ".index";
    }

    static Codec createCodec(CmdLineArgs args) {
        final KnnVectorsFormat format;
        if (args.indexType() == IndexType.IVF) {
            format = new IVFVectorsFormat(args.ivfClusterSize());
        } else {
            if (args.quantizeBits() == 1) {
                if (args.indexType() == IndexType.FLAT) {
                    format = new ES818BinaryQuantizedVectorsFormat();
                } else {
                    format = new ES818HnswBinaryQuantizedVectorsFormat(args.hnswM(), args.hnswEfConstruction(), 1, null);
                }
            } else if (args.quantizeBits() < 32) {
                if (args.indexType() == IndexType.FLAT) {
                    format = new ES813Int8FlatVectorFormat(null, args.quantizeBits(), true);
                } else {
                    format = new ES814HnswScalarQuantizedVectorsFormat(
                        args.hnswM(),
                        args.hnswEfConstruction(),
                        null,
                        args.quantizeBits(),
                        true
                    );
                }
            } else {
                format = new Lucene99HnswVectorsFormat(args.hnswM(), args.hnswEfConstruction(), 1, null);
            }
        }
        return new Lucene101Codec() {
            @Override
            public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
                return format;
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
                        .setDocVectors("/doc/vectors/path")
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
        Path jsonConfigPath = Path.of(jsonConfig);
        if (jsonConfigPath.toFile().exists() == false) {
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
            Results result = new Results(cmdLineArgs.indexType().name().toLowerCase(Locale.ROOT), cmdLineArgs.numDocs());
            System.out.println("Running KNN index tester with arguments: " + cmdLineArgs);
            Codec codec = createCodec(cmdLineArgs);
            Path indexPath = Path.of(formatIndexPath(cmdLineArgs));
            if (cmdLineArgs.reindex() || cmdLineArgs.forceMerge()) {
                KnnIndexer knnIndexer = new KnnIndexer(
                    cmdLineArgs.docVectors(),
                    indexPath,
                    codec,
                    cmdLineArgs.indexThreads(),
                    cmdLineArgs.vectorEncoding(),
                    cmdLineArgs.dimensions(),
                    cmdLineArgs.vectorSpace(),
                    cmdLineArgs.numDocs()
                );
                if (cmdLineArgs.reindex()) {
                    knnIndexer.createIndex(result);
                }
                if (cmdLineArgs.forceMerge()) {
                    knnIndexer.forceMerge(result);
                } else {
                    knnIndexer.numSegments(result);
                }
            }
            if (cmdLineArgs.queryVectors() != null) {
                KnnSearcher knnSearcher = new KnnSearcher(indexPath, cmdLineArgs);
                knnSearcher.runSearch(result);
            }
            formattedResults.results.add(result);
        }
        System.out.println("Results:");
        System.out.println(formattedResults);
    }

    static class FormattedResults {
        List<Results> results = new ArrayList<>();

        @Override
        public String toString() {
            if (results.isEmpty()) {
                return "No results available.";
            }

            // Define column headers
            String[] headers = {
                "index_type",
                "num_docs",
                "index_time(ms)",
                "force_merge_time(ms)",
                "num_segments",
                "latency(ms)",
                "QPS",
                "recall",
                "visited" };

            // Calculate appropriate column widths based on headers and data
            int[] widths = calculateColumnWidths(headers);

            StringBuilder sb = new StringBuilder();

            // Format and append header
            sb.append(formatRow(headers, widths));
            sb.append("\n");

            // Add separator line
            for (int width : widths) {
                sb.append("-".repeat(width)).append("  ");
            }
            sb.append("\n");

            // Format and append each row of data
            for (Results result : results) {
                String[] rowData = {
                    result.indexType,
                    Integer.toString(result.numDocs),
                    Long.toString(result.indexTimeMS),
                    Long.toString(result.forceMergeTimeMS),
                    Integer.toString(result.numSegments),
                    String.format(Locale.ROOT, "%.2f", result.avgLatency),
                    String.format(Locale.ROOT, "%.2f", result.qps),
                    String.format(Locale.ROOT, "%.2f", result.avgRecall),
                    String.format(Locale.ROOT, "%.2f", result.averageVisited) };
                sb.append(formatRow(rowData, widths));
                sb.append("\n");
            }

            return sb.toString();
        }

        // Helper method to format a single row with proper column widths
        private String formatRow(String[] values, int[] widths) {
            StringBuilder row = new StringBuilder();
            for (int i = 0; i < values.length; i++) {
                // Left-align text column (index_type), right-align numeric columns
                String format = (i == 0) ? "%-" + widths[i] + "s" : "%" + widths[i] + "s";
                row.append(String.format(format, values[i]));

                // Add separation between columns
                if (i < values.length - 1) {
                    row.append("  ");
                }
            }
            return row.toString();
        }

        // Calculate appropriate column widths based on headers and data
        private int[] calculateColumnWidths(String[] headers) {
            int[] widths = new int[headers.length];

            // Initialize widths with header lengths
            for (int i = 0; i < headers.length; i++) {
                widths[i] = headers[i].length();
            }

            // Update widths based on data
            for (Results result : results) {
                String[] values = {
                    result.indexType,
                    Integer.toString(result.numDocs),
                    Long.toString(result.indexTimeMS),
                    Long.toString(result.forceMergeTimeMS),
                    Integer.toString(result.numSegments),
                    String.format(Locale.ROOT, "%.2f", result.avgLatency),
                    String.format(Locale.ROOT, "%.2f", result.qps),
                    String.format(Locale.ROOT, "%.2f", result.avgRecall),
                    String.format(Locale.ROOT, "%.2f", result.averageVisited) };

                for (int i = 0; i < values.length; i++) {
                    widths[i] = Math.max(widths[i], values[i].length());
                }
            }

            return widths;
        }
    }

    static class Results {
        final String indexType;
        final int numDocs;
        long indexTimeMS;
        long forceMergeTimeMS;
        int numSegments;
        double avgLatency;
        double qps;
        double avgRecall;
        double averageVisited;

        Results(String indexType, int numDocs) {
            this.indexType = indexType;
            this.numDocs = numDocs;
        }
    }

    static final class SysOutLogger {

        void warn(String message) {
            if (LOG_LEVEL.ordinal() >= Level.WARN.ordinal()) {
                System.out.println(message);
            }
        }

        void warn(String message, Object... params) {
            if (LOG_LEVEL.ordinal() >= Level.WARN.ordinal()) {
                System.out.println(String.format(Locale.ROOT, message, params));
            }
        }

        void info(String message) {
            if (LOG_LEVEL.ordinal() >= Level.INFO.ordinal()) {
                System.out.println(message);
            }
        }

        void info(String message, Object... params) {
            if (LOG_LEVEL.ordinal() >= Level.INFO.ordinal()) {
                System.out.println(String.format(Locale.ROOT, message, params));
            }
        }

        void debug(String message) {
            if (LOG_LEVEL.ordinal() >= Level.DEBUG.ordinal()) {
                System.out.println(message);
            }
        }

        void debug(String message, Object... params) {
            if (LOG_LEVEL.ordinal() >= Level.DEBUG.ordinal()) {
                System.out.println(String.format(Locale.ROOT, message, params));
            }
        }

        void trace(String message) {
            if (LOG_LEVEL == Level.TRACE) {
                System.out.println(message);
            }
        }

        void trace(String message, Object... params) {
            if (LOG_LEVEL == Level.TRACE) {
                System.out.println(String.format(Locale.ROOT, message, params));
            }
        }
    }
}

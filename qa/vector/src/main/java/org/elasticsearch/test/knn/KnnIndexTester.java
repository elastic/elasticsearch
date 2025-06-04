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
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.index.codec.vectors.ES813Int8FlatVectorFormat;
import org.elasticsearch.index.codec.vectors.ES814HnswScalarQuantizedVectorsFormat;
import org.elasticsearch.index.codec.vectors.IVFVectorsFormat;
import org.elasticsearch.index.codec.vectors.es818.ES818BinaryQuantizedVectorsFormat;
import org.elasticsearch.index.codec.vectors.es818.ES818HnswBinaryQuantizedVectorsFormat;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

class KnnIndexTester {
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

    static class CmdLineArgs {
        static CmdLineArgs parse(String[] args) {
            CmdLineArgs cmdLineArgs = new CmdLineArgs();

            for (String arg : args) {
                String[] parts = arg.split("=");
                String key = parts[0].trim();
                String value = null;
                if (parts.length > 1) {
                    value = parts[1].trim();
                }
                if (parts.length > 2) {
                    throw new IllegalArgumentException("Too many parts in argument: " + arg);
                }

                switch (key) {
                    case "--docVectors":
                        cmdLineArgs.docVectors = Path.of(value);
                        break;
                    case "--queryVectors":
                        cmdLineArgs.queryVectors = Path.of(value);
                        break;
                    case "--numDocs":
                        cmdLineArgs.numDocs = Integer.parseInt(value);
                        break;
                    case "--numQueries":
                        cmdLineArgs.numQueries = Integer.parseInt(value);
                        break;
                    case "--indexType":
                        cmdLineArgs.indexType = IndexType.valueOf(value.toUpperCase());
                        break;
                    case "--numCandidates":
                        cmdLineArgs.numCandidates = Integer.parseInt(value);
                        break;
                    case "--k":
                        cmdLineArgs.k = Integer.parseInt(value);
                        break;
                    case "--nProbe":
                        cmdLineArgs.nProbe = Integer.parseInt(value);
                        break;
                    case "--ivfClusterSize":
                        cmdLineArgs.ivfClusterSize = Integer.parseInt(value);
                        break;
                    case "--overSamplingFactor":
                        cmdLineArgs.overSamplingFactor = Integer.parseInt(value);
                        break;
                    case "--hnswM":
                        cmdLineArgs.hnswM = Integer.parseInt(value);
                        break;
                    case "--hnswEfConstruction":
                        cmdLineArgs.hnswEfConstruction = Integer.parseInt(value);
                        break;
                    case "--searchThreads":
                        cmdLineArgs.searchThreads = Integer.parseInt(value);
                        break;
                    case "--indexThreads":
                        cmdLineArgs.indexThreads = Integer.parseInt(value);
                        break;
                    case "--reindex":
                        cmdLineArgs.reindex = true;
                        break;
                    case "--forceMerge":
                        cmdLineArgs.forceMerge = true;
                        break;
                    case "--vectorSpace":
                        cmdLineArgs.vectorSpace = VectorSimilarityFunction.valueOf(value.toUpperCase());
                        break;
                    case "--quantizeBits":
                        cmdLineArgs.quantizeBits = Integer.parseInt(value);
                        break;
                    case "--vectorEncoding":
                        cmdLineArgs.vectorEncoding = VectorEncoding.valueOf(value.toUpperCase());
                        break;
                    case "--dimensions":
                        cmdLineArgs.dimensions = Integer.parseInt(value);
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown argument: " + key);
                }
            }
            return cmdLineArgs;
        }

        int numDocs = 1000;
        int numQueries = 10;
        IndexType indexType = IndexType.IVF;
        int numCandidates = 100;
        int k = 100;
        int nProbe = -1;
        int ivfClusterSize = 384;
        int overSamplingFactor = 0;
        int hnswM = 16;
        int hnswEfConstruction = 100;
        int searchThreads = 1;
        int indexThreads = 1;
        boolean reindex = false;
        boolean forceMerge = false;
        VectorSimilarityFunction vectorSpace = VectorSimilarityFunction.EUCLIDEAN;
        // 32 means no quantization
        int quantizeBits = 32;
        int dimensions = 1024; // Default dimension size for vectors
        VectorEncoding vectorEncoding = VectorEncoding.FLOAT32;
        Path docVectors = null;
        Path queryVectors = null;
    }

    private static String formatIndexPath(CmdLineArgs args) {
        List<String> suffix = new ArrayList<>();
        if (args.indexType == IndexType.FLAT) {
            suffix.add("flat");
        } else if (args.indexType == IndexType.IVF) {
            suffix.add("ivf");
            suffix.add(Integer.toString(args.ivfClusterSize));
        } else {
            suffix.add(Integer.toString(args.hnswM));
            suffix.add(Integer.toString(args.hnswEfConstruction));
            if (args.quantizeBits < 32) {
                suffix.add(Integer.toString(args.quantizeBits));
            }
        }
        return INDEX_DIR + "/" + args.docVectors.getFileName() + "-" + String.join("-", suffix) + ".index";
    }

    static Codec createCodec(CmdLineArgs args) {
        final KnnVectorsFormat format;
        if (args.indexType == IndexType.IVF) {
            format = new IVFVectorsFormat(args.ivfClusterSize);
        } else {
            if (args.quantizeBits == 1) {
                if (args.indexType == IndexType.FLAT) {
                    format = new ES818BinaryQuantizedVectorsFormat();
                } else {
                    format = new ES818HnswBinaryQuantizedVectorsFormat(args.hnswM, args.hnswEfConstruction, 1, null);
                }
            } else if (args.quantizeBits < 32) {
                if (args.indexType == IndexType.FLAT) {
                    format = new ES813Int8FlatVectorFormat(null, args.quantizeBits, true);
                } else {
                    format = new ES814HnswScalarQuantizedVectorsFormat(args.hnswM, args.hnswEfConstruction, null, args.quantizeBits, true);
                }
            } else {
                format = new Lucene99HnswVectorsFormat(args.hnswM, args.hnswEfConstruction, 1, null);
            }
        }
        return new Lucene101Codec() {
            @Override
            public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
                return format;
            }
        };
    }

    public static void main(String[] args) throws Exception {
        CmdLineArgs cmdLineArgs = CmdLineArgs.parse(args);
        if (cmdLineArgs.docVectors == null || cmdLineArgs.docVectors.toFile().exists() == false) {
            throw new IllegalArgumentException("Document vectors file does not exist: " + cmdLineArgs.docVectors);
        }
        Codec codec = createCodec(cmdLineArgs);
        Path indexPath = Path.of(formatIndexPath(cmdLineArgs));
        long indexCreationTimeMS = 0;
        long forceMergeTimeMS = 0;
        int numSegments = 1;
        StringBuilder resultHeaders = new StringBuilder();
        StringBuilder resultValues = new StringBuilder();
        // indicate params used for index creation
        resultHeaders.append("index_type\t");
        resultValues.append(cmdLineArgs.indexType).append("\t");
        resultHeaders.append("num_docs\t");
        resultValues.append(cmdLineArgs.numDocs).append("\t");
        if (cmdLineArgs.reindex || cmdLineArgs.forceMerge) {
            KnnIndexer knnIndexer = new KnnIndexer(
                cmdLineArgs.docVectors,
                indexPath,
                codec,
                cmdLineArgs.indexThreads,
                cmdLineArgs.vectorEncoding,
                cmdLineArgs.dimensions,
                cmdLineArgs.vectorSpace,
                cmdLineArgs.numDocs
            );
            if (cmdLineArgs.reindex) {
                indexCreationTimeMS = knnIndexer.createIndex();
            }
            if (cmdLineArgs.forceMerge) {
                forceMergeTimeMS = knnIndexer.forceMerge();
            } else {
                numSegments = knnIndexer.numSegments();
            }
        }
        if (indexCreationTimeMS > 0) {
            resultHeaders.append("index_time(ms)").append("\t");
            resultValues.append(indexCreationTimeMS).append("\t");
        }
        if (forceMergeTimeMS > 0) {
            resultHeaders.append("force_merge_time(ms)").append("\t");
            resultValues.append(forceMergeTimeMS).append("\t");
        }
        resultHeaders.append("num_segments\t");
        resultValues.append(numSegments).append("\t");

        if (cmdLineArgs.queryVectors != null) {
            KnnSearcher knnSearcher = new KnnSearcher(indexPath, cmdLineArgs);
            KnnSearcher.SearcherResults results = knnSearcher.runSearch();
            resultHeaders.append("latency(ms)\t");
            resultValues.append(results.avgLatency()).append("\t");
            resultHeaders.append("qps\t");
            resultValues.append(results.qps()).append("\t");
            resultHeaders.append("recall\t");
            resultValues.append(results.avgRecall()).append("\t");
            resultHeaders.append("visited\t");
            resultValues.append(results.averageVisited()).append("\t");
        }
        System.out.println(resultHeaders);
        System.out.println(resultValues);
    }
}

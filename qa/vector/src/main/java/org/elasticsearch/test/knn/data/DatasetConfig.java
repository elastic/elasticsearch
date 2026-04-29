/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.knn.data;

import org.elasticsearch.test.knn.TestConfiguration;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

/**
 * Represents the "dataset" configuration using NamedXContent style parsing:
 *
 * <pre>
 *   "dataset": { "gcp": { "name": "somegcpbucketdataset" } }                                — GCP
 *   "dataset": { "file": { "doc_vectors": [...], "query_vectors": "..." } }                 — local files
 *   "dataset": { "partition_generated": { "num_partitions": 50, ... } }                      — synthetic data
 * </pre>
 *
 * Each variant provides a {@link DataGenerator} via {@link #createDataGenerator} that
 * encapsulates all data provisioning for indexing and searching.
 */
public sealed interface DatasetConfig extends ToXContentFragment permits DatasetConfig.GcpDataset, DatasetConfig.FileDataset,
    DatasetConfig.PartitionGenerated {

    /** Distribution strategy for assigning documents to partitions. */
    enum PartitionDistribution {
        UNIFORM,
        ZIPF;

        static PartitionDistribution fromString(String value) {
            return valueOf(value.toUpperCase(Locale.ROOT));
        }
    }

    /**
     * Creates a {@link DataGenerator} that supplies vectors for indexing and queries for searching.
     */
    DataGenerator createDataGenerator(TestConfiguration config) throws IOException;

    /** A dataset that is downloaded from a Google Cloud Storage bucket. */
    record GcpDataset(String name) implements DatasetConfig {

        static final ParseField DATASET_NAME = new ParseField("name");

        private static final ObjectParser<Builder, Void> PARSER = new ObjectParser<>("gcp_dataset", false, Builder::new);

        static {
            PARSER.declareString(Builder::setDatasetName, DATASET_NAME);
        }

        static GcpDataset fromXContent(XContentParser parser) {
            Builder builder = PARSER.apply(parser, null);
            if (builder.datasetName == null) {
                throw new IllegalArgumentException("gcp dataset config requires a 'name' field");
            }
            return new GcpDataset(builder.datasetName);
        }

        @Override
        public DataGenerator createDataGenerator(TestConfiguration config) {
            return new FileDataGenerator(config);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject("dataset");
            builder.startObject("gcp");
            builder.field("name", name);
            builder.endObject();
            return builder.endObject();
        }

        private static class Builder {
            private String datasetName;

            void setDatasetName(String datasetName) {
                this.datasetName = datasetName;
            }
        }
    }

    /** A dataset specified via local file paths for doc vectors and (optionally) query vectors. */
    record FileDataset(List<String> docVectors, String queryVectors) implements DatasetConfig {

        static final ParseField DOC_VECTORS = new ParseField("doc_vectors");
        static final ParseField QUERY_VECTORS = new ParseField("query_vectors");

        private static final ObjectParser<Builder, Void> PARSER = new ObjectParser<>("file_dataset", false, Builder::new);

        static {
            PARSER.declareStringArray(Builder::setDocVectors, DOC_VECTORS);
            PARSER.declareString(Builder::setQueryVectors, QUERY_VECTORS);
        }

        static FileDataset fromXContent(XContentParser parser) {
            Builder builder = PARSER.apply(parser, null);
            if (builder.docVectors == null || builder.docVectors.isEmpty()) {
                throw new IllegalArgumentException("file dataset config requires 'doc_vectors'");
            }
            return new FileDataset(builder.docVectors, builder.queryVectors);
        }

        @Override
        public DataGenerator createDataGenerator(TestConfiguration config) {
            return new FileDataGenerator(config);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject("dataset");
            builder.startObject("file");
            builder.field("doc_vectors", docVectors);
            if (queryVectors != null) {
                builder.field("query_vectors", queryVectors);
            }
            builder.endObject();
            return builder.endObject();
        }

        private static class Builder {
            private List<String> docVectors;
            private String queryVectors;

            void setDocVectors(List<String> docVectors) {
                this.docVectors = docVectors;
            }

            void setQueryVectors(String queryVectors) {
                this.queryVectors = queryVectors;
            }
        }
    }

    /** A synthetically generated partitioned dataset. */
    record PartitionGenerated(int numPartitions, PartitionDistribution partitionDistribution, long generatorSeed) implements DatasetConfig {

        static final ParseField NUM_PARTITIONS_FIELD = new ParseField("num_partitions");
        static final ParseField PARTITION_DISTRIBUTION_FIELD = new ParseField("partition_distribution");
        static final ParseField GENERATOR_SEED_FIELD = new ParseField("generator_seed");

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject("dataset");
            builder.startObject("partition_generated");
            builder.field("num_partitions", numPartitions);
            builder.field("partition_distribution", partitionDistribution.name().toLowerCase(Locale.ROOT));
            builder.field("generator_seed", generatorSeed);
            builder.endObject();
            return builder.endObject();
        }

        private static final ObjectParser<Builder, Void> PARSER = new ObjectParser<>("partition_generated", false, Builder::new);

        static {
            PARSER.declareInt(Builder::setNumPartitions, NUM_PARTITIONS_FIELD);
            PARSER.declareString(Builder::setPartitionDistribution, PARTITION_DISTRIBUTION_FIELD);
            PARSER.declareLong(Builder::setGeneratorSeed, GENERATOR_SEED_FIELD);
        }

        static PartitionGenerated fromXContent(XContentParser parser) {
            Builder builder = PARSER.apply(parser, null);
            return new PartitionGenerated(builder.numPartitions, builder.partitionDistribution, builder.generatorSeed);
        }

        @Override
        public DataGenerator createDataGenerator(TestConfiguration config) {
            return new PartitionDataGenerator(config.numDocs(), config.dimensions(), numPartitions, partitionDistribution, generatorSeed);
        }

        private static class Builder {
            private int numPartitions = 100;
            private PartitionDistribution partitionDistribution = PartitionDistribution.UNIFORM;
            private long generatorSeed = 42L;

            void setNumPartitions(int numPartitions) {
                this.numPartitions = numPartitions;
            }

            void setPartitionDistribution(String partitionDistribution) {
                this.partitionDistribution = PartitionDistribution.fromString(partitionDistribution);
            }

            void setGeneratorSeed(long generatorSeed) {
                this.generatorSeed = generatorSeed;
            }
        }
    }

    /**
     * Parses the "dataset" field. Accepts either a string shorthand for GCP datasets
     * or a NamedXContent-style object where the key identifies the dataset type.
     */
    static DatasetConfig parse(XContentParser parser) throws IOException {
        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            throw new IllegalArgumentException("Expected an object for dataset, got: " + parser.currentToken());
        }
        parser.nextToken();
        String typeName = parser.currentName();
        parser.nextToken();
        DatasetConfig result = switch (typeName) {
            case "gcp" -> GcpDataset.fromXContent(parser);
            case "file" -> FileDataset.fromXContent(parser);
            case "partition_generated" -> PartitionGenerated.fromXContent(parser);
            default -> throw new IllegalArgumentException(
                "Unknown dataset type: [" + typeName + "]. Supported: gcp, file, partition_generated"
            );
        };
        // consume the outer END_OBJECT
        parser.nextToken();
        return result;
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.knn;

import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Locale;

/**
 * Represents the "dataset" configuration using NamedXContent style parsing:
 *
 * <pre>
 *   "dataset": "somegcpbucketdataset"                                  — shorthand for GCP
 *   "dataset": { "gcp": { "name": "somegcpbucketdataset" } }           — explicit GCP
 *   "dataset": { "partition_generated": { "num_partitions": 50, ... } } — synthetic data
 * </pre>
 *
 * Each variant provides a {@link DataGenerator} via {@link #createDataGenerator} that
 * encapsulates all data provisioning for indexing and searching.
 */
sealed interface DatasetConfig permits DatasetConfig.GcpDataset, DatasetConfig.PartitionGenerated {

    /**
     * Creates a {@link DataGenerator} that supplies vectors for indexing and queries for searching.
     */
    DataGenerator createDataGenerator(TestConfiguration config) throws IOException;

    /** A dataset that is downloaded from a Google Cloud Storage bucket. */
    record GcpDataset(String name) implements DatasetConfig {

        @Override
        public DataGenerator createDataGenerator(TestConfiguration config) throws IOException {
            return new FileDataGenerator(config);
        }
    }

    /** A synthetically generated partitioned dataset. */
    record PartitionGenerated(int numPartitions, String partitionDistribution, long generatorSeed) implements DatasetConfig {

        static final ParseField NUM_PARTITIONS_FIELD = new ParseField("num_partitions");
        static final ParseField PARTITION_DISTRIBUTION_FIELD = new ParseField("partition_distribution");
        static final ParseField GENERATOR_SEED_FIELD = new ParseField("generator_seed");

        private static final ObjectParser<Builder, Void> PARSER = new ObjectParser<>("partition_generated", false, Builder::new);

        static {
            PARSER.declareInt(Builder::setNumPartitions, NUM_PARTITIONS_FIELD);
            PARSER.declareString(Builder::setPartitionDistribution, PARTITION_DISTRIBUTION_FIELD);
            PARSER.declareLong(Builder::setGeneratorSeed, GENERATOR_SEED_FIELD);
        }

        static PartitionGenerated fromXContent(XContentParser parser) throws IOException {
            Builder builder = PARSER.apply(parser, null);
            return new PartitionGenerated(builder.numPartitions, builder.partitionDistribution, builder.generatorSeed);
        }

        @Override
        public DataGenerator createDataGenerator(TestConfiguration config) {
            return new PartitionDataGenerator(config.numDocs(), config.dimensions(), numPartitions, partitionDistribution, generatorSeed);
        }

        private static class Builder {
            private int numPartitions = 100;
            private String partitionDistribution = "uniform";
            private long generatorSeed = 42L;

            void setNumPartitions(int numPartitions) {
                this.numPartitions = numPartitions;
            }

            void setPartitionDistribution(String partitionDistribution) {
                this.partitionDistribution = partitionDistribution.toLowerCase(Locale.ROOT);
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
        if (parser.currentToken() == XContentParser.Token.VALUE_STRING) {
            return new GcpDataset(parser.text());
        }
        // NamedXContent style: { "gcp": {...} } or { "partition_generated": {...} }
        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            throw new IllegalArgumentException("Expected a string or object for dataset, got: " + parser.currentToken());
        }
        parser.nextToken();
        String typeName = parser.currentName();
        parser.nextToken();
        DatasetConfig result = switch (typeName) {
            case "gcp" -> parseGcp(parser);
            case "partition_generated" -> PartitionGenerated.fromXContent(parser);
            default -> throw new IllegalArgumentException("Unknown dataset type: [" + typeName + "]. Supported: gcp, partition_generated");
        };
        // consume the outer END_OBJECT
        parser.nextToken();
        return result;
    }

    private static GcpDataset parseGcp(XContentParser parser) throws IOException {
        String name = null;
        if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                if ("name".equals(parser.currentName())) {
                    parser.nextToken();
                    name = parser.text();
                } else {
                    throw new IllegalArgumentException("Unknown field in gcp dataset config: " + parser.currentName());
                }
            }
        }
        if (name == null) {
            throw new IllegalArgumentException("gcp dataset config requires a 'name' field");
        }
        return new GcpDataset(name);
    }
}

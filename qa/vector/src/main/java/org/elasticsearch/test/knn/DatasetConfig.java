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
 * Represents the "dataset" configuration, which can be either a GCP bucket name (string)
 * or a structured object describing synthetic data generation.
 *
 * <p>Usage in config JSON:
 * <pre>
 *   "dataset": "somegcpbucketdataset"          — downloads from GCP
 *   "dataset": { "type": "partition_generated", "num_partitions": 50, ... }  — generates locally
 * </pre>
 */
sealed interface DatasetConfig permits DatasetConfig.GcpDataset, DatasetConfig.PartitionGenerated {

    /** A dataset that is downloaded from a Google Cloud Storage bucket. */
    record GcpDataset(String name) implements DatasetConfig {}

    /** A synthetically generated partitioned dataset. */
    record PartitionGenerated(int numPartitions, String partitionDistribution, long generatorSeed) implements DatasetConfig {

        static final ParseField TYPE_FIELD = new ParseField("type");
        static final ParseField NUM_PARTITIONS_FIELD = new ParseField("num_partitions");
        static final ParseField PARTITION_DISTRIBUTION_FIELD = new ParseField("partition_distribution");
        static final ParseField GENERATOR_SEED_FIELD = new ParseField("generator_seed");

        private static final ObjectParser<Builder, Void> PARSER = new ObjectParser<>("partition_generated", false, Builder::new);

        static {
            PARSER.declareString(Builder::setType, TYPE_FIELD);
            PARSER.declareInt(Builder::setNumPartitions, NUM_PARTITIONS_FIELD);
            PARSER.declareString(Builder::setPartitionDistribution, PARTITION_DISTRIBUTION_FIELD);
            PARSER.declareLong(Builder::setGeneratorSeed, GENERATOR_SEED_FIELD);
        }

        static PartitionGenerated fromXContent(XContentParser parser) throws IOException {
            Builder builder = PARSER.apply(parser, null);
            if ("partition_generated".equals(builder.type) == false) {
                throw new IllegalArgumentException("Unknown dataset type: [" + builder.type + "]. Supported: partition_generated");
            }
            return new PartitionGenerated(builder.numPartitions, builder.partitionDistribution, builder.generatorSeed);
        }

        private static class Builder {
            private String type;
            private int numPartitions = 100;
            private String partitionDistribution = "uniform";
            private long generatorSeed = 42L;

            void setType(String type) {
                this.type = type;
            }

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
     * Parses the "dataset" field which can be either a string (GCP bucket name)
     * or an object (structured dataset config).
     */
    static DatasetConfig parse(XContentParser parser) throws IOException {
        if (parser.currentToken() == XContentParser.Token.VALUE_STRING) {
            return new GcpDataset(parser.text());
        } else {
            return PartitionGenerated.fromXContent(parser);
        }
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.knn;

import org.elasticsearch.common.Strings;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.knn.data.DatasetConfig;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

public class TestConfigurationTests extends ESTestCase {

    public void testParameterParsing() throws Exception {
        String json = """
            {
              "doc_vectors": ["/path/to/docs"],
              "query_vectors": "/path/to/queries",
              "dimensions": 128,
              "num_candidates": [10, 20],
              "k": [5, 10],
              "visit_percentage": [0.5],
              "over_sampling_factor": [2.0],
              "search_threads": [1],
              "num_searchers": [1],
              "filter_selectivity": [0.8],
              "filter_cache": [true],
              "early_termination": [false],
              "seed": [123]
            }
            """;

        try (XContentParser parser = createParser(XContentType.JSON.xContent(), json)) {
            TestConfiguration config = TestConfiguration.fromXContent(parser);
            assertEquals(128, config.dimensions());
            assertEquals(1, config.docVectors().size());
            assertTrue(config.docVectors().get(0).equals(PathUtils.get("/path/to/docs")));
            assertTrue(config.queryVectors().equals(PathUtils.get("/path/to/queries")));

            List<SearchParameters> params = config.searchParams();
            assertEquals(4, params.size());

            // Verify combinations
            // Order: numCandidates, k, ...
            // result = [[10, 5], [10, 10], [20, 5], [20, 10]]

            assertEquals(10, params.get(0).numCandidates());
            assertEquals(5, params.get(0).topK());

            assertEquals(10, params.get(1).numCandidates());
            assertEquals(10, params.get(1).topK());

            assertEquals(20, params.get(2).numCandidates());
            assertEquals(5, params.get(2).topK());

            assertEquals(20, params.get(3).numCandidates());
            assertEquals(10, params.get(3).topK());
        }
    }

    public void testHelp() throws Exception {
        KnnIndexTester.main(new String[] { "--help" });
    }

    public void testDatasetConfigParsePartitionGenerated() throws Exception {
        String json = """
            {
              "dataset": {
                "random_generated": {
                  "num_partitions": 50,
                  "partition_distribution": "zipf",
                  "generator_seed": 99
                }
              },
              "dimensions": 64,
              "num_docs": 1000,
              "num_queries": 10
            }
            """;

        try (XContentParser parser = createParser(XContentType.JSON.xContent(), json)) {
            TestConfiguration config = TestConfiguration.fromXContent(parser);
            assertThat(config.datasetConfig(), instanceOf(DatasetConfig.RandomGenerated.class));
            DatasetConfig.RandomGenerated pg = (DatasetConfig.RandomGenerated) config.datasetConfig();
            assertEquals(50, pg.numPartitions());
            assertEquals(DatasetConfig.PartitionDistribution.ZIPF, pg.partitionDistribution());
            assertEquals(99L, pg.generatorSeed());
            assertEquals(64, config.dimensions());
            assertEquals(1000, config.numDocs());
        }
    }

    public void testDatasetConfigParseExplicitGcp() throws Exception {
        String json = """
            {
              "dataset": {
                "gcp": {
                  "name": "explicit-dataset"
                }
              },
              "doc_vectors": ["/path/to/docs"],
              "dimensions": 128
            }
            """;

        // Use PARSER.apply to test only the parsing, not build() which requires GCS
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), json)) {
            TestConfiguration.Builder builder = TestConfiguration.PARSER.apply(parser, null);
            assertThat(builder.datasetConfig(), instanceOf(DatasetConfig.GcpDataset.class));
            assertEquals("explicit-dataset", ((DatasetConfig.GcpDataset) builder.datasetConfig()).name());
        }
    }

    public void testDatasetConfigPartitionGeneratedDefaults() throws Exception {
        String json = """
            {
              "dataset": {
                "random_generated": {}
              },
              "dimensions": 32,
              "num_docs": 500,
              "num_queries": 5
            }
            """;

        try (XContentParser parser = createParser(XContentType.JSON.xContent(), json)) {
            TestConfiguration config = TestConfiguration.fromXContent(parser);
            assertThat(config.datasetConfig(), instanceOf(DatasetConfig.RandomGenerated.class));
            DatasetConfig.RandomGenerated pg = (DatasetConfig.RandomGenerated) config.datasetConfig();
            assertEquals(100, pg.numPartitions());
            assertEquals(DatasetConfig.PartitionDistribution.UNIFORM, pg.partitionDistribution());
            assertEquals(42L, pg.generatorSeed());
        }
    }

    public void testDatasetConfigRoundtripPartitionGenerated() throws Exception {
        // Parse a partition_generated config
        String json = """
            {
              "dataset": {
                "random_generated": {
                  "num_partitions": 25,
                  "partition_distribution": "uniform",
                  "generator_seed": 777
                }
              },
              "dimensions": 64,
              "num_docs": 2000,
              "num_queries": 20
            }
            """;

        TestConfiguration.Builder builder;
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), json)) {
            builder = TestConfiguration.PARSER.apply(parser, null);
        }

        // Serialize via toXContent
        String serialized = Strings.toString(builder, false, false);

        // Re-parse the serialized output and verify the dataset config survived the roundtrip
        try (XContentParser parser2 = createParser(XContentType.JSON.xContent(), serialized)) {
            TestConfiguration config2 = TestConfiguration.fromXContent(parser2);
            assertThat(config2.datasetConfig(), instanceOf(DatasetConfig.RandomGenerated.class));
            DatasetConfig.RandomGenerated pg = (DatasetConfig.RandomGenerated) config2.datasetConfig();
            assertEquals(25, pg.numPartitions());
            assertEquals(DatasetConfig.PartitionDistribution.UNIFORM, pg.partitionDistribution());
            assertEquals(777L, pg.generatorSeed());
        }
    }

    public void testDatasetConfigUnknownTypeThrows() throws Exception {
        String json = """
            {
              "dataset": {
                "unknown_type": {
                  "foo": "bar"
                }
              },
              "doc_vectors": ["/path/to/docs"],
              "dimensions": 128
            }
            """;

        try (XContentParser parser = createParser(XContentType.JSON.xContent(), json)) {
            expectThrows(IllegalArgumentException.class, () -> TestConfiguration.fromXContent(parser));
        }
    }

    public void testDatasetConfigParseFileDataset() throws Exception {
        String json = """
            {
              "dataset": {
                "file": {
                  "doc_vectors": ["/data/docs1.fvec", "/data/docs2.fvec"],
                  "query_vectors": "/data/queries.fvec"
                }
              },
              "dimensions": 128,
              "num_docs": 5000,
              "num_queries": 50
            }
            """;

        try (XContentParser parser = createParser(XContentType.JSON.xContent(), json)) {
            TestConfiguration config = TestConfiguration.fromXContent(parser);
            assertThat(config.datasetConfig(), instanceOf(DatasetConfig.FileDataset.class));
            DatasetConfig.FileDataset fd = (DatasetConfig.FileDataset) config.datasetConfig();
            assertThat(fd.docVectors(), contains("/data/docs1.fvec", "/data/docs2.fvec"));
            assertEquals("/data/queries.fvec", fd.queryVectors());
            assertThat(config.docVectors(), hasSize(2));
            assertEquals(PathUtils.get("/data/docs1.fvec"), config.docVectors().get(0));
            assertEquals(PathUtils.get("/data/queries.fvec"), config.queryVectors());
        }
    }

    public void testDatasetConfigParseFileDatasetNoQueries() throws Exception {
        String json = """
            {
              "dataset": {
                "file": {
                  "doc_vectors": ["/data/docs.fvec"]
                }
              },
              "dimensions": 64,
              "num_docs": 100,
              "num_queries": 10
            }
            """;

        try (XContentParser parser = createParser(XContentType.JSON.xContent(), json)) {
            TestConfiguration config = TestConfiguration.fromXContent(parser);
            assertThat(config.datasetConfig(), instanceOf(DatasetConfig.FileDataset.class));
            DatasetConfig.FileDataset fd = (DatasetConfig.FileDataset) config.datasetConfig();
            assertThat(fd.docVectors(), contains("/data/docs.fvec"));
            assertNull(fd.queryVectors());
            assertNull(config.queryVectors());
        }
    }

    public void testDatasetConfigFileDatasetMissingDocVectorsThrows() throws Exception {
        String json = """
            {
              "dataset": {
                "file": {
                  "query_vectors": "/data/queries.fvec"
                }
              },
              "dimensions": 128
            }
            """;

        try (XContentParser parser = createParser(XContentType.JSON.xContent(), json)) {
            expectThrows(Exception.class, () -> TestConfiguration.fromXContent(parser));
        }
    }

    public void testDatasetConfigRoundtripFileDataset() throws Exception {
        String json = """
            {
              "dataset": {
                "file": {
                  "doc_vectors": ["/data/docs.fvec"],
                  "query_vectors": "/data/queries.fvec"
                }
              },
              "dimensions": 64,
              "num_docs": 500,
              "num_queries": 10
            }
            """;

        TestConfiguration.Builder builder;
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), json)) {
            builder = TestConfiguration.PARSER.apply(parser, null);
        }

        String serialized = Strings.toString(builder, false, false);

        try (XContentParser parser2 = createParser(XContentType.JSON.xContent(), serialized)) {
            TestConfiguration.Builder builder2 = TestConfiguration.PARSER.apply(parser2, null);
            assertThat(builder2.datasetConfig(), instanceOf(DatasetConfig.FileDataset.class));
            DatasetConfig.FileDataset fd = (DatasetConfig.FileDataset) builder2.datasetConfig();
            assertThat(fd.docVectors(), contains("/data/docs.fvec"));
            assertEquals("/data/queries.fvec", fd.queryVectors());
        }
    }

    public void testDatasetConfigInvalidDistributionThrows() throws Exception {
        String json = """
            {
              "dataset": {
                "partition_generated": {
                  "num_partitions": 10,
                  "partition_distribution": "invalid_distribution",
                  "generator_seed": 1
                }
              },
              "dimensions": 32,
              "num_docs": 100,
              "num_queries": 5
            }
            """;

        try (XContentParser parser = createParser(XContentType.JSON.xContent(), json)) {
            // Distribution is validated at parse time via the enum
            expectThrows(IllegalArgumentException.class, () -> TestConfiguration.fromXContent(parser));
        }
    }

}

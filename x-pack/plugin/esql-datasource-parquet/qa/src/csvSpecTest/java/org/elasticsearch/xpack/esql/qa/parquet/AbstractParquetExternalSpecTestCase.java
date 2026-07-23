/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.parquet;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.esql.CsvSpecReader.CsvTestCase;
import org.elasticsearch.xpack.esql.datasources.FormatNameResolver;
import org.elasticsearch.xpack.esql.qa.rest.AbstractExternalSourceSpecTestCase;
import org.junit.ClassRule;
import org.junit.rules.TestRule;

/**
 * Shared cluster base for Parquet csv-spec tests.
 * <p>
 * Declaring the cluster here — at the abstract-class level — means all concrete subclasses
 * ({@link ParquetFormatSpecIT}, {@link ParquetCompressedFormatSpecIT},
 * {@link ParquetCompressedMultifileSpecIT}) inherit the same static {@code cluster} field and
 * therefore share a single {@code ElasticsearchCluster} instance across the entire JVM run.
 * This mirrors the pattern used in {@code AbstractEsqlSpecIT} in the single-node module and
 * lets the JVM-level {@code INGEST} guard in {@code EsqlSpecTestCase} fire exactly once:
 * data is loaded on the first suite and the cluster remains live for subsequent suites without
 * re-loading — which would fail because {@code wipeTestData()} no longer resets the guard.
 */
abstract class AbstractParquetExternalSpecTestCase extends AbstractExternalSourceSpecTestCase {

    public static ElasticsearchCluster cluster = Clusters.testCluster(() -> s3Fixture.getAddress());

    @ClassRule
    public static TestRule ruleChain = chainFixturesBeforeCluster(cluster);

    protected AbstractParquetExternalSpecTestCase(
        String fileName,
        String groupName,
        String testName,
        Integer lineNumber,
        CsvTestCase testCase,
        String instructions,
        StorageBackend storageBackend
    ) {
        super(fileName, groupName, testName, lineNumber, testCase, instructions, storageBackend, "parquet");
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected boolean enableRoundingDoubleValuesOnAsserting() {
        return true;
    }

    @Override
    protected String readerName() {
        return FormatNameResolver.READER_JAVA;
    }
}

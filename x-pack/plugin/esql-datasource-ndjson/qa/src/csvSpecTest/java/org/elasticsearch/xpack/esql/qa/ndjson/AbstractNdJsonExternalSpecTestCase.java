/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.ndjson;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.esql.CsvSpecReader.CsvTestCase;
import org.elasticsearch.xpack.esql.qa.rest.AbstractExternalSourceSpecTestCase;
import org.junit.ClassRule;

/**
 * Shared cluster base for NDJSON csv-spec tests.
 * <p>
 * Declaring the cluster here — at the abstract-class level — means all concrete subclasses
 * ({@link NdJsonFormatSpecIT}, {@link NdJsonCompressedFormatSpecIT}) inherit the same static
 * {@code cluster} field and therefore share a single {@code ElasticsearchCluster} instance
 * across the entire JVM run. This mirrors the pattern used in
 * {@code AbstractParquetExternalSpecTestCase} and lets the JVM-level {@code INGEST} guard in
 * {@code EsqlSpecTestCase} fire exactly once: data is loaded on the first suite and the cluster
 * remains live for subsequent suites without re-loading.
 */
abstract class AbstractNdJsonExternalSpecTestCase extends AbstractExternalSourceSpecTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster(() -> s3Fixture.getAddress());

    protected AbstractNdJsonExternalSpecTestCase(
        String fileName,
        String groupName,
        String testName,
        Integer lineNumber,
        CsvTestCase testCase,
        String instructions,
        StorageBackend storageBackend,
        String readerName
    ) {
        super(fileName, groupName, testName, lineNumber, testCase, instructions, storageBackend, readerName);
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }
}

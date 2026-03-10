/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.iceberg;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.esql.CsvSpecReader.CsvTestCase;
import org.elasticsearch.xpack.esql.SpecReader;
import org.junit.ClassRule;

import java.net.URL;
import java.util.List;

import static org.elasticsearch.xpack.esql.CsvSpecReader.specParser;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.classpathResources;
import static org.junit.Assert.assertTrue;

/** Integration tests for Iceberg tables with metadata (loads iceberg-*.csv-spec). */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
@AwaitsFix(bugUrl = "Iceberg integration tests disabled pending stabilization")
public class IcebergSpecIT extends IcebergSpecTestCase {

    /** Elasticsearch cluster with S3 fixture and Iceberg catalog for testing. */
    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster(() -> s3Fixture.getAddress());

    public IcebergSpecIT(
        String fileName,
        String groupName,
        String testName,
        Integer lineNumber,
        CsvTestCase testCase,
        String instructions
    ) {
        super(fileName, groupName, testName, lineNumber, testCase, instructions);
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @ParametersFactory(argumentFormatting = "csv-spec:%2$s.%3$s")
    public static List<Object[]> readScriptSpec() throws Exception {
        List<URL> urls = classpathResources("/iceberg-*.csv-spec");
        assertTrue("No iceberg-*.csv-spec files found", urls.size() > 0);
        return SpecReader.readScriptSpec(urls, specParser());
    }
}

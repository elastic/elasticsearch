/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.parquet;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.esql.datasources.AbstractFromDatasetSubqueryRestTestCase;
import org.elasticsearch.xpack.esql.datasources.BackendFixture;
import org.elasticsearch.xpack.esql.datasources.S3BackendFixture;
import org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.DataSourcesS3HttpFixture;
import org.elasticsearch.xpack.esql.qa.parquet.EmployeesParquetGenerator.EmployeeRow;
import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.test.rest.ESRestTestCase.basicAuthHeaderValue;
import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.WAREHOUSE;
import static org.hamcrest.Matchers.equalTo;

/**
 * An authenticated query over a multi-file external Parquet dataset must return rows on a security-enabled cluster.
 *
 * <p>Two conditions make this a gate rather than a coincidence. The dataset declares its columns through {@code mappings}, which
 * resolves one anchor footer and leaves the others for split discovery, so discovery is the first read of every other file. And
 * {@code esql.external.max_concurrent_requests=1} (see {@link Clusters#securityEnabledTestCluster}) gives the external IO pool one
 * thread, so each later footer read is submitted by a thread released by the previous read's completion. A continuation that resumes
 * with no security context then fails the internal compute action with
 * {@code there should always be a user when sending a message for action [indices:data/read/esql/compute]}.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class ParquetDatasetSecurityIT extends AbstractFromDatasetSubqueryRestTestCase {

    private static final String DATA_SOURCE = "security_ctx_s3_ds";
    private static final String DATASET = "security_ctx_employees";
    private static final String PREFIX = WAREHOUSE + "/security_ctx_multifile/";
    private static final int FILES = 3;
    private static final int ROWS_PER_FILE = 10;

    public static DataSourcesS3HttpFixture s3Fixture = new DataSourcesS3HttpFixture();
    public static ElasticsearchCluster cluster = Clusters.securityEnabledTestCluster(() -> s3Fixture.getAddress());

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(s3Fixture).around(cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(Clusters.SECURITY_USER, new SecureString(Clusters.SECURITY_PASSWORD.toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @AfterClass
    public static void cleanupRegistry() throws IOException {
        deleteIgnoringMissing("/_query/dataset/" + DATASET);
        deleteIgnoringMissing("/_query/data_source/" + DATA_SOURCE);
    }

    public void testMultiFileDatasetReturnsRowsWithSecurityEnabled() throws Exception {
        BackendFixture s3Backend = new S3BackendFixture(s3Fixture);
        int expectedSalarySum = 0;
        for (int file = 0; file < FILES; file++) {
            EmployeeRow[] rows = new EmployeeRow[ROWS_PER_FILE];
            for (int i = 0; i < ROWS_PER_FILE; i++) {
                int salary = 1000 + file * ROWS_PER_FILE + i;
                rows[i] = new EmployeeRow(file * ROWS_PER_FILE + i, randomAlphaOfLength(8), randomAlphaOfLength(8), salary);
                expectedSalarySum += salary;
            }
            s3Backend.uploadBlob(PREFIX + "part-" + file + ".parquet", EmployeesParquetGenerator.employeesParquetBytes(rows));
        }
        putDataSource(DATA_SOURCE, s3Backend.dataSourceType(), s3Backend.dataSourceSettings());

        Request putDataset = new Request("PUT", "/_query/dataset/" + DATASET);
        putDataset.setJsonEntity(String.format(Locale.ROOT, """
            {
              "data_source": "%s",
              "resource": "%s",
              "settings": { "format": "parquet" },
              "mappings": {
                "properties": {
                  "emp_no": { "type": "integer" },
                  "first_name": { "type": "keyword" },
                  "last_name": { "type": "keyword" },
                  "salary": { "type": "integer" }
                }
              }
            }
            """, DATA_SOURCE, s3Backend.resourceUri(PREFIX + "*.parquet")));
        assertOK(client().performRequest(putDataset));

        for (int i = 0; i < 5; i++) {
            Map<String, Object> response = runQuery("FROM " + DATASET + " | STATS c = COUNT(*), s = SUM(salary) | LIMIT 1");
            @SuppressWarnings("unchecked")
            List<List<Object>> values = (List<List<Object>>) response.get("values");
            assertThat(values.size(), equalTo(1));
            assertThat(((Number) values.get(0).get(0)).intValue(), equalTo(FILES * ROWS_PER_FILE));
            assertThat(((Number) values.get(0).get(1)).intValue(), equalTo(expectedSalarySum));
        }
    }
}

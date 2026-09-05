/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.single_node;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.esql.CsvTestUtils;
import org.elasticsearch.xpack.esql.CsvTestsDataLoader;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/** REST coverage for PromQL vector matching. */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class PromqlVectorMatchingIT extends RestEsqlTestCase {

    private static final Path CSV_DATA_PATH = CsvTestUtils.createCsvDataDirectory();
    private static boolean loaded;

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster(CSV_DATA_PATH, spec -> {}, true);

    public PromqlVectorMatchingIT() {
        super(Mode.SYNC);
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Before
    public void loadK8sData() throws IOException {
        synchronized (PromqlVectorMatchingIT.class) {
            if (loaded) {
                return;
            }
            CsvTestsDataLoader.loadDataSetIntoEs(
                client(),
                true,
                true,
                false,
                true,
                cap -> hasCapabilities(adminClient(), List.of(cap.capabilityName())),
                List.of("k8s")
            );
            loaded = true;
        }
    }

    @Before
    public void requirePromqlVectorMatching() {
        assumeTrue(
            "PromQL vector matching capability is required",
            hasCapabilities(adminClient(), List.of(EsqlCapabilities.Cap.PROMQL_VECTOR_MATCHING_V0.capabilityName()))
        );
    }

    public void testArithmeticHappyPaths() throws IOException {
        assertRows(
            "PROMQL index=k8s step=10m result=(sum by (cluster) (network.eth0.tx) / ignoring () sum by (cluster) (network.eth0.rx)) "
                + "| WHERE cluster == \"qa\" AND step == \"2024-05-10T00:20:00.000Z\" | KEEP result, cluster, step",
            List.of(List.of(1.219419184511587, "qa", "2024-05-10T00:20:00.000Z"))
        );
        assertRows(
            "PROMQL index=k8s step=10m result=(sum by (cluster) (network.eth0.tx) + on (cluster) sum by (cluster) (network.eth0.rx)) "
                + "| WHERE cluster == \"prod\" AND step == \"2024-05-10T00:10:00.000Z\" | KEEP result, cluster, step",
            List.of(List.of(2036.0, "prod", "2024-05-10T00:10:00.000Z"))
        );
    }

    public void testComparisonHappyPaths() throws IOException {
        assertRows(
            "PROMQL index=k8s step=10m result=(sum by (cluster) (network.eth0.tx) == bool on (cluster) sum by (cluster) "
                + "(network.eth0.rx)) | WHERE cluster == \"prod\" AND step == \"2024-05-10T00:10:00.000Z\" "
                + "| KEEP result, cluster, step",
            List.of(List.of(0.0, "prod", "2024-05-10T00:10:00.000Z"))
        );
        assertRows(
            "PROMQL index=k8s step=10m result=(sum by (cluster) (network.eth0.tx) != on (cluster) sum by (cluster) "
                + "(network.eth0.rx)) | WHERE cluster == \"qa\" AND step == \"2024-05-10T00:30:00.000Z\" "
                + "| KEEP result, cluster, step",
            List.of(List.of(4337.0, "qa", "2024-05-10T00:30:00.000Z"))
        );
    }

    public void testGroupModifierHappyPaths() throws IOException {
        assertRows(
            "PROMQL index=k8s step=10m result=(sum by (cluster) (network.eth0.tx) / on (cluster) group_left (pod) "
                + "sum by (cluster, pod) (network.eth0.rx{pod=\"one\"})) "
                + "| WHERE cluster == \"prod\" AND pod == \"one\" AND step == \"2024-05-10T00:10:00.000Z\" "
                + "| KEEP result, cluster, pod, step",
            List.of(List.of(2.4272727272727272, "prod", "one", "2024-05-10T00:10:00.000Z"))
        );
        assertRows(
            "PROMQL index=k8s step=10m result=(sum by (cluster, pod) (network.eth0.tx{pod=\"one\"}) / on (cluster) "
                + "group_right (pod) sum by (cluster) (network.eth0.rx)) "
                + "| WHERE cluster == \"qa\" AND pod == \"one\" AND step == \"2024-05-10T00:20:00.000Z\" "
                + "| KEEP result, cluster, pod, step",
            List.of(List.of(0.3660897623936638, "qa", "one", "2024-05-10T00:20:00.000Z"))
        );
    }

    public void testOnLabelAbsentFromOneOperandMatchesNothing() throws IOException {
        // The probe side groups only by cluster, so its pod is PromQL's empty string; every build series carries a
        // concrete pod. Nothing matches - the query degrades gracefully to an empty result instead of failing to plan.
        assertRows(
            "PROMQL index=k8s step=10m result=(sum by (cluster) (network.eth0.tx) / on (cluster, pod) group_left (pod) "
                + "sum by (cluster, pod) (network.eth0.rx)) "
                + "| KEEP result, cluster, pod, step",
            List.of()
        );
    }

    public void testOrWithOnIsRejected() throws IOException {
        requirePromqlUnion();
        assertPromqlError(
            "PROMQL index=k8s step=10m " + "sum by (cluster) (network.eth0.tx) or on (cluster) sum by (cluster) (network.eth0.rx)",
            "set operator [or] with on/ignoring is not supported at this time"
        );
    }

    private void requirePromqlUnion() {
        assumeTrue(
            "PromQL union set operator capability is required",
            hasCapabilities(adminClient(), List.of(EsqlCapabilities.Cap.PROMQL_SET_OPERATOR_UNION.capabilityName()))
        );
    }

    private void assertPromqlError(String query, String expectedMessage) throws IOException {
        ResponseException error = expectThrows(ResponseException.class, () -> runEsqlSync(requestObjectBuilder().query(query)));
        assertThat(EntityUtils.toString(error.getResponse().getEntity()), containsString(expectedMessage));
    }

    private void assertRows(String query, List<List<Object>> expectedRows) throws IOException {
        Map<String, Object> response = runEsqlSync(requestObjectBuilder().query(query));
        @SuppressWarnings("unchecked")
        List<List<Object>> values = (List<List<Object>>) response.get("values");
        assertThat(values, equalTo(expectedRows));
    }
}

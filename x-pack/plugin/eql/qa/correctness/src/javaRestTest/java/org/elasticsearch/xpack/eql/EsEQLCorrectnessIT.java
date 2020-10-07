/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.client.HttpAsyncResponseConsumerFactory;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.eql.EqlSearchRequest;
import org.elasticsearch.client.eql.EqlSearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.ql.TestUtils.assertNoSearchContexts;

public class EsEQLCorrectnessIT extends ESRestTestCase {

    private static final String PARAM_FORMATTING = "%2$s";
    private static final String QUERIES_FILENAME = "queries.toml";
    private static final String INDEX_NAME = "mitre";
    private static final int FETCH_SIZE = 10000;
    private static RequestOptions COMMON_REQUEST_OPTIONS;
    private static RestHighLevelClient highLevelClient;

    @BeforeClass
    private static void init() {
        RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();
        builder.setHttpAsyncResponseConsumerFactory(
            new HttpAsyncResponseConsumerFactory.HeapBufferedResponseConsumerFactory(1000 * 1024 * 1024)
        );
        COMMON_REQUEST_OPTIONS = builder.build();
    }

    @Before
    private void setup() throws Exception {
        if (client().performRequest(new Request("HEAD", "/" + INDEX_NAME)).getStatusLine().getStatusCode() != 200) {
            highLevelClient().snapshot()
                .createRepository(
                    new PutRepositoryRequest("eql_correctness_gcs_repo").type("gcs")
                        .settings(
                            Settings.builder().put("bucket", "matriv-gcs").put("base_path", "mitre-data").put("client", "eql_test").build()
                        ),
                    RequestOptions.DEFAULT
                );
            highLevelClient().snapshot()
                .restore(
                    new RestoreSnapshotRequest("eql_correctness_gcs_repo", "mitre-snapshot").indices(INDEX_NAME).waitForCompletion(true),
                    RequestOptions.DEFAULT
                );
        }
    }

    @After
    public void checkSearchContent() throws Exception {
        assertNoSearchContexts(client());
    }

    @AfterClass
    public static void wipeTestData() throws IOException {
        try {
            adminClient().performRequest(new Request("DELETE", "/*"));
        } catch (ResponseException e) {
            // 404 here just means we had no indexes
            if (e.getResponse().getStatusLine().getStatusCode() != 404) {
                throw e;
            }
        }
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        // Need to preserve data between parameterized tests runs
        return true;
    }

    private EqlSpec spec;

    public EsEQLCorrectnessIT(EqlSpec spec) {
        this.spec = spec;
    }

    private RestHighLevelClient highLevelClient() {
        if (highLevelClient == null) {
            String host = getClusterHosts().get(0).getHostName();
            int port = getClusterHosts().get(0).getPort();
            highLevelClient = new RestHighLevelClient(
                RestClient.builder(new HttpHost(host, port, "http"))
                    .setRequestConfigCallback(
                        requestConfigBuilder -> requestConfigBuilder.setConnectTimeout(900000)
                            .setConnectionRequestTimeout(900000)
                            .setSocketTimeout(900000)
                    )
            );
        }
        return highLevelClient;
    }

    @ParametersFactory(shuffle = false, argumentFormatting = PARAM_FORMATTING)
    public static Iterable<Object[]> parameters() throws Exception {
        List<EqlSpec> specs;
        try (InputStream is = EsEQLCorrectnessIT.class.getClassLoader().getResourceAsStream(QUERIES_FILENAME)) {
            specs = EqlSpecLoader.readFromStream(is);
        }
        assertFalse("Found 0 queries for testing", specs.isEmpty());

        List<Object[]> params = new ArrayList<>(specs.size());
        for (EqlSpec spec : specs) {
            params.add(new Object[] { spec });
        }
        return params;
    }

    public void test() throws Exception {
        long totalTime = 0;
        int queryNo = spec.queryNo();
        /* For debugging
        for (int i = 0; i < spec.filters().length; i++) {
            String filterQuery = spec.filters()[i];
            EqlSearchRequest eqlSearchRequest = new EqlSearchRequest(INDEX_NAME, filterQuery);
            eqlSearchRequest.eventCategoryField("event_type");
            eqlSearchRequest.size(100000);
            EqlSearchResponse response = client.eql().search(eqlSearchRequest, commonRequestOptions);
            assertEquals("Failed to match filter counts for query No: " + queryNo + " filterCount: " + i,
                    spec.filterCounts()[i], response.hits().events().size());
        } */

        EqlSearchRequest eqlSearchRequest = new EqlSearchRequest(INDEX_NAME, spec.query());
        eqlSearchRequest.eventCategoryField("event_type");
        eqlSearchRequest.tiebreakerField("serial_id");
        eqlSearchRequest.size(2000);
        eqlSearchRequest.fetchSize(FETCH_SIZE);
        EqlSearchResponse response = highLevelClient.eql().search(eqlSearchRequest, RequestOptions.DEFAULT);
        totalTime += response.took();
        assertEquals(
            "Failed to match sequence count for query No: " + queryNo + " : " + spec.query(),
            spec.seqCount(),
            response.hits().sequences().size()
        );
        int expectedEvenIdIdx = 0;
        for (EqlSearchResponse.Sequence seq : response.hits().sequences()) {
            for (EqlSearchResponse.Event event : seq.events()) {
                assertEquals(
                    "Failed to match event ids for query No: " + queryNo + " : " + spec.query(),
                    spec.expectedEventIds()[expectedEvenIdIdx++],
                    ((Integer) event.sourceAsMap().get("serial_id")).longValue()
                );
            }
        }
        // System.out.println("Total time: " + totalTime + "ms");
    }
}

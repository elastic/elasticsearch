/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.http.HttpHost;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.TimeUnits;
import org.elasticsearch.client.HttpAsyncResponseConsumerFactory;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.eql.EqlSearchRequest;
import org.elasticsearch.client.eql.EqlSearchResponse;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.elasticsearch.xpack.ql.TestUtils.assertNoSearchContexts;

@TimeoutSuite(millis = 30 * TimeUnits.MINUTE)
@TestLogging(value = "org.elasticsearch.xpack.eql.EsEQLCorrectnessIT:INFO", reason = "Log query execution time")
public class EsEQLCorrectnessIT extends ESRestTestCase {

    private static final String PARAM_FORMATTING = "%1$s";
    private static final String QUERIES_FILENAME = "queries.toml";

    private static Properties CFG;
    private static RestHighLevelClient highLevelClient;
    private static RequestOptions COMMON_REQUEST_OPTIONS;
    private static long totalTime = 0;

    private static final Logger LOGGER = LogManager.getLogger(EsEQLCorrectnessIT.class);

    @BeforeClass
    public static void init() throws IOException {
        CFG = EqlDataLoader.loadConfiguration();

        RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();
        builder.setHttpAsyncResponseConsumerFactory(
            new HttpAsyncResponseConsumerFactory.HeapBufferedResponseConsumerFactory(1000 * 1024 * 1024)
        );
        COMMON_REQUEST_OPTIONS = builder.build();
    }

    @Before
    public void restoreDataFromGcsRepo() throws Exception {
        EqlDataLoader.restoreSnapshot(highLevelClient(), CFG);
    }

    @After
    public void checkSearchContent() throws Exception {
        assertNoSearchContexts(client());
    }

    @AfterClass
    public static void logTotalExecutionTime() {
        LOGGER.info("Total time: {} ms", totalTime);
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        // Need to preserve data between parameterized tests runs
        return true;
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("admin", new SecureString("admin-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Override
    protected RestClient buildClient(Settings settings, HttpHost[] hosts) throws IOException {
        RestClientBuilder builder = RestClient.builder(hosts);
        configureClient(builder, settings);
        builder.setRequestConfigCallback(
            requestConfigBuilder -> requestConfigBuilder.setConnectTimeout(30000000)
                .setConnectionRequestTimeout(30000000)
                .setSocketTimeout(30000000)
        );
        builder.setStrictDeprecationMode(true);
        return builder.build();
    }

    private final EqlSpec spec;

    public EsEQLCorrectnessIT(EqlSpec spec) {
        this.spec = spec;
    }

    private RestHighLevelClient highLevelClient() {
        if (highLevelClient == null) {
            highLevelClient = new RestHighLevelClient(client(), ignore -> {}, Collections.emptyList()) {
            };
        }
        return highLevelClient;
    }

    @ParametersFactory(shuffle = false, argumentFormatting = PARAM_FORMATTING)
    public static Iterable<Object[]> parameters() throws Exception {
        Collection<EqlSpec> specs;
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

    // To enable test of subqueries (filtering) results: -Dtests.eql_correctness_debug=true
    public void test() throws Exception {
        boolean debugMode = Boolean.parseBoolean(System.getProperty("tests.eql_correctness_debug", "false"));
        int queryNo = spec.queryNo();

        if (debugMode) {
            for (int i = 0; i < spec.filters().length; i++) {
                String filterQuery = spec.filters()[i];
                EqlSearchRequest eqlSearchRequest = new EqlSearchRequest(CFG.getProperty("index_name"), filterQuery);
                eqlSearchRequest.eventCategoryField("event_type");
                eqlSearchRequest.size(100000);
                EqlSearchResponse response = highLevelClient().eql().search(eqlSearchRequest, COMMON_REQUEST_OPTIONS);
                assertEquals(
                    "Failed to match filter counts for query No: " + queryNo + " filterCount: " + i,
                    spec.filterCounts()[i],
                    response.hits().events().size()
                );
            }
        }

        EqlSearchRequest eqlSearchRequest = new EqlSearchRequest(CFG.getProperty("index_name"), spec.query());
        eqlSearchRequest.eventCategoryField("event_type");
        eqlSearchRequest.tiebreakerField("serial_id");
        eqlSearchRequest.size(Integer.parseInt(CFG.getProperty("size")));
        eqlSearchRequest.fetchSize(Integer.parseInt(CFG.getProperty("fetch_size")));
        eqlSearchRequest.resultPosition(CFG.getProperty("result_position"));
        EqlSearchResponse response = highLevelClient().eql().search(eqlSearchRequest, RequestOptions.DEFAULT);
        long responseTime = response.took();
        LOGGER.info("QueryNo: {}, took: {}ms", queryNo, responseTime);
        totalTime += responseTime;
        assertEquals(
            "Failed to match sequence count for query No: " + queryNo + " : " + spec.query() + System.lineSeparator(),
            spec.seqCount(),
            response.hits().sequences().size()
        );
        int expectedEvenIdIdx = 0;
        for (EqlSearchResponse.Sequence seq : response.hits().sequences()) {
            for (EqlSearchResponse.Event event : seq.events()) {
                assertEquals(
                    "Failed to match event ids for query No: " + queryNo + " : " + spec.query() + System.lineSeparator(),
                    spec.expectedEventIds()[expectedEvenIdIdx++],
                    ((Integer) event.sourceAsMap().get("serial_id")).longValue()
                );
            }
        }
    }
}

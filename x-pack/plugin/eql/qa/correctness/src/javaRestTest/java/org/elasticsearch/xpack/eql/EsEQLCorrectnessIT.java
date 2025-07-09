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
import org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix;
import org.apache.lucene.tests.util.TimeUnits;
import org.elasticsearch.client.HttpAsyncResponseConsumerFactory;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.elasticsearch.xpack.ql.TestUtils.assertNoSearchContexts;

@TimeoutSuite(millis = 30 * TimeUnits.MINUTE)
@TestLogging(value = "org.elasticsearch.xpack.eql.EsEQLCorrectnessIT:INFO", reason = "Log query execution time")
@AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/112572")
public class EsEQLCorrectnessIT extends ESRestTestCase {

    private static final String PARAM_FORMATTING = "%1$s";
    private static final String QUERIES_FILENAME = "queries.toml";

    private static Properties CFG;
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
        EqlDataLoader.restoreSnapshot(client(), CFG);
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
    @SuppressWarnings("unchecked")
    public void test() throws Exception {
        boolean debugMode = Booleans.parseBoolean(System.getProperty("tests.eql_correctness_debug", "false"));
        int queryNo = spec.queryNo();

        if (debugMode) {
            for (int i = 0; i < spec.filters().length; i++) {
                XContentBuilder builder = JsonXContent.contentBuilder()
                    .startObject()
                    .field("query", spec.filters()[i])
                    .field("event_category_field", "event_type")
                    .field("size", 100000)
                    .endObject();

                Request request = new Request("POST", "/" + CFG.getProperty("index_name") + "/_eql/search");
                request.setOptions(COMMON_REQUEST_OPTIONS);
                request.setJsonEntity(Strings.toString(builder));

                ObjectPath response = ObjectPath.createFromResponse(client().performRequest(request));

                assertEquals(
                    "Failed to match filter counts for query No: " + queryNo + " filterCount: " + i,
                    spec.filterCounts()[i],
                    (long) response.evaluate("hits.events.size")
                );
            }
        }

        XContentBuilder builder = JsonXContent.contentBuilder()
            .startObject()
            .field("query", spec.query())
            .field("event_category_field", "event_type")
            .field("tiebreaker_field", "serial_id")
            .field("size", Integer.parseInt(CFG.getProperty("size")))
            .field("fetch_size", Integer.parseInt(CFG.getProperty("fetch_size")))
            .field("result_position", CFG.getProperty("result_position"))
            .endObject();

        Request request = new Request("POST", "/" + CFG.getProperty("index_name") + "/_eql/search");
        request.setOptions(RequestOptions.DEFAULT);
        request.setJsonEntity(Strings.toString(builder));

        ObjectPath response = ObjectPath.createFromResponse(client().performRequest(request));

        int responseTime = response.evaluate("took");
        LOGGER.info("QueryNo: {}, took: {}ms", queryNo, responseTime);
        totalTime += responseTime;

        List<Map<String, Object>> sequences = response.evaluate("hits.sequences");
        assertEquals(
            "Failed to match sequence count for query No: " + queryNo + " : " + spec.query() + System.lineSeparator(),
            spec.seqCount(),
            sequences.size()
        );
        int expectedEvenIdIdx = 0;
        for (Map<String, Object> seq : sequences) {
            List<Map<String, Object>> events = (List<Map<String, Object>>) seq.get("events");
            for (Map<String, Object> event : events) {
                Map<String, Object> source = (Map<String, Object>) event.get("_source");
                assertEquals(
                    "Failed to match event ids for query No: " + queryNo + " : " + spec.query() + System.lineSeparator(),
                    spec.expectedEventIds()[expectedEvenIdIdx++],
                    ((Integer) source.get("serial_id")).longValue()
                );
            }
        }
    }
}

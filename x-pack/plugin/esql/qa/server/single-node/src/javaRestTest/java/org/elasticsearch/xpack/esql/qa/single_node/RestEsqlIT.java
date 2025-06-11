/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.qa.single_node;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.http.HttpEntity;
import org.apache.http.util.EntityUtils;
import org.apache.lucene.search.DocIdSetIterator;
import org.elasticsearch.Build;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ListMatcher;
import org.elasticsearch.test.MapMatcher;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.LogType;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase;
import org.elasticsearch.xpack.esql.tools.ProfileParser;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.ClassRule;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.Mode.SYNC;
import static org.elasticsearch.xpack.esql.tools.ProfileParser.parseProfile;
import static org.elasticsearch.xpack.esql.tools.ProfileParser.readProfileFromResponse;
import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.oneOf;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.core.Is.is;

@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class RestEsqlIT extends RestEsqlTestCase {
    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster(
        specBuilder -> specBuilder.plugin("mapper-size").plugin("mapper-murmur3")
    );

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @ParametersFactory(argumentFormatting = "%1s")
    public static List<Object[]> modes() {
        return Arrays.stream(Mode.values()).map(m -> new Object[] { m }).toList();
    }

    public RestEsqlIT(Mode mode) {
        super(mode);
    }

    public void testBasicEsql() throws IOException {
        indexTimestampData(1);

        RequestObjectBuilder builder = requestObjectBuilder().query(fromIndex() + " | stats avg(value)");
        if (Build.current().isSnapshot()) {
            builder.pragmas(Settings.builder().put("data_partitioning", "shard").build());
        }
        Map<String, Object> result = runEsql(builder);

        Map<String, String> colA = Map.of("name", "avg(value)", "type", "double");
        assertResultMap(result, List.of(colA), List.of(List.of(499.5d)));
        assertTrue(result.containsKey("took"));
    }

    public void testInvalidPragma() throws IOException {
        assumeTrue("pragma only enabled on snapshot builds", Build.current().isSnapshot());
        createIndex("test-index");
        for (int i = 0; i < 10; i++) {
            Request request = new Request("POST", "/test-index/_doc/");
            request.addParameter("refresh", "true");
            request.setJsonEntity("{\"f\":" + i + "}");
            assertOK(client().performRequest(request));
        }
        RequestObjectBuilder builder = requestObjectBuilder().query("from test-index | limit 1 | keep f").allowPartialResults(false);
        builder.pragmas(Settings.builder().put("data_partitioning", "invalid-option").build());
        ResponseException re = expectThrows(ResponseException.class, () -> runEsqlSync(builder));
        assertThat(EntityUtils.toString(re.getResponse().getEntity()), containsString("No enum constant"));

        assertThat(deleteIndex("test-index").isAcknowledged(), is(true)); // clean up
    }

    public void testPragmaNotAllowed() throws IOException {
        assumeFalse("pragma only disabled on release builds", Build.current().isSnapshot());
        RequestObjectBuilder builder = requestObjectBuilder().query("row a = 1, b = 2");
        builder.pragmas(Settings.builder().put("data_partitioning", "shard").build());
        ResponseException re = expectThrows(ResponseException.class, () -> runEsqlSync(builder));
        assertThat(EntityUtils.toString(re.getResponse().getEntity()), containsString("[pragma] only allowed in snapshot builds"));
    }

    public void testDoNotLogWithInfo() throws IOException {
        try {
            setLoggingLevel("INFO");
            RequestObjectBuilder builder = requestObjectBuilder().query("ROW DO_NOT_LOG_ME = 1");
            Map<String, Object> result = runEsql(builder);
            Map<String, String> colA = Map.of("name", "DO_NOT_LOG_ME", "type", "integer");
            assertResultMap(result, List.of(colA), List.of(List.of(1)));
            for (int i = 0; i < cluster.getNumNodes(); i++) {
                try (InputStream log = cluster.getNodeLog(i, LogType.SERVER)) {
                    Streams.readAllLines(log, line -> assertThat(line, not(containsString("DO_NOT_LOG_ME"))));
                }
            }
        } finally {
            setLoggingLevel(null);
        }
    }

    public void testDoLogWithDebug() throws IOException {
        try {
            setLoggingLevel("DEBUG");
            RequestObjectBuilder builder = requestObjectBuilder().query("ROW DO_LOG_ME = 1");
            Map<String, Object> result = runEsql(builder);
            Map<String, String> colA = Map.of("name", "DO_LOG_ME", "type", "integer");
            assertResultMap(result, List.of(colA), List.of(List.of(1)));
            boolean[] found = new boolean[] { false };
            for (int i = 0; i < cluster.getNumNodes(); i++) {
                try (InputStream log = cluster.getNodeLog(i, LogType.SERVER)) {
                    Streams.readAllLines(log, line -> {
                        if (line.contains("DO_LOG_ME")) {
                            found[0] = true;
                        }
                    });
                }
            }
            assertThat(found[0], equalTo(true));
        } finally {
            setLoggingLevel(null);
        }
    }

    private void setLoggingLevel(String level) throws IOException {
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity("""
            {
                "persistent": {
                    "logger.org.elasticsearch.xpack.esql.action": $LEVEL$
                }
            }
            """.replace("$LEVEL$", level == null ? "null" : '"' + level + '"'));
        client().performRequest(request);
    }

    public void testIncompatibleMappingsErrors() throws IOException {
        // create first index
        Request request = new Request("PUT", "/index1");
        request.setJsonEntity("""
            {
               "mappings": {
                 "_size": {
                   "enabled": true
                 },
                 "properties": {
                   "message": {
                     "type": "keyword",
                     "fields": {
                       "hash": {
                         "type": "murmur3"
                       }
                     }
                   }
                 }
               }
            }
            """);
        assertEquals(200, client().performRequest(request).getStatusLine().getStatusCode());

        // create second index
        request = new Request("PUT", "/index2");
        request.setJsonEntity("""
            {
              "mappings": {
                "properties": {
                  "message": {
                    "type": "long",
                    "fields": {
                      "hash": {
                        "type": "integer"
                      }
                    }
                  }
                }
              }
            }
            """);
        assertEquals(200, client().performRequest(request).getStatusLine().getStatusCode());

        // create alias
        request = new Request("POST", "/_aliases");
        request.setJsonEntity("""
            {
              "actions": [
                {
                  "add": {
                    "index": "index1",
                    "alias": "test_alias"
                  }
                },
                {
                  "add": {
                    "index": "index2",
                    "alias": "test_alias"
                  }
                }
              ]
            }
            """);
        assertEquals(200, client().performRequest(request).getStatusLine().getStatusCode());
        assertException(
            "from index1,index2 | stats count(message)",
            "VerificationException",
            "Cannot use field [message] due to ambiguities",
            "incompatible types: [keyword] in [index1], [long] in [index2]"
        );
        assertException(
            "from test_alias | where message is not null",
            "VerificationException",
            "Cannot use field [message] due to ambiguities",
            "incompatible types: [keyword] in [index1], [long] in [index2]"
        );
        assertException("from test_alias | where _size is not null | limit 1", "Unknown column [_size]");
        assertException(
            "from test_alias | where message.hash is not null | limit 1",
            "Cannot use field [message.hash] with unsupported type [murmur3]"
        );
        assertException(
            "from index1 | where message.hash is not null | limit 1",
            "Cannot use field [message.hash] with unsupported type [murmur3]"
        );
        // clean up
        assertThat(deleteIndex("index1").isAcknowledged(), Matchers.is(true));
        assertThat(deleteIndex("index2").isAcknowledged(), Matchers.is(true));
    }

    public void testTableDuplicateNames() throws IOException {
        Request request = new Request("POST", "/_query");
        request.setJsonEntity("""
            {
              "query": "FROM a=1",
              "tables": {
                "t": {
                  "a": {"integer": [1]},
                  "a": {"integer": [1]}
                }
              }
            }""");
        ResponseException re = expectThrows(ResponseException.class, () -> client().performRequest(request));
        assertThat(re.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(re.getMessage(), containsString("[6:10] Duplicate field 'a'"));
    }

    public void testProfile() throws IOException {
        indexTimestampData(1);

        RequestObjectBuilder builder = requestObjectBuilder().query(fromIndex() + " | STATS AVG(value)");
        builder.profile(true);
        if (Build.current().isSnapshot()) {
            // Lock to shard level partitioning, so we get consistent profile output
            builder.pragmas(Settings.builder().put("data_partitioning", "shard").build());
        }
        Map<String, Object> result = runEsql(builder);
        assertResultMap(
            result,
            getResultMatcher(result).entry("profile", getProfileMatcher()),
            matchesList().item(matchesMap().entry("name", "AVG(value)").entry("type", "double")),
            equalTo(List.of(List.of(499.5d)))
        );

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> profiles = (List<Map<String, Object>>) ((Map<String, Object>) result.get("profile")).get("drivers");
        for (Map<String, Object> p : profiles) {
            fixTypesOnProfile(p);
            assertThat(p, commonProfile());
            List<String> sig = new ArrayList<>();
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> operators = (List<Map<String, Object>>) p.get("operators");
            for (Map<String, Object> o : operators) {
                sig.add(checkOperatorProfile(o));
            }
            String taskDescription = p.get("task_description").toString();
            switch (taskDescription) {
                case "data" -> assertMap(
                    sig,
                    matchesList().item("LuceneSourceOperator")
                        .item("ValuesSourceReaderOperator")
                        .item("AggregationOperator")
                        .item("ExchangeSinkOperator")
                );
                case "node_reduce" -> assertThat(
                    sig,
                    either(matchesList().item("ExchangeSourceOperator").item("ExchangeSinkOperator")).or(
                        matchesList().item("ExchangeSourceOperator").item("AggregationOperator").item("ExchangeSinkOperator")
                    )
                );
                case "final" -> assertMap(
                    sig,
                    matchesList().item("ExchangeSourceOperator")
                        .item("AggregationOperator")
                        .item("ProjectOperator")
                        .item("LimitOperator")
                        .item("EvalOperator")
                        .item("ProjectOperator")
                        .item("OutputOperator")
                );
                default -> throw new IllegalArgumentException("can't match " + taskDescription);
            }
        }
    }

    private final String PROCESS_NAME = "process_name";
    private final String THREAD_NAME = "thread_name";

    @SuppressWarnings("unchecked")
    public void testProfileParsing() throws IOException {
        indexTimestampData(1);

        RequestObjectBuilder builder = new RequestObjectBuilder(XContentType.JSON).query(fromIndex() + " | stats avg(value)").profile(true);
        Request request = prepareRequestWithOptions(builder, SYNC);
        HttpEntity response = performRequest(request).getEntity();

        ProfileParser.Profile profile;
        try (InputStream responseContent = response.getContent()) {
            profile = readProfileFromResponse(responseContent);
        }

        ByteArrayOutputStream os = new ByteArrayOutputStream();
        try (XContentBuilder jsonOutputBuilder = new XContentBuilder(JsonXContent.jsonXContent, os)) {
            parseProfile(profile, jsonOutputBuilder);
        }

        // Read the written JSON again into a map, so we can make assertions on it
        ByteArrayInputStream profileJson = new ByteArrayInputStream(os.toByteArray());
        Map<String, Object> parsedProfile = XContentHelper.convertToMap(JsonXContent.jsonXContent, profileJson, true);

        assertEquals("ns", parsedProfile.get("displayTimeUnit"));
        List<Map<String, Object>> events = (List<Map<String, Object>>) parsedProfile.get("traceEvents");
        // At least 1 metadata event to declare the node, and 2 events each for the data, node_reduce and final drivers, resp.
        assertThat(events.size(), greaterThanOrEqualTo(7));

        String clusterName = "test-cluster";
        Set<String> expectedProcessNames = new HashSet<>();
        for (int i = 0; i < cluster.getNumNodes(); i++) {
            expectedProcessNames.add(clusterName + ":" + cluster.getName(i));
        }
        // Workaround for the missing driver fields (missing backport to 8.x).
        expectedProcessNames.add("missing node/cluster name");

        int seenNodes = 0;
        int seenDrivers = 0;
        // Declaration of each node as a "process" via a metadata event (phase `ph` is `M`)
        // First event has to declare the first seen node.
        Map<String, Object> nodeMetadata = events.get(0);
        assertProcessMetadataForNextNode(nodeMetadata, expectedProcessNames, seenNodes++);

        // The rest should be pairs of 2 events: first, a metadata event, declaring 1 "thread" per driver in the profile, then
        // a "complete" event (phase `ph` is `X`) with a timestamp, duration `dur`, thread duration `tdur` (cpu time) and additional
        // arguments obtained from the driver.
        // Except when run as part of the Serverless tests, which can involve more than 1 node - in which case, there will be more node
        // metadata events.
        for (int i = 1; i < events.size() - 1;) {
            String eventName = (String) events.get(i).get("name");
            assertTrue(Set.of(THREAD_NAME, PROCESS_NAME).contains(eventName));
            if (eventName.equals(THREAD_NAME)) {
                Map<String, Object> metadataEventForDriver = events.get(i);
                Map<String, Object> eventForDriver = events.get(i + 1);
                assertDriverData(metadataEventForDriver, eventForDriver, seenNodes, seenDrivers);
                i = i + 2;
                seenDrivers++;
            } else if (eventName.equals(PROCESS_NAME)) {
                Map<String, Object> metadataEventForNode = events.get(i);
                assertProcessMetadataForNextNode(metadataEventForNode, expectedProcessNames, seenNodes);
                i++;
                seenNodes++;
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void assertProcessMetadataForNextNode(Map<String, Object> nodeMetadata, Set<String> expectedNamesForNodes, int seenNodes) {
        assertEquals("M", nodeMetadata.get("ph"));
        assertEquals(PROCESS_NAME, nodeMetadata.get("name"));
        assertEquals(seenNodes, nodeMetadata.get("pid"));

        Map<String, Object> nodeMetadataArgs = (Map<String, Object>) nodeMetadata.get("args");
        assertTrue(expectedNamesForNodes.contains((String) nodeMetadataArgs.get("name")));
    }

    @SuppressWarnings("unchecked")
    public void assertDriverData(Map<String, Object> driverMetadata, Map<String, Object> driverEvent, int seenNodes, int seenDrivers) {
        assertEquals("M", driverMetadata.get("ph"));
        assertEquals(THREAD_NAME, driverMetadata.get("name"));
        assertTrue((int) driverMetadata.get("pid") < seenNodes);
        assertEquals(seenDrivers, driverMetadata.get("tid"));
        Map<String, Object> driverMetadataArgs = (Map<String, Object>) driverMetadata.get("args");
        String driverType = (String) driverMetadataArgs.get("name");
        // Workaround for the missing driver fields (missing backport to 8.x).
        assertThat(driverType, oneOf("data", "node_reduce", "final", "missing driver description"));

        assertEquals("X", driverEvent.get("ph"));
        // Workaround for the missing driver fields (missing backport to 8.x).
        assertThat((String) driverEvent.get("name"), either(startsWith(driverType)).or(equalTo("missing driver description")));
        // Category used to implicitly colour-code and group drivers
        assertEquals(driverType, driverEvent.get("cat"));
        assertTrue((int) driverEvent.get("pid") < seenNodes);
        assertEquals(seenDrivers, driverEvent.get("tid"));
        long timestampMillis = (long) driverEvent.get("ts");
        double durationMicros = (double) driverEvent.get("dur");
        double cpuDurationMicros = (double) driverEvent.get("tdur");
        assertTrue(timestampMillis >= 0);
        assertTrue(durationMicros >= 0);
        assertTrue(cpuDurationMicros >= 0);
        assertTrue(durationMicros >= cpuDurationMicros);

        // This should contain the essential information from a driver, like its operators, and will be just attached to the slice/
        // visible when clicking on it.
        Map<String, Object> driverSliceArgs = (Map<String, Object>) driverEvent.get("args");
        assertNotNull(driverSliceArgs.get("cpu_nanos"));
        assertNotNull(driverSliceArgs.get("took_nanos"));
        assertNotNull(driverSliceArgs.get("iterations"));
        assertNotNull(driverSliceArgs.get("sleeps"));
        assertThat(((List<String>) driverSliceArgs.get("operators")), not(empty()));
    }

    public void testProfileOrdinalsGroupingOperator() throws IOException {
        assumeTrue("requires pragmas", Build.current().isSnapshot());
        indexTimestampData(1);

        RequestObjectBuilder builder = requestObjectBuilder().query(fromIndex() + " | STATS AVG(value) BY test.keyword");
        builder.profile(true);
        // Lock to shard level partitioning, so we get consistent profile output
        builder.pragmas(Settings.builder().put("data_partitioning", "shard").build());
        Map<String, Object> result = runEsql(builder);

        List<List<String>> signatures = new ArrayList<>();
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> profiles = (List<Map<String, Object>>) ((Map<String, Object>) result.get("profile")).get("drivers");
        for (Map<String, Object> p : profiles) {
            fixTypesOnProfile(p);
            assertThat(p, commonProfile());
            List<String> sig = new ArrayList<>();
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> operators = (List<Map<String, Object>>) p.get("operators");
            for (Map<String, Object> o : operators) {
                sig.add((String) o.get("operator"));
            }
            signatures.add(sig);
        }

        assertThat(signatures, hasItem(hasItem("OrdinalsGroupingOperator[aggregators=[\"sum of longs\", \"count\"]]")));
    }

    @AwaitsFix(bugUrl = "disabled until JOIN infrastructrure properly lands")
    public void testInlineStatsProfile() throws IOException {
        assumeTrue("INLINESTATS only available on snapshots", Build.current().isSnapshot());
        indexTimestampData(1);

        RequestObjectBuilder builder = requestObjectBuilder().query(fromIndex() + " | INLINESTATS AVG(value) | SORT value ASC");
        builder.profile(true);
        if (Build.current().isSnapshot()) {
            // Lock to shard level partitioning, so we get consistent profile output
            builder.pragmas(Settings.builder().put("data_partitioning", "shard").build());
        }

        Map<String, Object> result = runEsql(builder);
        ListMatcher values = matchesList();
        for (int i = 0; i < 1000; i++) {
            values = values.item(matchesList().item("2020-12-12T00:00:00.000Z").item("value" + i).item("value" + i).item(i).item(499.5));
        }
        assertResultMap(
            result,
            getResultMatcher(result).entry("profile", getProfileMatcher()),
            matchesList().item(matchesMap().entry("name", "@timestamp").entry("type", "date"))
                .item(matchesMap().entry("name", "test").entry("type", "text"))
                .item(matchesMap().entry("name", "test.keyword").entry("type", "keyword"))
                .item(matchesMap().entry("name", "value").entry("type", "long"))
                .item(matchesMap().entry("name", "AVG(value)").entry("type", "double")),
            values
        );

        List<List<String>> signatures = new ArrayList<>();
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> profiles = (List<Map<String, Object>>) ((Map<String, Object>) result.get("profile")).get("drivers");
        for (Map<String, Object> p : profiles) {
            fixTypesOnProfile(p);
            assertThat(p, commonProfile());
            List<String> sig = new ArrayList<>();
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> operators = (List<Map<String, Object>>) p.get("operators");
            for (Map<String, Object> o : operators) {
                sig.add(checkOperatorProfile(o));
            }
            signatures.add(sig);
        }
        // TODO adapt this to use task_description once this is reenabled
        assertThat(
            signatures,
            containsInAnyOrder(
                // First pass read and start agg
                matchesList().item("LuceneSourceOperator")
                    .item("ValuesSourceReaderOperator")
                    .item("AggregationOperator")
                    .item("ExchangeSinkOperator"),
                // First pass node level reduce
                matchesList().item("ExchangeSourceOperator").item("ExchangeSinkOperator"),
                // First pass finish agg
                matchesList().item("ExchangeSourceOperator")
                    .item("AggregationOperator")
                    .item("ProjectOperator")
                    .item("EvalOperator")
                    .item("ProjectOperator")
                    .item("OutputOperator"),
                // Second pass read and join via eval
                matchesList().item("LuceneTopNSourceOperator")
                    .item("EvalOperator")
                    .item("ValuesSourceReaderOperator")
                    .item("ProjectOperator")
                    .item("ExchangeSinkOperator"),
                // Second pass node level reduce
                matchesList().item("ExchangeSourceOperator").item("ExchangeSinkOperator"),
                // Second pass finish
                matchesList().item("ExchangeSourceOperator").item("TopNOperator").item("OutputOperator")
            )
        );
    }

    public void testForceSleepsProfile() throws IOException {
        assumeTrue("requires pragmas", Build.current().isSnapshot());

        Request createIndex = new Request("PUT", testIndexName());
        createIndex.setJsonEntity("""
            {
              "settings": {
                "index": {
                  "number_of_shards": 1
                }
              }
            }""");
        Response response = client().performRequest(createIndex);
        assertThat(
            entityToMap(response.getEntity(), XContentType.JSON),
            matchesMap().entry("shards_acknowledged", true).entry("index", testIndexName()).entry("acknowledged", true)
        );

        int groupCount = 300;
        for (int group1 = 0; group1 < groupCount; group1++) {
            StringBuilder b = new StringBuilder();
            for (int group2 = 0; group2 < groupCount; group2++) {
                b.append(String.format(Locale.ROOT, """
                    {"create":{"_index":"%s"}}
                    {"@timestamp":"2020-12-12","value":1,"group1":%d,"group2":%d}
                    """, testIndexName(), group1, group2));
            }
            Request bulk = new Request("POST", "/_bulk");
            bulk.addParameter("refresh", "true");
            bulk.addParameter("filter_path", "errors");
            bulk.setJsonEntity(b.toString());
            response = client().performRequest(bulk);
            Assert.assertEquals("{\"errors\":false}", EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8));
        }

        RequestObjectBuilder builder = requestObjectBuilder().query(
            fromIndex() + " | STATS AVG(value), MAX(value), MIN(value) BY group1, group2 | SORT group1, group2 ASC | LIMIT 10"
        );
        // Lock to shard level partitioning, so we get consistent profile output
        builder.pragmas(Settings.builder().put("data_partitioning", "shard").put("page_size", 10).build());
        builder.profile(true);
        Map<String, Object> result = runEsql(builder);
        List<List<?>> expectedValues = new ArrayList<>();
        for (int group2 = 0; group2 < 10; group2++) {
            expectedValues.add(List.of(1.0, 1, 1, 0, group2));
        }
        assertResultMap(
            result,
            getResultMatcher(result).entry("profile", getProfileMatcher()),
            matchesList().item(matchesMap().entry("name", "AVG(value)").entry("type", "double"))
                .item(matchesMap().entry("name", "MAX(value)").entry("type", "long"))
                .item(matchesMap().entry("name", "MIN(value)").entry("type", "long"))
                .item(matchesMap().entry("name", "group1").entry("type", "long"))
                .item(matchesMap().entry("name", "group2").entry("type", "long")),
            equalTo(expectedValues)
        );

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> profiles = (List<Map<String, Object>>) ((Map<String, Object>) result.get("profile")).get("drivers");

        for (Map<String, Object> p : profiles) {
            fixTypesOnProfile(p);
            assertMap(p, commonProfile());
            @SuppressWarnings("unchecked")
            Map<String, Object> sleeps = (Map<String, Object>) p.get("sleeps");
            String operators = p.get("operators").toString();
            MapMatcher sleepMatcher = matchesMap().entry("reason", "exchange empty")
                .entry("sleep_millis", greaterThan(0L))
                .entry("wake_millis", greaterThan(0L));
            String taskDescription = p.get("task_description").toString();
            switch (taskDescription) {
                case "data" -> assertMap(sleeps, matchesMap().entry("counts", Map.of()).entry("first", List.of()).entry("last", List.of()));
                case "node_reduce" -> {
                    assertMap(sleeps, matchesMap().entry("counts", matchesMap().entry("exchange empty", greaterThan(0))).extraOk());
                    @SuppressWarnings("unchecked")
                    List<Map<String, Object>> first = (List<Map<String, Object>>) sleeps.get("first");
                    for (Map<String, Object> s : first) {
                        assertMap(s, sleepMatcher);
                    }
                    @SuppressWarnings("unchecked")
                    List<Map<String, Object>> last = (List<Map<String, Object>>) sleeps.get("last");
                    for (Map<String, Object> s : last) {
                        assertMap(s, sleepMatcher);
                    }
                }
                case "final" -> {
                    assertMap(
                        sleeps,
                        matchesMap().entry("counts", matchesMap().entry("exchange empty", 1))
                            .entry("first", List.of(sleepMatcher))
                            .entry("last", List.of(sleepMatcher))
                    );
                }
                default -> throw new IllegalArgumentException("unknown task: " + taskDescription);
            }
        }
    }

    public static MapMatcher commonProfile() {
        return matchesMap() //
            .entry("task_description", any(String.class))
            .entry("start_millis", greaterThan(0L))
            .entry("stop_millis", greaterThan(0L))
            .entry("iterations", greaterThan(0L))
            .entry("cpu_nanos", greaterThan(0L))
            .entry("took_nanos", greaterThan(0L))
            .entry("operators", instanceOf(List.class))
            .entry("sleeps", matchesMap().extraOk());
    }

    /**
     * Fix some of the types on the profile results. Sometimes they
     * come back as integers and sometimes longs. This just promotes
     * them to long every time.
     */
    public static void fixTypesOnProfile(Map<String, Object> profile) {
        profile.put("iterations", ((Number) profile.get("iterations")).longValue());
        profile.put("cpu_nanos", ((Number) profile.get("cpu_nanos")).longValue());
        profile.put("took_nanos", ((Number) profile.get("took_nanos")).longValue());
    }

    private String checkOperatorProfile(Map<String, Object> o) {
        String name = (String) o.get("operator");
        name = name.replaceAll("\\[.+", "");
        MapMatcher status = switch (name) {
            case "LuceneSourceOperator" -> matchesMap().entry("processed_slices", greaterThan(0))
                .entry("processed_shards", List.of(testIndexName() + ":0"))
                .entry("total_slices", greaterThan(0))
                .entry("slice_index", 0)
                .entry("slice_max", 0)
                .entry("slice_min", 0)
                .entry("current", DocIdSetIterator.NO_MORE_DOCS)
                .entry("pages_emitted", greaterThan(0))
                .entry("rows_emitted", greaterThan(0))
                .entry("processing_nanos", greaterThan(0))
                .entry("processed_queries", List.of("*:*"))
                .entry("partitioning_strategies", matchesMap().entry("rest-esql-test:0", "SHARD"));
            case "ValuesSourceReaderOperator" -> basicProfile().entry("readers_built", matchesMap().extraOk());
            case "AggregationOperator" -> matchesMap().entry("pages_processed", greaterThan(0))
                .entry("rows_received", greaterThan(0))
                .entry("rows_emitted", greaterThan(0))
                .entry("aggregation_nanos", greaterThan(0))
                .entry("aggregation_finish_nanos", greaterThan(0));
            case "ExchangeSinkOperator" -> matchesMap().entry("pages_received", greaterThan(0)).entry("rows_received", greaterThan(0));
            case "ExchangeSourceOperator" -> matchesMap().entry("pages_waiting", 0)
                .entry("pages_emitted", greaterThan(0))
                .entry("rows_emitted", greaterThan(0));
            case "ProjectOperator", "EvalOperator" -> basicProfile();
            case "LimitOperator" -> matchesMap().entry("pages_processed", greaterThan(0))
                .entry("limit", 1000)
                .entry("limit_remaining", 999)
                .entry("rows_received", greaterThan(0))
                .entry("rows_emitted", greaterThan(0));
            case "OutputOperator" -> null;
            case "TopNOperator" -> matchesMap().entry("occupied_rows", 0)
                .entry("pages_received", greaterThan(0))
                .entry("pages_emitted", greaterThan(0))
                .entry("rows_received", greaterThan(0))
                .entry("rows_emitted", greaterThan(0))
                .entry("ram_used", instanceOf(String.class))
                .entry("ram_bytes_used", greaterThan(0));
            case "LuceneTopNSourceOperator" -> matchesMap().entry("pages_emitted", greaterThan(0))
                .entry("rows_emitted", greaterThan(0))
                .entry("current", greaterThan(0))
                .entry("processed_slices", greaterThan(0))
                .entry("processed_shards", List.of("rest-esql-test:0"))
                .entry("total_slices", greaterThan(0))
                .entry("slice_max", 0)
                .entry("slice_min", 0)
                .entry("processing_nanos", greaterThan(0))
                .entry("processed_queries", List.of("*:*"))
                .entry("slice_index", 0);
            default -> throw new AssertionError("unexpected status: " + o);
        };
        MapMatcher expectedOp = matchesMap().entry("operator", startsWith(name));
        if (status != null) {
            expectedOp = expectedOp.entry("status", status);
        }
        assertMap(o, expectedOp);
        return name;
    }

    private MapMatcher basicProfile() {
        return matchesMap().entry("pages_processed", greaterThan(0))
            .entry("process_nanos", greaterThan(0))
            .entry("rows_received", greaterThan(0))
            .entry("rows_emitted", greaterThan(0));
    }

    private void assertException(String query, String... errorMessages) throws IOException {
        ResponseException re = expectThrows(ResponseException.class, () -> runEsqlSync(requestObjectBuilder().query(query)));
        assertThat(re.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        for (var error : errorMessages) {
            assertThat(re.getMessage(), containsString(error));
        }
    }
}

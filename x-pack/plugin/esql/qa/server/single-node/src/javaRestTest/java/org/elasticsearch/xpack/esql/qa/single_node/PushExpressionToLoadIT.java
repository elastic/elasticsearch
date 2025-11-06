/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.single_node;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.test.MapMatcher;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.AssertWarnings;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.qa.rest.ProfileLogger;
import org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.entityToMap;
import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.requestObjectBuilder;
import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.runEsql;
import static org.elasticsearch.xpack.esql.qa.single_node.RestEsqlIT.commonProfile;
import static org.elasticsearch.xpack.esql.qa.single_node.RestEsqlIT.fixTypesOnProfile;
import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;

/**
 * Tests for pushing expressions into field loading.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class PushExpressionToLoadIT extends ESRestTestCase {
    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster(spec -> spec.plugin("inference-service-test"));

    @Rule(order = Integer.MIN_VALUE)
    public ProfileLogger profileLogger = new ProfileLogger();

    @Before
    public void checkPushCapability() throws IOException {
        assumeTrue(
            "requires " + EsqlCapabilities.Cap.VECTOR_SIMILARITY_FUNCTIONS_PUSHDOWN.capabilityName(),
            clusterHasCapability(
                "POST",
                "_query",
                List.of(),
                List.of(EsqlCapabilities.Cap.VECTOR_SIMILARITY_FUNCTIONS_PUSHDOWN.capabilityName())
            ).orElseGet(() -> false)
        );
    }

    public void testLengthToKeyword() throws IOException {
        String value = "v".repeat(between(0, 256));
        test(
            justType("keyword"),
            b -> b.field("test", value),
            "| EVAL test = LENGTH(test)",
            matchesList().item(value.length()),
            matchesMap().entry("test:column_at_a_time:Utf8CodePointsFromOrds.Singleton", 1)
        );
    }

    public void testLengthNotPushedToWildcard() throws IOException {
        String value = "v".repeat(between(0, 256));
        test(
            justType("wildcard"),
            b -> b.field("test", value),
            "| EVAL test = LENGTH(test)",
            matchesList().item(value.length()),
            matchesMap().entry("test:column_at_a_time:BlockDocValuesReader.BytesCustom", 1)
        );
    }

    public void testLengthNotPushedToText() throws IOException {
        String value = "v".repeat(between(0, 256));
        test(
            justType("text"),
            b -> b.field("test", value),
            "| EVAL test = LENGTH(test)",
            matchesList().item(value.length()),
            matchesMap().entry("test:column_at_a_time:null", 1)
                .entry("stored_fields[requires_source:true, fields:0, sequential: false]", 1)
                .entry("test:row_stride:BlockSourceReader.Bytes", 1)
        );
    }

    /**
     * Tests {@code LENGTH} on a field that comes from a {@code LOOKUP JOIN}.
     */
    public void testLengthNotPushedToLookupJoinKeyword() throws IOException {
        initLookupIndex();
        test(
            b -> b.startObject("main_matching").field("type", "keyword").endObject(),
            b -> b.field("main_matching", "lookup"),
            """
                | LOOKUP JOIN lookup ON matching == main_matching
                | EVAL test = LENGTH(test)
                """,
            matchesList().item(1),
            matchesMap().entry("main_matching:column_at_a_time:BytesRefsFromOrds.Singleton", 1), //
            sig -> {}
        );
    }

    /**
     * Tests {@code LENGTH} on a field that comes from a {@code LOOKUP JOIN} with
     * the added complexity that the field also exists in the index, but we're not
     * querying it.
     */
    public void testLengthNotPushedToLookupJoinKeywordSameName() throws IOException {
        assumeFalse("fix in 137679 - we push to the index but that's just wrong!", true);
        String value = "v".repeat(between(0, 256));
        initLookupIndex();
        test(b -> {
            b.startObject("test").field("type", "keyword").endObject();
            b.startObject("main_matching").field("type", "keyword").endObject();
        },
            b -> b.field("test", value).field("main_matching", "lookup"),
            """
                | DROP test
                | LOOKUP JOIN lookup ON matching == main_matching
                | EVAL test = LENGTH(test)
                """,
            matchesList().item(1), // <--- This is incorrectly returning value.length()
            matchesMap().entry("main_matching:column_at_a_time:BytesRefsFromOrds.Singleton", 1),
            // ^^^^ This is incorrectly returning test:column_at_a_time:Utf8CodePointsFromOrds.Singleton
            sig -> {}
        );
    }

    public void testVCosine() throws IOException {
        test(
            justType("dense_vector"),
            b -> b.startArray("test").value(128).value(128).value(0).endArray(),
            "| EVAL test = V_COSINE(test, [0, 255, 255])",
            matchesList().item(0.5),
            matchesMap().entry("test:column_at_a_time:FloatDenseVectorFromDocValues.Normalized.V_COSINE", 1)
        );
    }

    public void testVHammingToByte() throws IOException {
        test(
            b -> b.startObject("test").field("type", "dense_vector").field("element_type", "byte").endObject(),
            b -> b.startArray("test").value(100).value(100).value(0).endArray(),
            "| EVAL test = V_HAMMING(test, [0, 100, 100])",
            matchesList().item(6.0),
            matchesMap().entry("test:column_at_a_time:ByteDenseVectorFromDocValues.V_HAMMING", 1)
        );
    }

    public void testVHammingToBit() throws IOException {
        test(
            b -> b.startObject("test").field("type", "dense_vector").field("element_type", "bit").endObject(),
            b -> b.startArray("test").value(100).value(100).value(0).endArray(),
            "| EVAL test = V_HAMMING(test, [0, 100, 100])",
            matchesList().item(6.0),
            matchesMap().entry("test:column_at_a_time:BitDenseVectorFromDocValues.V_HAMMING", 1)
        );
    }

    private void test(
        CheckedConsumer<XContentBuilder, IOException> mapping,
        CheckedConsumer<XContentBuilder, IOException> doc,
        String eval,
        Matcher<?> expectedValue,
        MapMatcher expectedLoaders
    ) throws IOException {
        test(
            mapping,
            doc,
            eval,
            expectedValue,
            expectedLoaders,
            sig -> assertMap(
                sig,
                matchesList().item("LuceneSourceOperator")
                    .item("ValuesSourceReaderOperator") // the real work is here, checkOperatorProfile checks the status
                    .item("EvalOperator") // this one just renames the field
                    .item("AggregationOperator")
                    .item("ExchangeSinkOperator")
            )
        );
    }

    private void test(
        CheckedConsumer<XContentBuilder, IOException> mapping,
        CheckedConsumer<XContentBuilder, IOException> doc,
        String eval,
        Matcher<?> expectedValue,
        MapMatcher expectedLoaders,
        Consumer<List<String>> assertDataNodeSig
    ) throws IOException {
        indexValue(mapping, doc);
        RestEsqlTestCase.RequestObjectBuilder builder = requestObjectBuilder().query("""
            FROM test
            """ + eval + """
            | STATS test = VALUES(test)
            """);
        /*
         * TODO if you just do KEEP test then the load is in the data node reduce driver and not merged:
         *  \_ProjectExec[[test{f}#7]]
         *    \_FieldExtractExec[test{f}#7]<[],[]>
         *      \_EsQueryExec[test], indexMode[standard]]
         *  \_ExchangeSourceExec[[test{f}#7],false]}, {cluster_name=test-cluster, node_name=test-cluster-0, descrip
         *  \_ProjectExec[[test{r}#3]]
         *    \_EvalExec[[LENGTH(test{f}#7) AS test#3]]
         *      \_LimitExec[1000[INTEGER],50]
         *        \_ExchangeSourceExec[[test{f}#7],false]}], query={to
         */
        builder.profile(true);
        Map<String, Object> result = runEsql(builder, new AssertWarnings.NoWarnings(), profileLogger, RestEsqlTestCase.Mode.SYNC);

        assertResultMap(
            result,
            getResultMatcher(result).entry(
                "profile",
                matchesMap() //
                    .entry("drivers", instanceOf(List.class))
                    .entry("plans", instanceOf(List.class))
                    .entry("planning", matchesMap().extraOk())
                    .entry("query", matchesMap().extraOk())
            ),
            matchesList().item(matchesMap().entry("name", "test").entry("type", any(String.class))),
            matchesList().item(expectedValue)
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
                sig.add(checkOperatorProfile(o, expectedLoaders));
            }
            String description = p.get("description").toString();
            switch (description) {
                case "data" -> {
                    logger.info("data {}", sig);
                    assertDataNodeSig.accept(sig);
                }
                case "node_reduce" -> logger.info("node_reduce {}", sig);
                case "final" -> logger.info("final {}", sig);
                default -> throw new IllegalArgumentException("can't match " + description);
            }
        }
    }

    private void indexValue(CheckedConsumer<XContentBuilder, IOException> mapping, CheckedConsumer<XContentBuilder, IOException> doc)
        throws IOException {
        try {
            // Delete the index if it has already been created.
            client().performRequest(new Request("DELETE", "test"));
        } catch (ResponseException e) {
            if (e.getResponse().getStatusLine().getStatusCode() != 404) {
                throw e;
            }
        }

        Request createIndex = new Request("PUT", "test");
        try (XContentBuilder config = JsonXContent.contentBuilder()) {
            config.startObject();
            config.startObject("settings");
            {
                config.startObject("index");
                config.field("number_of_shards", 1);
                config.endObject();
            }
            config.endObject();
            config.startObject("mappings");
            {
                config.startObject("properties");
                mapping.accept(config);
                config.endObject();
            }
            config.endObject();

            createIndex.setJsonEntity(Strings.toString(config.endObject()));
        }
        Response createResponse = client().performRequest(createIndex);
        assertThat(
            entityToMap(createResponse.getEntity(), XContentType.JSON),
            matchesMap().entry("shards_acknowledged", true).entry("index", "test").entry("acknowledged", true)
        );

        Request bulk = new Request("POST", "/_bulk");
        bulk.addParameter("refresh", "");
        try (XContentBuilder docJson = JsonXContent.contentBuilder()) {
            docJson.startObject();
            doc.accept(docJson);
            docJson.endObject();
            bulk.setJsonEntity("""
                    {"create":{"_index":"test"}}
                """ + Strings.toString(docJson) + "\n");
        }
        Response bulkResponse = client().performRequest(bulk);
        assertThat(entityToMap(bulkResponse.getEntity(), XContentType.JSON), matchesMap().entry("errors", false).extraOk());
    }

    private void initLookupIndex() throws IOException {
        if (indexExists("lookup")) {
            return;
        }
        Request createIndex = new Request("PUT", "lookup");
        try (XContentBuilder config = JsonXContent.contentBuilder()) {
            config.startObject();
            config.startObject("settings");
            {
                config.startObject("index");
                config.field("number_of_shards", 1);
                config.field("mode", "lookup");
                config.endObject();
            }
            config.endObject();
            config.startObject("mappings");
            {
                config.startObject("properties");
                config.startObject("matching").field("type", "keyword").endObject();
                config.startObject("test").field("type", "keyword").endObject();
                config.endObject();
            }
            config.endObject();

            createIndex.setJsonEntity(Strings.toString(config.endObject()));
        }
        Response createResponse = client().performRequest(createIndex);
        assertThat(
            entityToMap(createResponse.getEntity(), XContentType.JSON),
            matchesMap().entry("shards_acknowledged", true).entry("index", "lookup").entry("acknowledged", true)
        );

        Request bulk = new Request("POST", "/_bulk");
        bulk.addParameter("refresh", "");
        bulk.setJsonEntity("""
                {"create":{"_index":"lookup"}}
                {"test": "a", "matching": "lookup"}
            """);
        Response bulkResponse = client().performRequest(bulk);
        assertThat(entityToMap(bulkResponse.getEntity(), XContentType.JSON), matchesMap().entry("errors", false).extraOk());
    }

    private CheckedConsumer<XContentBuilder, IOException> justType(String type) {
        return b -> b.startObject("test").field("type", type).endObject();
    }

    private static String checkOperatorProfile(Map<String, Object> o, MapMatcher expectedLoaders) {
        String name = (String) o.get("operator");
        name = PushQueriesIT.TO_NAME.matcher(name).replaceAll("");
        if (name.equals("ValuesSourceReaderOperator")) {
            MapMatcher expectedOp = matchesMap().entry("operator", startsWith(name))
                .entry("status", matchesMap().entry("readers_built", expectedLoaders).extraOk());
            assertMap(o, expectedOp);
        }
        return name;
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        // Preserve the cluser to speed up the semantic_text tests
        return true;
    }

    private static boolean setupEmbeddings = false;

    private void setUpTextEmbeddingInferenceEndpoint() throws IOException {
        setupEmbeddings = true;
        Request request = new Request("PUT", "_inference/text_embedding/test");
        request.setJsonEntity("""
                  {
                   "service": "text_embedding_test_service",
                   "service_settings": {
                     "model": "my_model",
                     "api_key": "abc64",
                     "dimensions": 128
                   },
                   "task_settings": {
                   }
                 }
            """);
        adminClient().performRequest(request);
    }
}

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
import org.elasticsearch.xpack.esql.qa.rest.ProfileLogger;
import org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase;
import org.hamcrest.Matcher;
import org.junit.ClassRule;
import org.junit.Rule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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

    public void testLength() throws IOException {
        String value = "v".repeat(between(0, 256));
        test(
            justType("keyword"),
            b -> b.value(value),
            "LENGTH(test)",
            matchesList().item(value.length()),
            "Utf8CodePointsFromOrds.SingletonOrdinals"
        );
    }

    public void testVCosine() throws IOException {
        test(
            justType("dense_vector"),
            b -> b.startArray().value(128).value(128).value(0).endArray(),
            "V_COSINE(test, [0, 255, 255])",
            matchesList().item(0.5),
            "BlockDocValuesReader.FloatDenseVectorNormalizedValuesBlockReader"
        );
    }

    private void test(
        CheckedConsumer<XContentBuilder, IOException> mapping,
        CheckedConsumer<XContentBuilder, IOException> value,
        String functionInvocation,
        Matcher<?> expectedValue,
        String expectedLoader
    ) throws IOException {
        indexValue(mapping, value);
        RestEsqlTestCase.RequestObjectBuilder builder = requestObjectBuilder().query("""
            FROM test
            | EVAL test =""" + " " + functionInvocation + """
            | STATS test = VALUES(test)""");
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
                sig.add(checkOperatorProfile(o, expectedLoader));
            }
            String description = p.get("description").toString();
            switch (description) {
                case "data" -> assertMap(
                    sig,
                    matchesList().item("LuceneSourceOperator")
                        .item("ValuesSourceReaderOperator") // the real work is here, checkOperatorProfile checks the status
                        .item("EvalOperator") // this one just renames the field
                        .item("AggregationOperator")
                        .item("ExchangeSinkOperator")
                );
                case "node_reduce" -> logger.info("node_reduce {}", sig);
                case "final" -> logger.info("final {}", sig);
                default -> throw new IllegalArgumentException("can't match " + description);
            }
        }
    }

    private void indexValue(CheckedConsumer<XContentBuilder, IOException> mapping, CheckedConsumer<XContentBuilder, IOException> value)
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
                config.startObject("properties").startObject("test");
                mapping.accept(config);
                config.endObject().endObject();
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
        try (XContentBuilder doc = JsonXContent.contentBuilder()) {
            doc.startObject();
            doc.field("test");
            value.accept(doc);
            doc.endObject();
            bulk.setJsonEntity("""
                    {"create":{"_index":"test"}}
                """ + Strings.toString(doc) + "\n");
        }
        Response bulkResponse = client().performRequest(bulk);
        assertThat(entityToMap(bulkResponse.getEntity(), XContentType.JSON), matchesMap().entry("errors", false).extraOk());
    }

    private CheckedConsumer<XContentBuilder, IOException> justType(String type) {
        return b -> b.field("type", type);
    }

    private static String checkOperatorProfile(Map<String, Object> o, String expectedLoader) {
        String name = (String) o.get("operator");
        name = PushQueriesIT.TO_NAME.matcher(name).replaceAll("");
        if (name.equals("ValuesSourceReaderOperator")) {
            MapMatcher expectedOp = matchesMap().entry("operator", startsWith(name))
                .entry(
                    "status",
                    matchesMap().entry("readers_built", matchesMap().entry("test:column_at_a_time:" + expectedLoader, 1)).extraOk()
                );
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

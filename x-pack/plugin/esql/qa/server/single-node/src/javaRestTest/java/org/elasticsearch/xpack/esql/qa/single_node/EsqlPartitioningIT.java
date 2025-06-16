/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.single_node;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.hamcrest.Matcher;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class EsqlPartitioningIT extends ESRestTestCase {
    /**
     * The size of the large index we use to test partitioning. It has to be
     * at least 250,000 entries for AUTO partitioning to do interesting things.
     */
    private static final int IDX_DOCS = 300_000;
    /**
     * The size of the small index we use to test partitioning. It's small enough
     * that AUTO should always choose SHARD partitioning.
     */
    private static final int SMALL_IDX_DOCS = 20_000;

    private static final long BASE_TIME = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2025-01-01T00:00:00Z");
    private static final int BULK_CHARS = Math.toIntExact(ByteSizeValue.ofMb(1).getBytes());

    record Case(String suffix, String idxPartition) {}

    @ParametersFactory(argumentFormatting = "[%1$s] %3$s -> %4$s")
    public static Iterable<Object[]> parameters() {
        List<Object[]> params = new ArrayList<>();
        for (String defaultDataPartitioning : new String[] { null, "shard", "segment", "doc", "auto" }) {
            for (String index : new String[] { "idx", "small_idx" }) {
                for (Case c : new Case[] {
                    new Case("", "SHARD"),
                    new Case("| SORT @timestamp ASC", "SHARD"),
                    new Case("| WHERE ABS(a) == 1", "DOC"),
                    new Case("| WHERE a == 1", "SHARD"),
                    new Case("| STATS SUM(a)", "DOC"),
                    new Case("| MV_EXPAND a | STATS SUM(a)", "DOC"),
                    new Case("| WHERE a == 1 | STATS SUM(a)", "SEGMENT"),
                    new Case("| WHERE a == 1 | MV_EXPAND a | STATS SUM(a)", "SEGMENT"), }) {
                    params.add(
                        new Object[] {
                            defaultDataPartitioning,
                            index,
                            "FROM " + index + " " + c.suffix,
                            expectedPartition(defaultDataPartitioning, index, c.idxPartition) }
                    );
                }
            }
        }
        return params;
    }

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster();

    private final String defaultDataPartitioning;
    private final String index;
    private final String query;
    private final Matcher<String> expectedPartition;

    public EsqlPartitioningIT(String defaultDataPartitioning, String index, String query, Matcher<String> expectedPartition) {
        this.defaultDataPartitioning = defaultDataPartitioning;
        this.index = index;
        this.query = query;
        this.expectedPartition = expectedPartition;
    }

    public void test() throws IOException {
        setupIndex(index, switch (index) {
            case "idx" -> IDX_DOCS;
            case "small_idx" -> SMALL_IDX_DOCS;
            default -> throw new IllegalArgumentException("unknown index [" + index + "]");
        });
        setDefaultDataPartitioning(defaultDataPartitioning);
        try {
            assertThat(partitionForQuery(query), expectedPartition);
        } finally {
            setDefaultDataPartitioning(null);
        }
    }

    private void setDefaultDataPartitioning(String defaultDataPartitioning) throws IOException {
        Request request = new Request("PUT", "/_cluster/settings");
        XContentBuilder builder = JsonXContent.contentBuilder().startObject();
        builder.startObject("transient");
        builder.field("esql.default_data_partitioning", defaultDataPartitioning);
        builder.endObject();
        request.setJsonEntity(Strings.toString(builder.endObject()));
        int code = client().performRequest(request).getStatusLine().getStatusCode();
        assertThat(code, equalTo(200));
    }

    private static Matcher<String> expectedPartition(String defaultDataPartitioning, String index, String idxPartition) {
        if (defaultDataPartitioning == null || "auto".equals(defaultDataPartitioning)) {
            return expectedAutoPartition(index, idxPartition);
        } else {
            return equalTo(defaultDataPartitioning.toUpperCase(Locale.ROOT));
        }
    }

    private static Matcher<String> expectedAutoPartition(String index, String idxPartition) {
        return equalTo(switch (index) {
            case "idx" -> idxPartition;
            case "small_idx" -> "SHARD";
            default -> throw new UnsupportedOperationException("unknown index [" + index + "]");
        });
    }

    private String partitionForQuery(String query) throws IOException {
        Request request = new Request("POST", "_query");
        request.addParameter("pretty", "");
        request.addParameter("error_trace", "");
        XContentBuilder b = JsonXContent.contentBuilder().startObject();
        b.field("query", query);
        b.field("profile", true);
        request.setJsonEntity(Strings.toString(b.endObject()));
        request.setOptions(request.getOptions().toBuilder().setWarningsHandler(w -> {
            w.remove("No limit defined, adding default limit of [1000]");
            return w.isEmpty() == false;
        }));
        String response = EntityUtils.toString(client().performRequest(request).getEntity());
        logger.info("Response: {}", response);
        Map<?, ?> profile = (Map<?, ?>) XContentHelper.convertToMap(JsonXContent.jsonXContent, response, true).get("profile");
        List<?> drivers = (List<?>) profile.get("drivers");
        for (Object dItem : drivers) {
            Map<?, ?> d = (Map<?, ?>) dItem;
            List<?> operators = (List<?>) d.get("operators");
            for (Object oItem : operators) {
                Map<?, ?> o = (Map<?, ?>) oItem;
                String operator = o.get("operator").toString();
                if (false == operator.startsWith("Lucene")) {
                    continue;
                }
                Map<?, ?> status = (Map<?, ?>) o.get("status");
                Map<?, ?> partitioningStrategies = (Map<?, ?>) status.get("partitioning_strategies");
                String strat = (String) partitioningStrategies.get(index + ":0");
                if (strat == null) {
                    throw new IllegalArgumentException("didn't find partition strategy for: " + o);
                }
                return strat;
            }
        }
        throw new IllegalArgumentException("didn't find partition strategy for: " + drivers);
    }

    private void setupIndex(String index, int docs) throws IOException {
        Response exists = client().performRequest(new Request("HEAD", "/" + index));
        if (exists.getStatusLine().getStatusCode() == 200) {
            return;
        }
        Request create = new Request("PUT", index);
        create.setJsonEntity("""
            {
              "settings": {
                "index": {
                  "number_of_shards": 1
                }
              }
            }""");  // Use a single shard to get consistent results.
        client().performRequest(create);
        StringBuilder bulk = new StringBuilder();
        for (int d = 0; d < docs; d++) {
            bulk.append("{\"index\":{}}\n");
            bulk.append("{\"@timestamp\": \"");
            bulk.append(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(BASE_TIME + d));
            bulk.append("\", \"a\": ");
            bulk.append(d % 10);
            bulk.append("}\n");
            if (bulk.length() > BULK_CHARS) {
                logger.info("indexing {}: {}", index, d);
                bulk(index, bulk.toString());
                bulk.setLength(0);
            }
        }
        logger.info("indexing {}: {}", index, docs);
        bulk(index, bulk.toString());
        client().performRequest(new Request("POST", "/" + index + "/_refresh"));
    }

    private void bulk(String index, String bulk) throws IOException {
        Request request = new Request("POST", index + "/_bulk");
        request.setJsonEntity(bulk);
        Response response = client().performRequest(request);
        if (response.getStatusLine().getStatusCode() != 200) {
            throw new AssertionError("error with bulk: " + EntityUtils.toString(response.getEntity()));
        }
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }
}

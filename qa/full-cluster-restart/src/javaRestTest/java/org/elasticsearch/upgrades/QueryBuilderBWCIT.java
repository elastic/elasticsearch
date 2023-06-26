/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.upgrades;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.ConstantScoreQueryBuilder;
import org.elasticsearch.index.query.DisMaxQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.SpanNearQueryBuilder;
import org.elasticsearch.index.query.SpanTermQueryBuilder;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.RandomScoreFunctionBuilder;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.LocalClusterConfigProvider;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.ClassRule;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

/**
 * An integration test that tests whether percolator queries stored in older supported ES version can still be read by the
 * current ES version. Percolator queries are stored in the binary format in a dedicated doc values field (see
 * PercolatorFieldMapper#createQueryBuilderField(...) method). Using the query builders writable contract. This test
 * does best effort verifying that we don't break bwc for query builders between the first previous major version and
 * the latest current major release.
 *
 * The queries to test are specified in json format, which turns out to work because we tend break here rarely. If the
 * json format of a query being tested here then feel free to change this.
 */
public class QueryBuilderBWCIT extends ParameterizedFullClusterRestartTestCase {
    private static final List<Object[]> CANDIDATES = new ArrayList<>();

    protected static LocalClusterConfigProvider clusterConfig = c -> {};

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .version(getOldClusterTestVersion())
        .nodes(2)
        .setting("xpack.security.enabled", "false")
        .apply(() -> clusterConfig)
        .build();

    @Override
    protected ElasticsearchCluster getUpgradeCluster() {
        return cluster;
    }

    public QueryBuilderBWCIT(@Name("cluster") FullClusterRestartUpgradeStatus upgradeStatus) {
        super(upgradeStatus);
    }

    static {
        addCandidate("\"match\": { \"text_field\": \"value\"}", new MatchQueryBuilder("text_field", "value"));
        addCandidate(
            "\"match\": { \"text_field\": {\"query\": \"value\", \"operator\": \"and\"} }",
            new MatchQueryBuilder("text_field", "value").operator(Operator.AND)
        );
        addCandidate(
            "\"match\": { \"text_field\": {\"query\": \"value\", \"analyzer\": \"english\"} }",
            new MatchQueryBuilder("text_field", "value").analyzer("english")
        );
        addCandidate(
            "\"match\": { \"text_field\": {\"query\": \"value\", \"minimum_should_match\": 3} }",
            new MatchQueryBuilder("text_field", "value").minimumShouldMatch("3")
        );
        addCandidate(
            "\"match\": { \"text_field\": {\"query\": \"value\", \"fuzziness\": \"auto\"} }",
            new MatchQueryBuilder("text_field", "value").fuzziness(Fuzziness.AUTO)
        );
        addCandidate("\"match_phrase\": { \"text_field\": \"value\"}", new MatchPhraseQueryBuilder("text_field", "value"));
        addCandidate(
            "\"match_phrase\": { \"text_field\": {\"query\": \"value\", \"slop\": 3}}",
            new MatchPhraseQueryBuilder("text_field", "value").slop(3)
        );
        addCandidate("\"range\": { \"long_field\": {\"gte\": 1, \"lte\": 9}}", new RangeQueryBuilder("long_field").from(1).to(9));
        addCandidate(
            "\"bool\": { \"must_not\": [{\"match_all\": {}}], \"must\": [{\"match_all\": {}}], "
                + "\"filter\": [{\"match_all\": {}}], \"should\": [{\"match_all\": {}}]}",
            new BoolQueryBuilder().mustNot(new MatchAllQueryBuilder())
                .must(new MatchAllQueryBuilder())
                .filter(new MatchAllQueryBuilder())
                .should(new MatchAllQueryBuilder())
        );
        addCandidate(
            "\"dis_max\": {\"queries\": [{\"match_all\": {}},{\"match_all\": {}},{\"match_all\": {}}], \"tie_breaker\": 0.01}",
            new DisMaxQueryBuilder().add(new MatchAllQueryBuilder())
                .add(new MatchAllQueryBuilder())
                .add(new MatchAllQueryBuilder())
                .tieBreaker(0.01f)
        );
        addCandidate(
            "\"constant_score\": {\"filter\": {\"match_all\": {}}, \"boost\": 0.1}",
            new ConstantScoreQueryBuilder(new MatchAllQueryBuilder()).boost(0.1f)
        );
        addCandidate(
            "\"function_score\": {\"query\": {\"match_all\": {}},"
                + "\"functions\": [{\"random_score\": {}, \"filter\": {\"match_all\": {}}, \"weight\": 0.2}]}",
            new FunctionScoreQueryBuilder(
                new MatchAllQueryBuilder(),
                new FunctionScoreQueryBuilder.FilterFunctionBuilder[] {
                    new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                        new MatchAllQueryBuilder(),
                        new RandomScoreFunctionBuilder().setWeight(0.2f)
                    ) }
            )
        );
        addCandidate(
            "\"span_near\": {\"clauses\": [{ \"span_term\": { \"keyword_field\": \"value1\" }}, "
                + "{ \"span_term\": { \"keyword_field\": \"value2\" }}]}",
            new SpanNearQueryBuilder(new SpanTermQueryBuilder("keyword_field", "value1"), 0).addClause(
                new SpanTermQueryBuilder("keyword_field", "value2")
            )
        );
        addCandidate(
            "\"span_near\": {\"clauses\": [{ \"span_term\": { \"keyword_field\": \"value1\" }}, "
                + "{ \"span_term\": { \"keyword_field\": \"value2\" }}], \"slop\": 2}",
            new SpanNearQueryBuilder(new SpanTermQueryBuilder("keyword_field", "value1"), 2).addClause(
                new SpanTermQueryBuilder("keyword_field", "value2")
            )
        );
        addCandidate(
            "\"span_near\": {\"clauses\": [{ \"span_term\": { \"keyword_field\": \"value1\" }}, "
                + "{ \"span_term\": { \"keyword_field\": \"value2\" }}], \"slop\": 2, \"in_order\": false}",
            new SpanNearQueryBuilder(new SpanTermQueryBuilder("keyword_field", "value1"), 2).addClause(
                new SpanTermQueryBuilder("keyword_field", "value2")
            ).inOrder(false)
        );
    }

    private static void addCandidate(String querySource, QueryBuilder expectedQb) {
        CANDIDATES.add(new Object[] { "{\"query\": {" + querySource + "}}", expectedQb });
    }

    public void testQueryBuilderBWC() throws Exception {
        final String type = getOldClusterVersion().before(Version.V_7_0_0) ? "doc" : "_doc";
        String index = "queries";
        if (isRunningAgainstOldCluster()) {
            XContentBuilder mappingsAndSettings = jsonBuilder();
            mappingsAndSettings.startObject();
            {
                mappingsAndSettings.startObject("settings");
                mappingsAndSettings.field("number_of_shards", 1);
                mappingsAndSettings.field("number_of_replicas", 0);
                mappingsAndSettings.endObject();
            }
            {
                mappingsAndSettings.startObject("mappings");
                if (isRunningAgainstAncientCluster()) {
                    mappingsAndSettings.startObject(type);
                }
                mappingsAndSettings.startObject("properties");
                {
                    mappingsAndSettings.startObject("query");
                    mappingsAndSettings.field("type", "percolator");
                    mappingsAndSettings.endObject();
                }
                {
                    mappingsAndSettings.startObject("keyword_field");
                    mappingsAndSettings.field("type", "keyword");
                    mappingsAndSettings.endObject();
                }
                {
                    mappingsAndSettings.startObject("text_field");
                    mappingsAndSettings.field("type", "text");
                    mappingsAndSettings.endObject();
                }
                {
                    mappingsAndSettings.startObject("long_field");
                    mappingsAndSettings.field("type", "long");
                    mappingsAndSettings.endObject();
                }
                mappingsAndSettings.endObject();
                mappingsAndSettings.endObject();
                if (isRunningAgainstAncientCluster()) {
                    mappingsAndSettings.endObject();
                }
            }
            mappingsAndSettings.endObject();
            Request request = new Request("PUT", "/" + index);
            request.setOptions(allowTypesRemovalWarnings());
            request.setJsonEntity(Strings.toString(mappingsAndSettings));
            Response rsp = client().performRequest(request);
            assertEquals(200, rsp.getStatusLine().getStatusCode());

            for (int i = 0; i < CANDIDATES.size(); i++) {
                request = new Request("PUT", "/" + index + "/" + type + "/" + Integer.toString(i));
                request.setJsonEntity((String) CANDIDATES.get(i)[0]);
                rsp = client().performRequest(request);
                assertEquals(201, rsp.getStatusLine().getStatusCode());
            }
        } else {
            NamedWriteableRegistry registry = new NamedWriteableRegistry(
                new SearchModule(Settings.EMPTY, false, Collections.emptyList()).getNamedWriteables()
            );

            for (int i = 0; i < CANDIDATES.size(); i++) {
                QueryBuilder expectedQueryBuilder = (QueryBuilder) CANDIDATES.get(i)[1];
                Request request = new Request("GET", "/" + index + "/_search");
                request.setJsonEntity(
                    "{\"query\": {\"ids\": {\"values\": [\""
                        + Integer.toString(i)
                        + "\"]}}, "
                        + "\"docvalue_fields\": [{\"field\":\"query.query_builder_field\"}]}"
                );
                Response rsp = client().performRequest(request);
                assertEquals(200, rsp.getStatusLine().getStatusCode());
                Map<?, ?> hitRsp = (Map<?, ?>) ((List<?>) ((Map<?, ?>) toMap(rsp).get("hits")).get("hits")).get(0);
                String queryBuilderStr = (String) ((List<?>) ((Map<?, ?>) hitRsp.get("fields")).get("query.query_builder_field")).get(0);
                byte[] qbSource = Base64.getDecoder().decode(queryBuilderStr);
                try (InputStream in = new ByteArrayInputStream(qbSource, 0, qbSource.length)) {
                    try (StreamInput input = new NamedWriteableAwareStreamInput(new InputStreamStreamInput(in), registry)) {
                        input.setVersion(getOldClusterVersion());
                        QueryBuilder queryBuilder = input.readNamedWriteable(QueryBuilder.class);
                        assert in.read() == -1;
                        assertEquals(expectedQueryBuilder, queryBuilder);
                    }
                }
            }
        }
    }

    private static Map<String, Object> toMap(Response response) throws IOException {
        return toMap(EntityUtils.toString(response.getEntity()));
    }

    private static Map<String, Object> toMap(String response) throws IOException {
        return XContentHelper.convertToMap(JsonXContent.jsonXContent, response, false);
    }
}

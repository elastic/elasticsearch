/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.upgrades;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

/**
 * Lucene 10.6 changed the output of Thai analysis. Indices created before {@link IndexVersions#UPGRADE_TO_LUCENE_10_6_0} must keep
 * analyzing exactly as they did before the upgrade, at both index and query time and for documents indexed after the upgrade, while
 * indices created on the upgraded cluster get Lucene's current output. Covers every Thai entry point: the prebuilt {@code thai}
 * analyzer, a {@code type: thai} analyzer, the preconfigured {@code thai} tokenizer, a {@code type: thai} tokenizer and
 * {@code stopwords: _thai_}.
 * <p>
 * The legacy expectations only apply when the old cluster created the index below the gate; if it did not (for example when the
 * "old" cluster is itself a build that already has the gate), the old index is a current-behaviour index on both sides. Either way
 * the old index's output must be identical before and after the upgrade.
 */
public class ThaiAnalysisFullClusterRestartIT extends ParameterizedFullClusterRestartTestCase {

    private static final String OLD_INDEX = "thai_analysis_old";
    private static final String RECORDED_INDEX = "thai_analysis_recorded";
    private static final String NEW_INDEX = "thai_analysis_new";

    /** 1021 {@code ก}, a space, then {@code แมว} straddling the tokenizer's 1024 char buffer. */
    private static final String LONG_TEXT = "ก".repeat(1021) + " แมว";

    /** Each entry is field, text, Lucene 10.5 tokens, current tokens. */
    private static final List<Fixture> FIXTURES = List.of(
        new Fixture("prebuilt", "เเมว", List.of("เเมว"), List.of("แมว")),
        new Fixture("prebuilt", "เร็วๆ", List.of("เร็ว", "ๆ"), List.of("เร็ว", "เร็ว")),
        new Fixture("prebuilt", "ไป", List.of(), List.of("ไป")),
        new Fixture("prebuilt", "ที่ผ่านมา", List.of(), List.of("ผ่าน", "มา")),
        new Fixture("typed", "เเมว", List.of("เเมว"), List.of("แมว")),
        new Fixture("typed", "เร็วๆ", List.of("เร็ว", "ๆ"), List.of("เร็ว", "เร็ว")),
        new Fixture("typed", "ที่ผ่านมา", List.of(), List.of("ผ่าน", "มา")),
        new Fixture("stop", "ไป", List.of(), List.of("ไป")),
        new Fixture("stop", "ที่ผ่านมา", List.of(), List.of("ผ่าน", "มา")),
        new Fixture("preconfigured_tokenizer", LONG_TEXT, longTextTokens("แม", "ว"), longTextTokens("แมว")),
        new Fixture("typed_tokenizer", LONG_TEXT, longTextTokens("แม", "ว"), longTextTokens("แมว"))
    );

    @ClassRule
    public static final ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .version(OLD_CLUSTER_VERSION, isOldClusterDetachedVersion())
        .setting("xpack.security.enabled", "false")
        .build();

    public ThaiAnalysisFullClusterRestartIT(@Name("cluster") FullClusterRestartUpgradeStatus upgradeStatus) {
        super(upgradeStatus);
    }

    @Override
    protected ElasticsearchCluster getUpgradeCluster() {
        return cluster;
    }

    public void testThaiAnalysisFollowsIndexCreatedVersion() throws IOException {
        if (isRunningAgainstOldCluster()) {
            createThaiIndex(OLD_INDEX);
            indexDoc(OLD_INDEX, "1");
            boolean legacy = isLegacy(OLD_INDEX);
            for (Fixture fixture : FIXTURES) {
                List<String> tokens = analyze(OLD_INDEX, fixture.field, fixture.text);
                assertThat(fixture.toString(), tokens, equalTo(fixture.expected(legacy)));
                record(fixture, tokens);
            }
            assertIndexedTerms(OLD_INDEX, legacy, 1);
            assertMatchPhrase(OLD_INDEX, legacy, 1);
        } else {
            ensureGreen(OLD_INDEX);
            boolean legacy = isLegacy(OLD_INDEX);
            // Byte-identical output to what the old cluster produced for the same index, and the expected side of the gate.
            for (Fixture fixture : FIXTURES) {
                List<String> tokens = analyze(OLD_INDEX, fixture.field, fixture.text);
                assertThat(fixture.toString(), tokens, equalTo(recorded(fixture)));
                assertThat(fixture.toString(), tokens, equalTo(fixture.expected(legacy)));
            }
            // Documents indexed before the upgrade are still found by queries analyzed on the upgraded cluster.
            assertIndexedTerms(OLD_INDEX, legacy, 1);
            assertMatchPhrase(OLD_INDEX, legacy, 1);

            // A document indexed into the old index after the upgrade is analyzed the same way as the ones before it.
            indexDoc(OLD_INDEX, "2");
            assertIndexedTerms(OLD_INDEX, legacy, 2);
            assertMatchPhrase(OLD_INDEX, legacy, 2);

            // A fresh index on the upgraded cluster gets the current behaviour.
            createThaiIndex(NEW_INDEX);
            indexDoc(NEW_INDEX, "1");
            assertFalse("index created on the upgraded cluster must be past the gate", isLegacy(NEW_INDEX));
            for (Fixture fixture : FIXTURES) {
                assertThat(fixture.toString(), analyze(NEW_INDEX, fixture.field, fixture.text), equalTo(fixture.current));
            }
            assertIndexedTerms(NEW_INDEX, false, 1);
            assertMatchPhrase(NEW_INDEX, false, 1);
        }
    }

    private record Fixture(String field, String text, List<String> legacy, List<String> current) {
        List<String> expected(boolean isLegacy) {
            return isLegacy ? legacy : current;
        }

        String id() {
            return field + "_" + Integer.toHexString(text.hashCode());
        }
    }

    private static List<String> longTextTokens(String... tail) {
        List<String> tokens = new ArrayList<>(Collections.nCopies(510, "กก"));
        tokens.add("ก");
        tokens.addAll(Arrays.asList(tail));
        return List.copyOf(tokens);
    }

    private static boolean isLegacy(String index) throws IOException {
        Response response = client().performRequest(new Request("GET", "/" + index + "/_settings"));
        String created = ObjectPath.createFromResponse(response).evaluate(index + ".settings.index.version.created");
        return IndexVersion.fromId(Integer.parseInt(created)).before(IndexVersions.UPGRADE_TO_LUCENE_10_6_0);
    }

    private static void createThaiIndex(String index) throws IOException {
        XContentBuilder body = JsonXContent.contentBuilder().startObject();
        body.startObject("settings");
        {
            body.field("number_of_shards", 1).field("number_of_replicas", 0);
            body.startObject("analysis");
            body.startObject("analyzer");
            body.startObject("my_thai").field("type", "thai").endObject();
            body.startObject("thai_preconfigured_tokenizer").field("tokenizer", "thai").endObject();
            body.startObject("thai_typed_tokenizer").field("tokenizer", "my_thai_tokenizer").endObject();
            body.startObject("thai_stop").field("tokenizer", "thai").array("filter", "my_thai_stop").endObject();
            body.endObject();
            body.startObject("tokenizer").startObject("my_thai_tokenizer").field("type", "thai").endObject().endObject();
            body.startObject("filter")
                .startObject("my_thai_stop")
                .field("type", "stop")
                .field("stopwords", "_thai_")
                .endObject()
                .endObject();
            body.endObject();
        }
        body.endObject();
        body.startObject("mappings").startObject("properties");
        {
            textField(body, "prebuilt", "thai");
            textField(body, "typed", "my_thai");
            textField(body, "preconfigured_tokenizer", "thai_preconfigured_tokenizer");
            textField(body, "typed_tokenizer", "thai_typed_tokenizer");
            textField(body, "stop", "thai_stop");
        }
        body.endObject().endObject();
        body.endObject();
        Request request = new Request("PUT", "/" + index);
        request.setJsonEntity(Strings.toString(body));
        client().performRequest(request);
    }

    private static void textField(XContentBuilder body, String name, String analyzer) throws IOException {
        body.startObject(name).field("type", "text").field("analyzer", analyzer).endObject();
    }

    private static void indexDoc(String index, String id) throws IOException {
        XContentBuilder doc = JsonXContent.contentBuilder()
            .startObject()
            .field("prebuilt", "เเมว เร็วๆ ที่ผ่านมา")
            .field("typed", "เเมว เร็วๆ ที่ผ่านมา")
            .field("preconfigured_tokenizer", LONG_TEXT)
            .field("typed_tokenizer", LONG_TEXT)
            .field("stop", "ที่ผ่านมา")
            .endObject();
        Request request = new Request("PUT", "/" + index + "/_doc/" + id);
        request.addParameter("refresh", "true");
        request.setJsonEntity(Strings.toString(doc));
        client().performRequest(request);
    }

    private static List<String> analyze(String index, String field, String text) throws IOException {
        Request request = new Request("POST", "/" + index + "/_analyze");
        request.setJsonEntity(
            Strings.toString(JsonXContent.contentBuilder().startObject().field("field", field).field("text", text).endObject())
        );
        List<Map<String, Object>> tokens = ObjectPath.createFromResponse(client().performRequest(request)).evaluate("tokens");
        return tokens.stream().map(t -> (String) t.get("token")).toList();
    }

    /** Stores the old cluster's output so the upgraded cluster can be checked against what was actually produced. */
    private static void record(Fixture fixture, List<String> tokens) throws IOException {
        Request request = new Request("PUT", "/" + RECORDED_INDEX + "/_doc/" + fixture.id());
        request.addParameter("refresh", "true");
        request.setJsonEntity(Strings.toString(JsonXContent.contentBuilder().startObject().field("tokens", tokens).endObject()));
        client().performRequest(request);
    }

    private static List<String> recorded(Fixture fixture) throws IOException {
        Response response = client().performRequest(new Request("GET", "/" + RECORDED_INDEX + "/_doc/" + fixture.id()));
        return ObjectPath.createFromResponse(response).evaluate("_source.tokens");
    }

    /** Term queries are not analyzed, so they check exactly which terms each document was indexed with. */
    private static void assertIndexedTerms(String index, boolean legacy, int docs) throws IOException {
        assertHits(index, "term", "prebuilt", legacy ? "เเมว" : "แมว", docs);
        assertHits(index, "term", "prebuilt", legacy ? "แมว" : "เเมว", 0);
        assertHits(index, "term", "typed", legacy ? "ๆ" : "เร็ว", docs);
        assertHits(index, "term", "stop", "ผ่าน", legacy ? 0 : docs);
        assertHits(index, "term", "preconfigured_tokenizer", legacy ? "แม" : "แมว", docs);
        assertHits(index, "term", "typed_tokenizer", legacy ? "แม" : "แมว", docs);
    }

    /** Phrase queries are analyzed with the index's search analyzer, so they only match if query and index time agree. */
    private static void assertMatchPhrase(String index, boolean legacy, int docs) throws IOException {
        assertHits(index, "match_phrase", "prebuilt", "เเมว", docs);
        assertHits(index, "match_phrase", "typed", "เร็วๆ", docs);
        assertHits(index, "match_phrase", "stop", "ที่ผ่านมา", legacy ? 0 : docs);
    }

    private static void assertHits(String index, String queryType, String field, String value, int expected) throws IOException {
        Request request = new Request("POST", "/" + index + "/_search");
        request.addParameter("rest_total_hits_as_int", "true");
        request.setJsonEntity(
            Strings.toString(
                JsonXContent.contentBuilder()
                    .startObject()
                    .startObject("query")
                    .startObject(queryType)
                    .field(field, value)
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );
        Response response = client().performRequest(request);
        assertThat(
            queryType + " " + field + ":" + value + " on " + index,
            ObjectPath.createFromResponse(response).evaluate("hits.total"),
            equalTo(expected)
        );
    }
}

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
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

/**
 * Lucene 10.6 changed the output of Thai analysis; indices created before {@link IndexVersions#THAI_ANALYSIS_LUCENE_10_6} must keep
 * the old output. In a mixed cluster every copy of a shard analyzes documents and queries itself, so old and new nodes must agree on
 * how an old index analyzes Thai text or the copies diverge. The old index has a copy on every node, and each phase indexes a document
 * and then queries each node's copy separately ({@code preference=_only_nodes}) to check that every copy indexed every document,
 * whichever node analyzed it, with the same terms, and that query analysis on every node matches them.
 * <p>
 * The legacy expectations only apply when the old cluster created the index below the gate; otherwise the old index is a
 * current-behaviour index throughout.
 */
public class ThaiAnalysisRollingUpgradeIT extends AbstractRollingUpgradeTestCase {

    private static final String OLD_INDEX = "thai_analysis_rolling_old";
    private static final String NEW_INDEX = "thai_analysis_rolling_new";

    /** 1021 {@code ก}, a space, then {@code แมว} straddling the tokenizer's 1024 char buffer. */
    private static final String LONG_TEXT = "ก".repeat(1021) + " แมว";

    private final int upgradedNodes;

    public ThaiAnalysisRollingUpgradeIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
        this.upgradedNodes = upgradedNodes;
    }

    public void testThaiAnalysisFollowsIndexCreatedVersion() throws IOException {
        if (isOldCluster()) {
            createThaiIndex(OLD_INDEX, NODE_NUM - 1);
        }
        ensureGreen(OLD_INDEX);
        boolean legacy = isLegacy(OLD_INDEX);
        // A new document per phase, so each phase's document is analyzed by whichever nodes now hold the copies.
        indexDoc(OLD_INDEX, Integer.toString(upgradedNodes));
        int docs = count(OLD_INDEX);
        assertAnalyze(OLD_INDEX, legacy);
        for (String nodeId : nodeIds()) {
            assertIndexedTerms(OLD_INDEX, nodeId, legacy, docs);
            assertMatchPhrase(OLD_INDEX, nodeId, legacy, docs);
        }

        if (isUpgradedCluster()) {
            createThaiIndex(NEW_INDEX, 0);
            indexDoc(NEW_INDEX, "1");
            assertFalse("index created on the upgraded cluster must be past the gate", isLegacy(NEW_INDEX));
            assertAnalyze(NEW_INDEX, false);
            assertIndexedTerms(NEW_INDEX, null, false, 1);
            assertMatchPhrase(NEW_INDEX, null, false, 1);
        }
    }

    private static List<String> longTextTokens(String... tail) {
        List<String> tokens = new ArrayList<>(Collections.nCopies(510, "กก"));
        tokens.add("ก");
        tokens.addAll(List.of(tail));
        return List.copyOf(tokens);
    }

    private static void assertAnalyze(String index, boolean legacy) throws IOException {
        assertThat(analyze(index, "prebuilt", "เเมว"), equalTo(legacy ? List.of("เเมว") : List.of("แมว")));
        assertThat(analyze(index, "prebuilt", "เร็วๆ"), equalTo(legacy ? List.of("เร็ว", "ๆ") : List.of("เร็ว", "เร็ว")));
        assertThat(analyze(index, "typed", "ไป"), equalTo(legacy ? List.of() : List.of("ไป")));
        assertThat(analyze(index, "stop", "ที่ผ่านมา"), equalTo(legacy ? List.of() : List.of("ผ่าน", "มา")));
        List<String> longTokens = legacy ? longTextTokens("แม", "ว") : longTextTokens("แมว");
        assertThat(analyze(index, "preconfigured_tokenizer", LONG_TEXT), equalTo(longTokens));
        assertThat(analyze(index, "typed_tokenizer", LONG_TEXT), equalTo(longTokens));
    }

    private static boolean isLegacy(String index) throws IOException {
        Response response = client().performRequest(new Request("GET", "/" + index + "/_settings"));
        String created = ObjectPath.createFromResponse(response).evaluate(index + ".settings.index.version.created");
        return IndexVersion.fromId(Integer.parseInt(created)).before(IndexVersions.THAI_ANALYSIS_LUCENE_10_6);
    }

    private static int count(String index) throws IOException {
        return ObjectPath.createFromResponse(client().performRequest(new Request("GET", "/" + index + "/_count"))).evaluate("count");
    }

    private static List<String> nodeIds() throws IOException {
        Map<String, Object> nodes = ObjectPath.createFromResponse(client().performRequest(new Request("GET", "/_nodes"))).evaluate("nodes");
        return List.copyOf(nodes.keySet());
    }

    private static void createThaiIndex(String index, int replicas) throws IOException {
        XContentBuilder body = JsonXContent.contentBuilder().startObject();
        body.startObject("settings");
        {
            body.field("number_of_shards", 1).field("number_of_replicas", replicas);
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

    /** Term queries are not analyzed, so they check exactly which terms each copy indexed. */
    private static void assertIndexedTerms(String index, String nodeId, boolean legacy, int docs) throws IOException {
        assertHits(index, nodeId, "term", "prebuilt", legacy ? "เเมว" : "แมว", docs);
        assertHits(index, nodeId, "term", "prebuilt", legacy ? "แมว" : "เเมว", 0);
        assertHits(index, nodeId, "term", "typed", legacy ? "ๆ" : "เร็ว", docs);
        assertHits(index, nodeId, "term", "stop", "ผ่าน", legacy ? 0 : docs);
        assertHits(index, nodeId, "term", "preconfigured_tokenizer", legacy ? "แม" : "แมว", docs);
        assertHits(index, nodeId, "term", "typed_tokenizer", legacy ? "แม" : "แมว", docs);
    }

    /** Phrase queries are analyzed on the node serving the copy, so they only match if that node agrees with index time. */
    private static void assertMatchPhrase(String index, String nodeId, boolean legacy, int docs) throws IOException {
        assertHits(index, nodeId, "match_phrase", "prebuilt", "เเมว", docs);
        assertHits(index, nodeId, "match_phrase", "typed", "เร็วๆ", docs);
        assertHits(index, nodeId, "match_phrase", "stop", "ที่ผ่านมา", legacy ? 0 : docs);
    }

    private static void assertHits(String index, String nodeId, String queryType, String field, String value, int expected)
        throws IOException {
        Request request = new Request("POST", "/" + index + "/_search");
        request.addParameter("rest_total_hits_as_int", "true");
        if (nodeId != null) {
            request.addParameter("preference", "_only_nodes:" + nodeId);
        }
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
        String description = queryType + " " + field + ":" + value + " on " + index + (nodeId == null ? "" : " node " + nodeId);
        assertThat(description, ObjectPath.createFromResponse(response).evaluate("hits.total"), equalTo(expected));
    }
}

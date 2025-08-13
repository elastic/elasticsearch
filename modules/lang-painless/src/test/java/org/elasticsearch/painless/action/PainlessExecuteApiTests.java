/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.painless.action;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.painless.PainlessPlugin;
import org.elasticsearch.painless.action.PainlessExecuteAction.Request;
import org.elasticsearch.painless.action.PainlessExecuteAction.Response;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.painless.action.PainlessExecuteAction.TransportAction.innerShardOperation;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class PainlessExecuteApiTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singleton(PainlessPlugin.class);
    }

    public void testDefaults() throws IOException {
        ScriptService scriptService = getInstanceFromNode(ScriptService.class);
        Request request = new Request(new Script("100.0 / 1000.0"), null, null);
        Response response = innerShardOperation(request, scriptService, null);
        assertThat(response.getResult(), equalTo("0.1"));

        Map<String, Object> params = new HashMap<>();
        params.put("count", 100.0D);
        params.put("total", 1000.0D);
        request = new Request(new Script(ScriptType.INLINE, "painless", "params.count / params.total", params), null, null);
        response = innerShardOperation(request, scriptService, null);
        assertThat(response.getResult(), equalTo("0.1"));

        Exception e = expectThrows(ScriptException.class, () -> {
            Request r = new Request(
                new Script(ScriptType.INLINE, "painless", "params.count / params.total + doc['constant']", params),
                null,
                null
            );
            innerShardOperation(r, scriptService, null);
        });
        assertThat(e.getCause().getMessage(), equalTo("cannot resolve symbol [doc]"));
    }

    public void testNestedDocs() throws IOException {
        ScriptService scriptService = getInstanceFromNode(ScriptService.class);
        IndexService indexService = createIndex("index", Settings.EMPTY, "doc", "rank", "type=long", "nested", "type=nested");

        Request.ContextSetup contextSetup = new Request.ContextSetup("index", new BytesArray("""
            {"rank": 4.0, "nested": [{"text": "foo"}, {"text": "bar"}]}"""), new MatchAllQueryBuilder());
        contextSetup.setXContentType(XContentType.JSON);
        Request request = new Request(new Script(ScriptType.INLINE, "painless", "doc['rank'].value", Map.of()), "score", contextSetup);
        Response response = innerShardOperation(request, scriptService, indexService);
        assertThat(response.getResult(), equalTo(4.0D));
    }

    public void testFilterExecutionContext() throws IOException {
        ScriptService scriptService = getInstanceFromNode(ScriptService.class);
        IndexService indexService = createIndex("index", Settings.EMPTY, "doc", "field", "type=long");

        Request.ContextSetup contextSetup = new Request.ContextSetup("index", new BytesArray("{\"field\": 3}"), null);
        contextSetup.setXContentType(XContentType.JSON);
        Request request = new Request(new Script("doc['field'].value >= 3"), "filter", contextSetup);
        Response response = innerShardOperation(request, scriptService, indexService);
        assertThat(response.getResult(), equalTo(true));

        contextSetup = new Request.ContextSetup("index", new BytesArray("{\"field\": 3}"), null);
        contextSetup.setXContentType(XContentType.JSON);
        request = new Request(
            new Script(ScriptType.INLINE, "painless", "doc['field'].value >= params.max", singletonMap("max", 3)),
            "filter",
            contextSetup
        );
        response = innerShardOperation(request, scriptService, indexService);
        assertThat(response.getResult(), equalTo(true));

        contextSetup = new Request.ContextSetup("index", new BytesArray("{\"field\": 2}"), null);
        contextSetup.setXContentType(XContentType.JSON);
        request = new Request(
            new Script(ScriptType.INLINE, "painless", "doc['field'].value >= params.max", singletonMap("max", 3)),
            "filter",
            contextSetup
        );
        response = innerShardOperation(request, scriptService, indexService);
        assertThat(response.getResult(), equalTo(false));
    }

    public void testScoreExecutionContext() throws IOException {
        ScriptService scriptService = getInstanceFromNode(ScriptService.class);
        IndexService indexService = createIndex("index", Settings.EMPTY, "doc", "rank", "type=long", "text", "type=text");

        Request.ContextSetup contextSetup = new Request.ContextSetup("index", new BytesArray("""
            {"rank": 4.0, "text": "quick brown fox"}"""), new MatchQueryBuilder("text", "fox"));
        contextSetup.setXContentType(XContentType.JSON);
        Request request = new Request(
            new Script(
                ScriptType.INLINE,
                "painless",
                "Math.round((_score + (doc['rank'].value / params.max_rank)) * 100.0) / 100.0",
                singletonMap("max_rank", 5.0)
            ),
            "score",
            contextSetup
        );
        Response response = innerShardOperation(request, scriptService, indexService);
        assertThat(response.getResult(), equalTo(0.93D));
    }

    public void testBooleanFieldExecutionContext() throws IOException {
        ScriptService scriptService = getInstanceFromNode(ScriptService.class);
        IndexService indexService = createIndex("index", Settings.EMPTY, "doc", "rank", "type=long", "text", "type=text");

        Request.ContextSetup contextSetup = new Request.ContextSetup("index", new BytesArray("""
            {"rank": 4.0, "text": "quick brown fox"}"""), new MatchQueryBuilder("text", "fox"));
        contextSetup.setXContentType(XContentType.JSON);
        Request request = new Request(
            new Script(ScriptType.INLINE, "painless", "emit(doc['rank'].value < params.max_rank)", singletonMap("max_rank", 5.0)),
            "boolean_field",
            contextSetup
        );
        Response response = innerShardOperation(request, scriptService, indexService);
        assertEquals(Collections.singletonList(true), response.getResult());

        contextSetup = new Request.ContextSetup("index", new BytesArray("{}"), new MatchAllQueryBuilder());
        contextSetup.setXContentType(XContentType.JSON);
        request = new Request(
            new Script(ScriptType.INLINE, "painless", "emit(false); emit(true); emit (false);", emptyMap()),
            "boolean_field",
            contextSetup
        );
        response = innerShardOperation(request, scriptService, indexService);
        assertEquals(Arrays.asList(false, false, true), response.getResult());
    }

    public void testDateFieldExecutionContext() throws IOException {
        ScriptService scriptService = getInstanceFromNode(ScriptService.class);
        IndexService indexService = createIndex("index", Settings.EMPTY, "doc", "test_date", "type=date");

        Request.ContextSetup contextSetup = new Request.ContextSetup(
            "index",
            new BytesArray("{\"test_date\":\"2015-01-01T12:10:30Z\"}"),
            new MatchAllQueryBuilder()
        );
        contextSetup.setXContentType(XContentType.JSON);
        Request request = new Request(
            new Script(ScriptType.INLINE, "painless", "emit(doc['test_date'].value.toInstant().toEpochMilli())", emptyMap()),
            "date_field",
            contextSetup
        );
        Response response = innerShardOperation(request, scriptService, indexService);
        assertEquals(Collections.singletonList("2015-01-01T12:10:30.000Z"), response.getResult());

        contextSetup = new Request.ContextSetup("index", new BytesArray("{}"), new MatchAllQueryBuilder());
        contextSetup.setXContentType(XContentType.JSON);
        request = new Request(new Script(ScriptType.INLINE, "painless", """
            emit(ZonedDateTime.parse("2021-01-01T00:00:00Z").toInstant().toEpochMilli());
            emit(ZonedDateTime.parse("1942-05-31T15:16:17Z").toInstant().toEpochMilli());
            emit(ZonedDateTime.parse("2035-10-13T10:54:19Z").toInstant().toEpochMilli());""", emptyMap()), "date_field", contextSetup);
        response = innerShardOperation(request, scriptService, indexService);
        assertEquals(
            Arrays.asList("2021-01-01T00:00:00.000Z", "1942-05-31T15:16:17.000Z", "2035-10-13T10:54:19.000Z"),
            response.getResult()
        );
    }

    @SuppressWarnings("unchecked")
    public void testDoubleFieldExecutionContext() throws IOException {
        ScriptService scriptService = getInstanceFromNode(ScriptService.class);
        IndexService indexService = createIndex("index", Settings.EMPTY, "doc", "rank", "type=long", "text", "type=text");

        Request.ContextSetup contextSetup = new Request.ContextSetup(
            "index",
            new BytesArray("{\"rank\": 4.0, \"text\": \"quick brown fox\"}"),
            new MatchQueryBuilder("text", "fox")
        );
        contextSetup.setXContentType(XContentType.JSON);
        Request request = new Request(
            new Script(ScriptType.INLINE, "painless", "emit(doc['rank'].value); emit(Math.log(doc['rank'].value))", emptyMap()),
            "double_field",
            contextSetup
        );
        Response response = innerShardOperation(request, scriptService, indexService);
        List<Double> doubles = (List<Double>) response.getResult();
        assertEquals(4.0, doubles.get(0), 0.00001);
        assertEquals(Math.log(4.0), doubles.get(1), 0.00001);

        contextSetup = new Request.ContextSetup("index", new BytesArray("{}"), new MatchAllQueryBuilder());
        contextSetup.setXContentType(XContentType.JSON);
        request = new Request(
            new Script(
                ScriptType.INLINE,
                "painless",
                "emit(3.1); emit(2.29); emit(-12.47); emit(-12.46); emit(Double.MAX_VALUE); emit(0.0);",
                emptyMap()
            ),
            "double_field",
            contextSetup
        );
        response = innerShardOperation(request, scriptService, indexService);
        doubles = (List<Double>) response.getResult();
        assertEquals(3.1, doubles.get(0), 0.00001);
        assertEquals(2.29, doubles.get(1), 0.00001);
        assertEquals(-12.47, doubles.get(2), 0.00001);
        assertEquals(-12.46, doubles.get(3), 0.00001);
        assertEquals(Double.MAX_VALUE, doubles.get(4), 0.00001);
        assertEquals(0.0, doubles.get(5), 0.00001);
    }

    @SuppressWarnings("unchecked")
    public void testGeoPointFieldExecutionContext() throws IOException {
        ScriptService scriptService = getInstanceFromNode(ScriptService.class);
        IndexService indexService = createIndex("index", Settings.EMPTY, "doc", "test_point", "type=geo_point");

        Request.ContextSetup contextSetup = new Request.ContextSetup(
            "index",
            new BytesArray("{\"test_point\":\"30.0,40.0\"}"),
            new MatchAllQueryBuilder()
        );
        contextSetup.setXContentType(XContentType.JSON);
        Request request = new Request(
            new Script(ScriptType.INLINE, "painless", "emit(doc['test_point'].value.lat, doc['test_point'].value.lon)", emptyMap()),
            "geo_point_field",
            contextSetup
        );
        Response response = innerShardOperation(request, scriptService, indexService);
        List<Map<String, Object>> points = (List<Map<String, Object>>) response.getResult();
        assertEquals(40.0, (double) ((List<Object>) points.get(0).get("coordinates")).get(0), 0.00001);
        assertEquals(30.0, (double) ((List<Object>) points.get(0).get("coordinates")).get(1), 0.00001);
        assertEquals("Point", points.get(0).get("type"));

        contextSetup = new Request.ContextSetup("index", new BytesArray("{}"), new MatchAllQueryBuilder());
        contextSetup.setXContentType(XContentType.JSON);
        request = new Request(
            new Script(ScriptType.INLINE, "painless", "emit(78.96, 12.12); emit(13.45, 56.78);", emptyMap()),
            "geo_point_field",
            contextSetup
        );
        response = innerShardOperation(request, scriptService, indexService);
        points = (List<Map<String, Object>>) response.getResult();
        assertEquals(12.12, (double) ((List<Object>) points.get(0).get("coordinates")).get(0), 0.00001);
        assertEquals(78.96, (double) ((List<Object>) points.get(0).get("coordinates")).get(1), 0.00001);
        assertEquals("Point", points.get(0).get("type"));
        assertEquals(56.78, (double) ((List<Object>) points.get(1).get("coordinates")).get(0), 0.00001);
        assertEquals(13.45, (double) ((List<Object>) points.get(1).get("coordinates")).get(1), 0.00001);
        assertEquals("Point", points.get(1).get("type"));
    }

    @SuppressWarnings("unchecked")
    public void testGeometryFieldExecutionContext() throws IOException {
        ScriptService scriptService = getInstanceFromNode(ScriptService.class);
        IndexService indexService = createIndex("index", Settings.EMPTY, "doc", "test_point", "type=geo_point");

        Request.ContextSetup contextSetup = new Request.ContextSetup(
            "index",
            new BytesArray("{\"test_point\":\"30.0,40.0\"}"),
            new MatchAllQueryBuilder()
        );
        contextSetup.setXContentType(XContentType.JSON);
        Request request = new Request(
            new Script(
                ScriptType.INLINE,
                "painless",
                "emit(\"Point(\" + doc['test_point'].value.lon + \" \" + doc['test_point'].value.lat + \")\")",
                emptyMap()
            ),
            "geometry_field",
            contextSetup
        );
        Response response = innerShardOperation(request, scriptService, indexService);
        List<Map<String, Object>> geometry = (List<Map<String, Object>>) response.getResult();
        assertEquals(40.0, (double) ((List<Object>) geometry.get(0).get("coordinates")).get(0), 0.00001);
        assertEquals(30.0, (double) ((List<Object>) geometry.get(0).get("coordinates")).get(1), 0.00001);
        assertEquals("Point", geometry.get(0).get("type"));

        contextSetup = new Request.ContextSetup("index", new BytesArray("{}"), new MatchAllQueryBuilder());
        contextSetup.setXContentType(XContentType.JSON);
        request = new Request(
            new Script(
                ScriptType.INLINE,
                "painless",
                "emit(\"LINESTRING(78.96 12.12, 12.12 78.96)\"); emit(\"POINT(13.45 56.78)\");",
                emptyMap()
            ),
            "geometry_field",
            contextSetup
        );
        response = innerShardOperation(request, scriptService, indexService);
        geometry = (List<Map<String, Object>>) response.getResult();
        assertEquals("GeometryCollection", geometry.get(0).get("type"));
        assertEquals(2, ((List<?>) geometry.get(0).get("geometries")).size());
    }

    public void testIpFieldExecutionContext() throws IOException {
        ScriptService scriptService = getInstanceFromNode(ScriptService.class);
        IndexService indexService = createIndex("index", Settings.EMPTY, "doc", "test_ip", "type=ip");

        Request.ContextSetup contextSetup = new Request.ContextSetup(
            "index",
            new BytesArray("{\"test_ip\":\"192.168.1.254\"}"),
            new MatchAllQueryBuilder()
        );
        contextSetup.setXContentType(XContentType.JSON);
        Request request = new Request(
            new Script(ScriptType.INLINE, "painless", "emit(doc['test_ip'].value);", emptyMap()),
            "ip_field",
            contextSetup
        );
        Response response = innerShardOperation(request, scriptService, indexService);
        assertEquals(Collections.singletonList("192.168.1.254"), response.getResult());

        contextSetup = new Request.ContextSetup("index", new BytesArray("{}"), new MatchAllQueryBuilder());
        contextSetup.setXContentType(XContentType.JSON);
        request = new Request(
            new Script(
                ScriptType.INLINE,
                "painless",
                "emit(\"192.168.0.1\"); emit(\"2001:db8::8a2e:370:7334\"); emit(\"2001:0db8:0000:0000:0000:8a2e:0370:7333\"); "
                    + "emit(\"127.0.0.1\"); emit(\"255.255.255.255\"); emit(\"0.0.0.0\");",
                emptyMap()
            ),
            "ip_field",
            contextSetup
        );
        response = innerShardOperation(request, scriptService, indexService);
        assertEquals(
            Arrays.asList("192.168.0.1", "2001:db8::8a2e:370:7334", "2001:db8::8a2e:370:7333", "127.0.0.1", "255.255.255.255", "0.0.0.0"),
            response.getResult()
        );
    }

    public void testLongFieldExecutionContext() throws IOException {
        ScriptService scriptService = getInstanceFromNode(ScriptService.class);
        IndexService indexService = createIndex("index", Settings.EMPTY, "doc", "test_value", "type=long");

        Request.ContextSetup contextSetup = new Request.ContextSetup(
            "index",
            new BytesArray("{\"test_value\":\"42\"}"),
            new MatchAllQueryBuilder()
        );
        contextSetup.setXContentType(XContentType.JSON);
        Request request = new Request(
            new Script(ScriptType.INLINE, "painless", "emit(doc['test_value'].value); emit(doc['test_value'].value - 2);", emptyMap()),
            "long_field",
            contextSetup
        );
        Response response = innerShardOperation(request, scriptService, indexService);
        assertEquals(Arrays.asList(42L, 40L), response.getResult());

        contextSetup = new Request.ContextSetup("index", new BytesArray("{}"), new MatchAllQueryBuilder());
        contextSetup.setXContentType(XContentType.JSON);
        request = new Request(
            new Script(
                ScriptType.INLINE,
                "painless",
                "emit(3L); emit(1L); emit(20000000000L); emit(10L); emit(-1000L); emit(0L);",
                emptyMap()
            ),
            "long_field",
            contextSetup
        );
        response = innerShardOperation(request, scriptService, indexService);
        assertEquals(Arrays.asList(3L, 1L, 20000000000L, 10L, -1000L, 0L), response.getResult());
    }

    public void testKeywordFieldExecutionContext() throws IOException {
        ScriptService scriptService = getInstanceFromNode(ScriptService.class);
        IndexService indexService = createIndex("index", Settings.EMPTY, "doc", "rank", "type=long", "text", "type=keyword");

        Request.ContextSetup contextSetup = new Request.ContextSetup("index", new BytesArray("""
            {"rank": 4.0, "text": "quick brown fox"}"""), new MatchQueryBuilder("text", "fox"));
        contextSetup.setXContentType(XContentType.JSON);
        contextSetup.setXContentType(XContentType.JSON);
        Request request = new Request(
            new Script(ScriptType.INLINE, "painless", "emit(doc['rank'].value + doc['text'].value)", emptyMap()),
            "keyword_field",
            contextSetup
        );
        Response response = innerShardOperation(request, scriptService, indexService);
        assertEquals(Collections.singletonList("4quick brown fox"), response.getResult());

        contextSetup = new Request.ContextSetup("index", new BytesArray("{}"), new MatchAllQueryBuilder());
        contextSetup.setXContentType(XContentType.JSON);
        request = new Request(
            new Script(
                ScriptType.INLINE,
                "painless",
                "emit(\"test\"); emit(\"baz was not here\"); emit(\"Data\"); emit(\"-10\"); emit(\"20\"); emit(\"9\");",
                emptyMap()
            ),
            "keyword_field",
            contextSetup
        );
        response = innerShardOperation(request, scriptService, indexService);
        assertEquals(Arrays.asList("test", "baz was not here", "Data", "-10", "20", "9"), response.getResult());
    }

    public void testCompositeExecutionContext() throws IOException {
        ScriptService scriptService = getInstanceFromNode(ScriptService.class);
        IndexService indexService = createIndex("index", Settings.EMPTY, "doc", "rank", "type=long", "text", "type=keyword");

        Request.ContextSetup contextSetup = new Request.ContextSetup("index", new BytesArray("{}"), new MatchAllQueryBuilder());
        contextSetup.setXContentType(XContentType.JSON);
        Request request = new Request(
            new Script(ScriptType.INLINE, "painless", "emit(\"foo\", \"bar\"); emit(\"foo2\", 2);", emptyMap()),
            "composite_field",
            contextSetup
        );
        Response response = innerShardOperation(request, scriptService, indexService);
        assertEquals(Map.of("composite_field.foo", List.of("bar"), "composite_field.foo2", List.of(2)), response.getResult());
    }

    public void testContextWhitelists() throws IOException {
        ScriptService scriptService = getInstanceFromNode(ScriptService.class);
        // score
        Request request = new Request(new Script("sigmoid(1.0, 2.0, 3.0)"), null, null);
        Response response = innerShardOperation(request, scriptService, null);
        double result = Double.parseDouble((String) response.getResult());
        assertEquals(0.111, result, 0.001);

        // ingest
        request = new Request(new Script("'foo'.sha1()"), null, null);
        response = innerShardOperation(request, scriptService, null);
        assertEquals("0beec7b5ea3f0fdbc95d0dd47f3c5bc275da8a33", response.getResult());

        // json
        request = new Request(new Script("Json.load('{\"a\": 1, \"b\": 2}')['b']"), null, null);
        response = innerShardOperation(request, scriptService, null);
        assertEquals(2, Integer.parseInt((String) response.getResult()));
    }

    /**
     * When an index expression with a remote cluster name is passed into Request.ContextSetup, it
     * is parsed into separate fields - clusterAlias and index.
     * The other tests in this suite test without a clusterAlias prefix.
     * This test ensures that innerShardOperation works the same with one present, since the clusterAlias
     * field is only needed by the initial coordinator of the action to determine where to run the
     * action (which is not part of the tests in this suite).
     */
    public void testFilterExecutionContextWorksWithRemoteClusterPrefix() throws IOException {
        ScriptService scriptService = getInstanceFromNode(ScriptService.class);
        String indexName = "index";
        IndexService indexService = createIndex(indexName, Settings.EMPTY, "doc", "field", "type=long");

        String indexNameWithClusterAlias = "remote1:" + indexName;
        Request.ContextSetup contextSetup = new Request.ContextSetup(indexNameWithClusterAlias, new BytesArray("{\"field\": 3}"), null);
        contextSetup.setXContentType(XContentType.JSON);
        Request request = new Request(new Script("doc['field'].value >= 3"), "filter", contextSetup);
        Response response = innerShardOperation(request, scriptService, indexService);
        assertThat(response.getResult(), equalTo(true));

        contextSetup = new Request.ContextSetup(indexNameWithClusterAlias, new BytesArray("{\"field\": 3}"), null);
        contextSetup.setXContentType(XContentType.JSON);
        request = new Request(
            new Script(ScriptType.INLINE, "painless", "doc['field'].value >= params.max", singletonMap("max", 3)),
            "filter",
            contextSetup
        );
        response = innerShardOperation(request, scriptService, indexService);
        assertThat(response.getResult(), equalTo(true));

        contextSetup = new Request.ContextSetup(indexNameWithClusterAlias, new BytesArray("{\"field\": 2}"), null);
        contextSetup.setXContentType(XContentType.JSON);
        request = new Request(
            new Script(ScriptType.INLINE, "painless", "doc['field'].value >= params.max", singletonMap("max", 3)),
            "filter",
            contextSetup
        );
        response = innerShardOperation(request, scriptService, indexService);
        assertThat(response.getResult(), equalTo(false));
    }

    public void testParseClusterAliasAndIndex() {
        record ValidTestCase(String input, Tuple<String, String> output) {}

        ValidTestCase[] cases = new ValidTestCase[] {
            // valid index expressions
            new ValidTestCase("remote1:foo", new Tuple<>("remote1", "foo")),
            new ValidTestCase("foo", new Tuple<>(null, "foo")),
            new ValidTestCase("foo,bar", new Tuple<>(null, "foo,bar")), // this method only checks for invalid ":"
            new ValidTestCase("", new Tuple<>(null, "")),
            new ValidTestCase(null, new Tuple<>(null, null)) };

        for (ValidTestCase testCase : cases) {
            Tuple<String, String> output = Request.ContextSetup.parseClusterAliasAndIndex(testCase.input);
            assertEquals(testCase.output(), output);
        }

        expectThrows(IllegalArgumentException.class, () -> Request.ContextSetup.parseClusterAliasAndIndex("remote1::foo"));
        expectThrows(IllegalArgumentException.class, () -> Request.ContextSetup.parseClusterAliasAndIndex("remote1:foo:"));
        expectThrows(IllegalArgumentException.class, () -> Request.ContextSetup.parseClusterAliasAndIndex(" :remote1:foo"));
        expectThrows(IllegalArgumentException.class, () -> Request.ContextSetup.parseClusterAliasAndIndex("remote1:foo: "));
        expectThrows(IllegalArgumentException.class, () -> Request.ContextSetup.parseClusterAliasAndIndex("remote1::::"));
        expectThrows(IllegalArgumentException.class, () -> Request.ContextSetup.parseClusterAliasAndIndex("::"));
        expectThrows(IllegalArgumentException.class, () -> Request.ContextSetup.parseClusterAliasAndIndex(":x:"));
        expectThrows(IllegalArgumentException.class, () -> Request.ContextSetup.parseClusterAliasAndIndex(" : : "));
        expectThrows(IllegalArgumentException.class, () -> Request.ContextSetup.parseClusterAliasAndIndex(":blogs:"));
        expectThrows(IllegalArgumentException.class, () -> Request.ContextSetup.parseClusterAliasAndIndex(":blogs"));
        expectThrows(IllegalArgumentException.class, () -> Request.ContextSetup.parseClusterAliasAndIndex(" :blogs"));
        expectThrows(IllegalArgumentException.class, () -> Request.ContextSetup.parseClusterAliasAndIndex("blogs:"));
        expectThrows(IllegalArgumentException.class, () -> Request.ContextSetup.parseClusterAliasAndIndex("blogs:  "));
        expectThrows(IllegalArgumentException.class, () -> Request.ContextSetup.parseClusterAliasAndIndex("remote1:foo,remote2:bar"));
        expectThrows(IllegalArgumentException.class, () -> Request.ContextSetup.parseClusterAliasAndIndex("a:b,c:d,e:f"));
    }

    public void testRemoveClusterAliasFromIndexExpression() {
        {
            // index expressions with no clusterAlias should come back unchanged
            PainlessExecuteAction.Request request = createRequest("blogs");
            assertThat(request.index(), equalTo("blogs"));
            PainlessExecuteAction.TransportAction.removeClusterAliasFromIndexExpression(request);
            assertThat(request.index(), equalTo("blogs"));
        }
        {
            // index expressions with no index specified should come back unchanged
            PainlessExecuteAction.Request request = createRequest(null);
            assertThat(request.index(), nullValue());
            PainlessExecuteAction.TransportAction.removeClusterAliasFromIndexExpression(request);
            assertThat(request.index(), nullValue());
        }
        {
            // index expressions with clusterAlias should come back with it stripped off
            PainlessExecuteAction.Request request = createRequest("remote1:blogs");
            assertThat(request.index(), equalTo("remote1:blogs"));
            PainlessExecuteAction.TransportAction.removeClusterAliasFromIndexExpression(request);
            assertThat(request.index(), equalTo("blogs"));
        }
        {
            // index expressions with clusterAlias should come back with it stripped off
            PainlessExecuteAction.Request request = createRequest("remote1:remote1");
            assertThat(request.index(), equalTo("remote1:remote1"));
            PainlessExecuteAction.TransportAction.removeClusterAliasFromIndexExpression(request);
            assertThat(request.index(), equalTo("remote1"));
        }
    }

    private PainlessExecuteAction.Request createRequest(String indexExpression) {
        return new PainlessExecuteAction.Request(
            new Script("100.0 / 1000.0"),
            null,
            new PainlessExecuteAction.Request.ContextSetup(indexExpression, null, null)
        );
    }
}

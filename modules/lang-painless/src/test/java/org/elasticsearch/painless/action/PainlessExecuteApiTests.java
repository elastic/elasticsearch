/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.painless.action;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
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

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.painless.action.PainlessExecuteAction.TransportAction.innerShardOperation;
import static org.hamcrest.Matchers.equalTo;

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

        Exception e = expectThrows(ScriptException.class,
            () -> {
            Request r = new Request(new Script(ScriptType.INLINE,
                "painless", "params.count / params.total + doc['constant']", params), null, null);
            innerShardOperation(r, scriptService, null);
        });
        assertThat(e.getCause().getMessage(), equalTo("cannot resolve symbol [doc]"));
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
        request = new Request(new Script(ScriptType.INLINE, "painless", "doc['field'].value >= params.max",
            singletonMap("max", 3)), "filter", contextSetup);
        response = innerShardOperation(request, scriptService, indexService);
        assertThat(response.getResult(), equalTo(true));

        contextSetup = new Request.ContextSetup("index", new BytesArray("{\"field\": 2}"), null);
        contextSetup.setXContentType(XContentType.JSON);
        request = new Request(new Script(ScriptType.INLINE, "painless", "doc['field'].value >= params.max",
            singletonMap("max", 3)), "filter", contextSetup);
        response = innerShardOperation(request, scriptService, indexService);
        assertThat(response.getResult(), equalTo(false));
    }

    public void testScoreExecutionContext() throws IOException {
        ScriptService scriptService = getInstanceFromNode(ScriptService.class);
        IndexService indexService = createIndex("index", Settings.EMPTY, "doc", "rank", "type=long", "text", "type=text");

        Request.ContextSetup contextSetup = new Request.ContextSetup("index",
            new BytesArray("{\"rank\": 4.0, \"text\": \"quick brown fox\"}"), new MatchQueryBuilder("text", "fox"));
        contextSetup.setXContentType(XContentType.JSON);
        Request request = new Request(new Script(ScriptType.INLINE, "painless",
            "Math.round((_score + (doc['rank'].value / params.max_rank)) * 100.0) / 100.0", singletonMap("max_rank", 5.0)), "score",
            contextSetup);
        Response response = innerShardOperation(request, scriptService, indexService);
        assertThat(response.getResult(), equalTo(0.93D));
    }

    public void testBooleanFieldExecutionContext() throws IOException {
        ScriptService scriptService = getInstanceFromNode(ScriptService.class);
        IndexService indexService = createIndex("index", Settings.EMPTY, "doc", "rank", "type=long", "text", "type=text");

        Request.ContextSetup contextSetup = new Request.ContextSetup("index",
                new BytesArray("{\"rank\": 4.0, \"text\": \"quick brown fox\"}"), new MatchQueryBuilder("text", "fox"));
        contextSetup.setXContentType(XContentType.JSON);
        Request request = new Request(new Script(ScriptType.INLINE, "painless",
                "emit(doc['rank'].value < params.max_rank)", singletonMap("max_rank", 5.0)), "boolean_field",
                contextSetup);
        Response response = innerShardOperation(request, scriptService, indexService);
        assertArrayEquals((boolean[])response.getResult(), new boolean[] {true});

        contextSetup = new Request.ContextSetup("index", new BytesArray("{}"), new MatchAllQueryBuilder());
        contextSetup.setXContentType(XContentType.JSON);
        request = new Request(new Script(ScriptType.INLINE, "painless",
                "emit(false); emit(true); emit (false);", emptyMap()), "boolean_field",
                contextSetup);
        response = innerShardOperation(request, scriptService, indexService);
        assertArrayEquals((boolean[])response.getResult(), new boolean[] {false, false, true});
    }

    public void testDateFieldExecutionContext() throws IOException {
        ScriptService scriptService = getInstanceFromNode(ScriptService.class);
        IndexService indexService = createIndex("index", Settings.EMPTY, "doc", "test_date", "type=date");

        Request.ContextSetup contextSetup = new Request.ContextSetup("index",
                new BytesArray("{\"test_date\":\"2015-01-01T12:10:30Z\"}"), new MatchAllQueryBuilder());
        contextSetup.setXContentType(XContentType.JSON);
        Request request = new Request(new Script(ScriptType.INLINE, "painless",
                "emit(doc['test_date'].value.toInstant().toEpochMilli())", emptyMap()), "date_field",
                contextSetup);
        Response response = innerShardOperation(request, scriptService, indexService);
        assertArrayEquals((long[])response.getResult(), new long[] {1420114230000L});

        contextSetup = new Request.ContextSetup("index", new BytesArray("{}"), new MatchAllQueryBuilder());
        contextSetup.setXContentType(XContentType.JSON);
        request = new Request(new Script(ScriptType.INLINE, "painless",
                "emit(ZonedDateTime.parse(\"2021-01-01T00:00:00Z\").toInstant().toEpochMilli());\n" +
                "emit(ZonedDateTime.parse(\"1942-05-31T15:16:17Z\").toInstant().toEpochMilli());\n" +
                "emit(ZonedDateTime.parse(\"2035-10-13T10:54:19Z\").toInstant().toEpochMilli());",
                emptyMap()), "date_field", contextSetup);
        response = innerShardOperation(request, scriptService, indexService);
        assertArrayEquals((long[])response.getResult(), new long[] {-870597823000L, 1609459200000L, 2075885659000L});
    }

    public void testDoubleFieldExecutionContext() throws IOException {
        ScriptService scriptService = getInstanceFromNode(ScriptService.class);
        IndexService indexService = createIndex("index", Settings.EMPTY, "doc", "rank", "type=long", "text", "type=text");

        Request.ContextSetup contextSetup = new Request.ContextSetup("index",
                new BytesArray("{\"rank\": 4.0, \"text\": \"quick brown fox\"}"), new MatchQueryBuilder("text", "fox"));
        contextSetup.setXContentType(XContentType.JSON);
        Request request = new Request(new Script(ScriptType.INLINE, "painless",
                "emit(doc['rank'].value); emit(Math.log(doc['rank'].value))", emptyMap()), "double_field",
                contextSetup);
        Response response = innerShardOperation(request, scriptService, indexService);
        assertArrayEquals((double[])response.getResult(), new double[] {Math.log(4.0), 4.0}, 0.00001);

        contextSetup = new Request.ContextSetup("index", new BytesArray("{}"), new MatchAllQueryBuilder());
        contextSetup.setXContentType(XContentType.JSON);
        request = new Request(new Script(ScriptType.INLINE, "painless",
                "emit(3.1); emit(2.29); emit(-12.47); emit(-12.46); emit(Double.MAX_VALUE); emit(0.0);",
                emptyMap()), "double_field", contextSetup);
        response = innerShardOperation(request, scriptService, indexService);
        assertArrayEquals((double[])response.getResult(), new double[] {-12.47, -12.46, 0.0, 2.29, 3.1, Double.MAX_VALUE}, 0.00001);
    }

    public void testGeoPointFieldExecutionContext() throws IOException {
        ScriptService scriptService = getInstanceFromNode(ScriptService.class);
        IndexService indexService = createIndex("index", Settings.EMPTY, "doc", "test_point", "type=geo_point");

        Request.ContextSetup contextSetup = new Request.ContextSetup("index",
                new BytesArray("{\"test_point\":\"30.0,40.0\"}"), new MatchAllQueryBuilder());
        contextSetup.setXContentType(XContentType.JSON);
        Request request = new Request(new Script(ScriptType.INLINE, "painless",
                "emit(doc['test_point'].value.lat + 1.0, doc['test_point'].value.lon - 1.0)", emptyMap()),
                "geo_point_field", contextSetup);
        Response response = innerShardOperation(request, scriptService, indexService);
        assertArrayEquals((long[])response.getResult(), new long[] {3176939252927413179L});

        contextSetup = new Request.ContextSetup("index", new BytesArray("{}"), new MatchAllQueryBuilder());
        contextSetup.setXContentType(XContentType.JSON);
        request = new Request(new Script(ScriptType.INLINE, "painless",
                "emit(78.96, 12.12); emit(13.45, 56.78);",
                emptyMap()), "geo_point_field", contextSetup);
        response = innerShardOperation(request, scriptService, indexService);
        assertArrayEquals((long[])response.getResult(), new long[] {1378381707499043786L, 8091971733044486384L});
    }

    public void testIpFieldExecutionContext() throws IOException {
        ScriptService scriptService = getInstanceFromNode(ScriptService.class);
        IndexService indexService = createIndex("index", Settings.EMPTY, "doc", "test_ip", "type=ip");

        Request.ContextSetup contextSetup = new Request.ContextSetup("index",
                new BytesArray("{\"test_ip\":\"192.168.1.254\"}"), new MatchAllQueryBuilder());
        contextSetup.setXContentType(XContentType.JSON);
        Request request = new Request(new Script(ScriptType.INLINE, "painless",
                "emit(doc['test_ip'].value);", emptyMap()),
                "ip_field", contextSetup);
        Response response = innerShardOperation(request, scriptService, indexService);
        assertArrayEquals((BytesRef[])response.getResult(),
                new BytesRef[] {new BytesRef(new byte[] {0,0,0,0,0,0,0,0,0,0,(byte)255,(byte)255,(byte)192,(byte)168,1,(byte)254})});

        contextSetup = new Request.ContextSetup("index", new BytesArray("{}"), new MatchAllQueryBuilder());
        contextSetup.setXContentType(XContentType.JSON);
        request = new Request(new Script(ScriptType.INLINE, "painless",
                "emit(\"192.168.0.1\"); emit(\"127.0.0.1\"); emit(\"255.255.255.255\"); emit(\"0.0.0.0\");",
                emptyMap()), "ip_field", contextSetup);
        response = innerShardOperation(request, scriptService, indexService);
        assertArrayEquals((BytesRef[])response.getResult(), new BytesRef[] {
                new BytesRef(new byte[] {0,0,0,0,0,0,0,0,0,0,-1,-1,0,0,0,0}),
                new BytesRef(new byte[] {0,0,0,0,0,0,0,0,0,0,-1,-1,127,0,0,1}),
                new BytesRef(new byte[] {0,0,0,0,0,0,0,0,0,0,-1,-1,-64,-88,0,1}),
                new BytesRef(new byte[] {0,0,0,0,0,0,0,0,0,0,-1,-1,-1,-1,-1,-1})});
    }

    public void testLongFieldExecutionContext() throws IOException {
        ScriptService scriptService = getInstanceFromNode(ScriptService.class);
        IndexService indexService = createIndex("index", Settings.EMPTY, "doc", "test_point", "type=geo_point");

        Request.ContextSetup contextSetup = new Request.ContextSetup("index",
                new BytesArray("{\"test_point\":\"30.2,40.2\"}"), new MatchAllQueryBuilder());
        contextSetup.setXContentType(XContentType.JSON);
        Request request = new Request(new Script(ScriptType.INLINE, "painless",
                "emit((long)doc['test_point'].value.lat); emit((long)doc['test_point'].value.lon);", emptyMap()),
                "long_field", contextSetup);
        Response response = innerShardOperation(request, scriptService, indexService);
        assertArrayEquals((long[])response.getResult(), new long[] {30, 40});

        contextSetup = new Request.ContextSetup("index", new BytesArray("{}"), new MatchAllQueryBuilder());
        contextSetup.setXContentType(XContentType.JSON);
        request = new Request(new Script(ScriptType.INLINE, "painless",
                "emit(3L); emit(1L); emit(20000000000L); emit(10L); emit(-1000L); emit(0L);",
                emptyMap()), "long_field", contextSetup);
        response = innerShardOperation(request, scriptService, indexService);
        assertArrayEquals((long[])response.getResult(), new long[] {-1000L, 0L, 1L, 3L, 10L, 20000000000L});
    }

    public void testKeywordFieldExecutionContext() throws IOException {
        ScriptService scriptService = getInstanceFromNode(ScriptService.class);
        IndexService indexService = createIndex("index", Settings.EMPTY, "doc", "test_point", "type=geo_point");

        Request.ContextSetup contextSetup = new Request.ContextSetup("index",
                new BytesArray("{\"test_point\":\"30.2,40.2\"}"), new MatchAllQueryBuilder());
        contextSetup.setXContentType(XContentType.JSON);
        Request request = new Request(new Script(ScriptType.INLINE, "painless",
                "emit(doc['test_point'].value.lat.toString().substring(0, 5)); " +
                        "emit(doc['test_point'].value.lon.toString().substring(0, 5));", emptyMap()),
                "keyword_field", contextSetup);
        Response response = innerShardOperation(request, scriptService, indexService);
        assertArrayEquals((String[])response.getResult(), new String[] {"30.19", "40.19"});

        contextSetup = new Request.ContextSetup("index", new BytesArray("{}"), new MatchAllQueryBuilder());
        contextSetup.setXContentType(XContentType.JSON);
        request = new Request(new Script(ScriptType.INLINE, "painless",
                "emit(\"test\"); emit(\"baz was not here\"); emit(\"Data\"); emit(\"-10\"); emit(\"20\"); emit(\"9\");",
                emptyMap()), "keyword_field", contextSetup);
        response = innerShardOperation(request, scriptService, indexService);
        assertArrayEquals((String[])response.getResult(), new String[] {"-10", "20", "9", "Data", "baz was not here", "test"});
    }

    public void testContextWhitelists() throws IOException {
        ScriptService scriptService = getInstanceFromNode(ScriptService.class);
        // score
        Request request = new Request(new Script("sigmoid(1.0, 2.0, 3.0)"), null, null);
        Response response = innerShardOperation(request, scriptService, null);
        double result = Double.parseDouble((String)response.getResult());
        assertEquals(0.111, result, 0.001);

        // ingest
        request = new Request(new Script("'foo'.sha1()"), null, null);
        response = innerShardOperation(request, scriptService, null);
        assertEquals("0beec7b5ea3f0fdbc95d0dd47f3c5bc275da8a33", response.getResult());

        // movfn
        request = new Request(new Script("MovingFunctions.max(new double[]{1, 3, 2})"), null, null);
        response = innerShardOperation(request, scriptService, null);
        assertEquals(3.0, Double.parseDouble((String)response.getResult()), .1);

        // json
        request = new Request(new Script("Json.load('{\"a\": 1, \"b\": 2}')['b']"), null, null);
        response = innerShardOperation(request, scriptService, null);
        assertEquals(2, Integer.parseInt((String)response.getResult()));
    }

}

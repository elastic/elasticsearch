/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.Column;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.parser.QueryParam;
import org.elasticsearch.xpack.esql.parser.QueryParams;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class EsqlQueryRequestTests extends ESTestCase {

    public void testParseFields() throws IOException {
        String query = randomAlphaOfLengthBetween(1, 100);
        boolean columnar = randomBoolean();
        Locale locale = randomLocale(random());
        QueryBuilder filter = randomQueryBuilder();

        List<QueryParam> params = randomParameters();
        boolean hasParams = params.isEmpty() == false;
        StringBuilder paramsString = paramsString(params, hasParams);
        String json = String.format(Locale.ROOT, """
            {
                "query": "%s",
                "columnar": %s,
                "locale": "%s",
                "filter": %s
                %s""", query, columnar, locale.toLanguageTag(), filter, paramsString);

        EsqlQueryRequest request = parseEsqlQueryRequestSync(json);

        assertEquals(query, request.query());
        assertEquals(columnar, request.columnar());
        assertEquals(locale.toLanguageTag(), request.locale().toLanguageTag());
        assertEquals(locale, request.locale());
        assertEquals(filter, request.filter());
        assertEquals(params.size(), request.params().size());
        for (int i = 0; i < params.size(); i++) {
            assertEquals(params.get(i), request.params().get(i + 1));
        }
    }

    public void testNamedParams() throws IOException {
        String query = randomAlphaOfLengthBetween(1, 100);
        boolean columnar = randomBoolean();
        Locale locale = randomLocale(random());
        QueryBuilder filter = randomQueryBuilder();

        String paramsString = """
            ,"params":[ {"n1" : "8.15.0" }, { "n2" : 0.05 }, {"n3" : -799810013 },
             {"n4" : "127.0.0.1"}, {"n5" : "esql"}, {"n_6" : null}, {"n7_" : false},
             {"_n1" : "8.15.0" }, { "__n2" : 0.05 }, {"__3" : -799810013 },
             {"__4n" : "127.0.0.1"}, {"_n5" : "esql"}, {"_n6" : null}, {"_n7" : false}] }""";
        List<QueryParam> params = new ArrayList<>(4);
        params.add(new QueryParam("n1", "8.15.0", DataType.KEYWORD));
        params.add(new QueryParam("n2", 0.05, DataType.DOUBLE));
        params.add(new QueryParam("n3", -799810013, DataType.INTEGER));
        params.add(new QueryParam("n4", "127.0.0.1", DataType.KEYWORD));
        params.add(new QueryParam("n5", "esql", DataType.KEYWORD));
        params.add(new QueryParam("n_6", null, DataType.NULL));
        params.add(new QueryParam("n7_", false, DataType.BOOLEAN));
        params.add(new QueryParam("_n1", "8.15.0", DataType.KEYWORD));
        params.add(new QueryParam("__n2", 0.05, DataType.DOUBLE));
        params.add(new QueryParam("__3", -799810013, DataType.INTEGER));
        params.add(new QueryParam("__4n", "127.0.0.1", DataType.KEYWORD));
        params.add(new QueryParam("_n5", "esql", DataType.KEYWORD));
        params.add(new QueryParam("_n6", null, DataType.NULL));
        params.add(new QueryParam("_n7", false, DataType.BOOLEAN));
        String json = String.format(Locale.ROOT, """
            {
                "query": "%s",
                "columnar": %s,
                "locale": "%s",
                "filter": %s
                %s""", query, columnar, locale.toLanguageTag(), filter, paramsString);

        EsqlQueryRequest request = parseEsqlQueryRequestSync(json);

        assertEquals(query, request.query());
        assertEquals(columnar, request.columnar());
        assertEquals(locale.toLanguageTag(), request.locale().toLanguageTag());
        assertEquals(locale, request.locale());
        assertEquals(filter, request.filter());
        assertEquals(params.size(), request.params().size());

        for (int i = 0; i < request.params().size(); i++) {
            assertEquals(params.get(i), request.params().get(i + 1));
        }
    }

    public void testInvalidParams() throws IOException {
        String query = randomAlphaOfLengthBetween(1, 100);
        boolean columnar = randomBoolean();
        Locale locale = randomLocale(random());
        QueryBuilder filter = randomQueryBuilder();

        String paramsString1 = """
            "params":[ {"1" : "v1" }, {"1x" : "v1" }, {"@a" : "v1" }, {"@-#" : "v1" }, 1, 2, {"_1" : "v1" }, {"Å" : 0}, {"x " : 0}]""";
        String json1 = String.format(Locale.ROOT, """
            {
                %s
                "query": "%s",
                "columnar": %s,
                "locale": "%s",
                "filter": %s
            }""", paramsString1, query, columnar, locale.toLanguageTag(), filter);

        Exception e1 = expectThrows(XContentParseException.class, () -> parseEsqlQueryRequestSync(json1));
        assertThat(
            e1.getCause().getMessage(),
            containsString(
                "Failed to parse params: [2:16] [1] is not a valid parameter name, "
                    + "a valid parameter name starts with a letter or underscore, and contains letters, digits and underscores only"
            )
        );
        assertThat(e1.getCause().getMessage(), containsString("[2:31] [1x] is not a valid parameter name"));
        assertThat(e1.getCause().getMessage(), containsString("[2:47] [@a] is not a valid parameter name"));
        assertThat(e1.getCause().getMessage(), containsString("[2:63] [@-#] is not a valid parameter name"));
        assertThat(e1.getCause().getMessage(), containsString("[2:102] [Å] is not a valid parameter name"));
        assertThat(e1.getCause().getMessage(), containsString("[2:113] [x ] is not a valid parameter name"));

        assertThat(
            e1.getCause().getMessage(),
            containsString(
                "Params cannot contain both named and unnamed parameters; "
                    + "got [{1:v1}, {1x:v1}, {@a:v1}, {@-#:v1}, {_1:v1}, {Å:0}, {x :0}] and [{1}, {2}]"
            )
        );

        String paramsString2 = """
            "params":[ 1, 2, {"1" : "v1" }, {"1x" : "v1" }]""";
        String json2 = String.format(Locale.ROOT, """
            {
                %s
                "query": "%s",
                "columnar": %s,
                "locale": "%s",
                "filter": %s
            }""", paramsString2, query, columnar, locale.toLanguageTag(), filter);

        Exception e2 = expectThrows(XContentParseException.class, () -> parseEsqlQueryRequestSync(json2));
        assertThat(
            e2.getCause().getMessage(),
            containsString(
                "Failed to parse params: [2:22] [1] is not a valid parameter name, "
                    + "a valid parameter name starts with a letter or underscore, and contains letters, digits and underscores only"
            )
        );
        assertThat(e2.getCause().getMessage(), containsString("[2:37] [1x] is not a valid parameter name"));
        assertThat(
            e2.getCause().getMessage(),
            containsString("Params cannot contain both named and unnamed parameters; got [{1:v1}, {1x:v1}] and [{1}, {2}]")
        );
    }

    // Test for https://github.com/elastic/elasticsearch/issues/110028
    public void testNamedParamsMutation() {
        EsqlQueryRequest request1 = new EsqlQueryRequest();
        assertThat(request1.params(), equalTo(new QueryParams()));
        var exceptionMessage = randomAlphaOfLength(10);
        var paramName = randomAlphaOfLength(5);
        var paramValue = randomAlphaOfLength(5);
        request1.params().addParsingError(new ParsingException(Source.EMPTY, exceptionMessage));
        request1.params().addTokenParam(null, new QueryParam(paramName, paramValue, DataType.KEYWORD));

        EsqlQueryRequest request2 = new EsqlQueryRequest();
        assertThat(request2.params(), equalTo(new QueryParams()));
    }

    public void testParseFieldsForAsync() throws IOException {
        String query = randomAlphaOfLengthBetween(1, 100);
        boolean columnar = randomBoolean();
        Locale locale = randomLocale(random());
        QueryBuilder filter = randomQueryBuilder();

        List<QueryParam> params = randomParameters();
        boolean hasParams = params.isEmpty() == false;
        StringBuilder paramsString = paramsString(params, hasParams);
        boolean keepOnCompletion = randomBoolean();
        TimeValue waitForCompletion = randomTimeValue();
        TimeValue keepAlive = randomTimeValue();
        String json = String.format(
            Locale.ROOT,
            """
                {
                    "query": "%s",
                    "columnar": %s,
                    "locale": "%s",
                    "filter": %s,
                    "keep_on_completion": %s,
                    "wait_for_completion_timeout": "%s",
                    "keep_alive": "%s"
                    %s""",
            query,
            columnar,
            locale.toLanguageTag(),
            filter,
            keepOnCompletion,
            waitForCompletion.getStringRep(),
            keepAlive.getStringRep(),
            paramsString
        );

        EsqlQueryRequest request = parseEsqlQueryRequestAsync(json);

        assertEquals(query, request.query());
        assertEquals(columnar, request.columnar());
        assertEquals(locale.toLanguageTag(), request.locale().toLanguageTag());
        assertEquals(locale, request.locale());
        assertEquals(filter, request.filter());
        assertEquals(keepOnCompletion, request.keepOnCompletion());
        assertEquals(waitForCompletion, request.waitForCompletionTimeout());
        assertEquals(keepAlive, request.keepAlive());
        assertEquals(params.size(), request.params().size());
        for (int i = 0; i < params.size(); i++) {
            assertEquals(params.get(i), request.params().get(i + 1));
        }
    }

    public void testDefaultValueForOptionalAsyncParams() throws IOException {
        String query = randomAlphaOfLengthBetween(1, 100);
        String json = String.format(Locale.ROOT, """
            {
                "query": "%s"
            }
            """, query);
        EsqlQueryRequest request = parseEsqlQueryRequestAsync(json);
        assertEquals(query, request.query());
        assertFalse(request.keepOnCompletion());
        assertEquals(TimeValue.timeValueSeconds(1), request.waitForCompletionTimeout());
        assertEquals(TimeValue.timeValueDays(5), request.keepAlive());
    }

    public void testRejectUnknownFields() {
        assertParserErrorMessage("""
            {
                "query": "foo",
                "columbar": true
            }""", "unknown field [columbar] did you mean [columnar]?");

        assertParserErrorMessage("""
            {
                "query": "foo",
                "asdf": "Z"
            }""", "unknown field [asdf]");
    }

    public void testMissingQueryIsNotValid() throws IOException {
        String json = """
            {
                "columnar": true
            }""";
        EsqlQueryRequest request = parseEsqlQueryRequest(json, randomBoolean());
        assertNotNull(request.validate());
        assertThat(request.validate().getMessage(), containsString("[query] is required"));
    }

    public void testPragmasOnlyValidOnSnapshot() throws IOException {
        String json = """
            {
                "query": "ROW x = 1",
                "pragma": {"foo": "bar"}
            }
            """;

        EsqlQueryRequest request = parseEsqlQueryRequest(json, randomBoolean());
        request.onSnapshotBuild(true);
        assertNull(request.validate());

        request.onSnapshotBuild(false);
        assertNotNull(request.validate());
        assertThat(request.validate().getMessage(), containsString("[pragma] only allowed in snapshot builds"));

        request.acceptedPragmaRisks(true);
        assertNull(request.validate());
    }

    public void testTablesKeyword() throws IOException {
        String json = """
            {
                "query": "ROW x = 1",
                "tables": {"a": {"c": {"keyword": ["a", "b", null, 1, 2.0, ["c", "d"], false]}}}
            }
            """;
        EsqlQueryRequest request = parseEsqlQueryRequest(json, randomBoolean());
        Column c = request.tables().get("a").get("c");
        assertThat(c.type(), equalTo(DataType.KEYWORD));
        try (
            BytesRefBlock.Builder builder = new BlockFactory(
                new NoopCircuitBreaker(CircuitBreaker.REQUEST),
                BigArrays.NON_RECYCLING_INSTANCE
            ).newBytesRefBlockBuilder(10)
        ) {
            builder.appendBytesRef(new BytesRef("a"));
            builder.appendBytesRef(new BytesRef("b"));
            builder.appendNull();
            builder.appendBytesRef(new BytesRef("1"));
            builder.appendBytesRef(new BytesRef("2.0"));
            builder.beginPositionEntry();
            builder.appendBytesRef(new BytesRef("c"));
            builder.appendBytesRef(new BytesRef("d"));
            builder.endPositionEntry();
            builder.appendBytesRef(new BytesRef("false"));
            assertThat(c.values(), equalTo(builder.build()));
        }
        assertTablesOnlyValidOnSnapshot(request);
    }

    public void testTablesInteger() throws IOException {
        String json = """
            {
                "query": "ROW x = 1",
                "tables": {"a": {"c": {"integer": [1, 2, "3", null, [5, 6]]}}}
            }
            """;

        EsqlQueryRequest request = parseEsqlQueryRequest(json, randomBoolean());
        Column c = request.tables().get("a").get("c");
        assertThat(c.type(), equalTo(DataType.INTEGER));
        try (
            IntBlock.Builder builder = new BlockFactory(new NoopCircuitBreaker(CircuitBreaker.REQUEST), BigArrays.NON_RECYCLING_INSTANCE)
                .newIntBlockBuilder(10)
        ) {
            builder.appendInt(1);
            builder.appendInt(2);
            builder.appendInt(3);
            builder.appendNull();
            builder.beginPositionEntry();
            builder.appendInt(5);
            builder.appendInt(6);
            builder.endPositionEntry();
            assertThat(c.values(), equalTo(builder.build()));
        }
        assertTablesOnlyValidOnSnapshot(request);
    }

    public void testTablesLong() throws IOException {
        String json = """
            {
                "query": "ROW x = 1",
                "tables": {"a": {"c": {"long": [1, 2, "3", null, [5, 6]]}}}
            }
            """;

        EsqlQueryRequest request = parseEsqlQueryRequest(json, randomBoolean());
        Column c = request.tables().get("a").get("c");
        assertThat(c.type(), equalTo(DataType.LONG));
        try (
            LongBlock.Builder builder = new BlockFactory(new NoopCircuitBreaker(CircuitBreaker.REQUEST), BigArrays.NON_RECYCLING_INSTANCE)
                .newLongBlockBuilder(10)
        ) {
            builder.appendLong(1);
            builder.appendLong(2);
            builder.appendLong(3);
            builder.appendNull();
            builder.beginPositionEntry();
            builder.appendLong(5);
            builder.appendLong(6);
            builder.endPositionEntry();
            assertThat(c.values(), equalTo(builder.build()));
        }
        assertTablesOnlyValidOnSnapshot(request);
    }

    public void testTablesDouble() throws IOException {
        String json = """
            {
                "query": "ROW x = 1",
                "tables": {"a": {"c": {"double": [1.1, 2, "3.1415", null, [5.1, "-6"]]}}}
            }
            """;

        EsqlQueryRequest request = parseEsqlQueryRequest(json, randomBoolean());
        Column c = request.tables().get("a").get("c");
        assertThat(c.type(), equalTo(DataType.DOUBLE));
        try (
            DoubleBlock.Builder builder = new BlockFactory(new NoopCircuitBreaker(CircuitBreaker.REQUEST), BigArrays.NON_RECYCLING_INSTANCE)
                .newDoubleBlockBuilder(10)
        ) {
            builder.appendDouble(1.1);
            builder.appendDouble(2);
            builder.appendDouble(3.1415);
            builder.appendNull();
            builder.beginPositionEntry();
            builder.appendDouble(5.1);
            builder.appendDouble(-6);
            builder.endPositionEntry();
            assertThat(c.values(), equalTo(builder.build()));
        }
        assertTablesOnlyValidOnSnapshot(request);
    }

    public void testManyTables() throws IOException {
        String json = """
            {
                "query": "ROW x = 1",
                "tables": {
                    "t1": {
                        "a": {"long": [1]},
                        "b": {"long": [1]},
                        "c": {"keyword": [1]},
                        "d": {"long": [1]}
                    },
                    "t2": {
                        "a": {"long": [1]},
                        "b": {"integer": [1]},
                        "c": {"long": [1]},
                        "d": {"long": [1]}
                    }
                }
            }
            """;

        EsqlQueryRequest request = parseEsqlQueryRequest(json, randomBoolean());
        assertThat(request.tables().keySet(), hasSize(2));
        Map<String, Column> t1 = request.tables().get("t1");
        assertThat(t1.get("a").type(), equalTo(DataType.LONG));
        assertThat(t1.get("b").type(), equalTo(DataType.LONG));
        assertThat(t1.get("c").type(), equalTo(DataType.KEYWORD));
        assertThat(t1.get("d").type(), equalTo(DataType.LONG));
        Map<String, Column> t2 = request.tables().get("t2");
        assertThat(t2.get("a").type(), equalTo(DataType.LONG));
        assertThat(t2.get("b").type(), equalTo(DataType.INTEGER));
        assertThat(t2.get("c").type(), equalTo(DataType.LONG));
        assertThat(t2.get("d").type(), equalTo(DataType.LONG));
        assertTablesOnlyValidOnSnapshot(request);
    }

    private void assertTablesOnlyValidOnSnapshot(EsqlQueryRequest request) {
        request.onSnapshotBuild(true);
        assertNull(request.validate());

        request.onSnapshotBuild(false);
        assertNotNull(request.validate());
        assertThat(request.validate().getMessage(), containsString("[tables] only allowed in snapshot builds"));
    }

    public void testTask() throws IOException {
        String query = randomAlphaOfLength(10);
        int id = randomInt();

        String requestJson = """
            {
                "query": "QUERY"
            }""".replace("QUERY", query);

        EsqlQueryRequest request = parseEsqlQueryRequestSync(requestJson);
        Task task = request.createTask(id, "transport", EsqlQueryAction.NAME, TaskId.EMPTY_TASK_ID, Map.of());
        assertThat(task.getDescription(), equalTo(query));

        String localNode = randomAlphaOfLength(2);
        TaskInfo taskInfo = task.taskInfo(localNode, true);
        String json = taskInfo.toString();
        String expected = Streams.readFully(getClass().getClassLoader().getResourceAsStream("query_task.json")).utf8ToString();
        expected = expected.replaceAll("\r\n", "\n")
            .replaceAll("\s*<\\d+>", "")
            .replaceAll("FROM test \\| STATS MAX\\(d\\) by a, b", query)
            .replaceAll("5326", Integer.toString(id))
            .replaceAll("2j8UKw1bRO283PMwDugNNg", localNode)
            .replaceAll("2023-07-31T15:46:32\\.328Z", DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(taskInfo.startTime()))
            .replaceAll("1690818392328", Long.toString(taskInfo.startTime()))
            .replaceAll("41.7ms", TimeValue.timeValueNanos(taskInfo.runningTimeNanos()).toString())
            .replaceAll("41770830", Long.toString(taskInfo.runningTimeNanos()))
            .trim();
        assertThat(json, equalTo(expected));
    }

    private List<QueryParam> randomParameters() {
        if (randomBoolean()) {
            return Collections.emptyList();
        } else {
            int len = randomIntBetween(1, 10);
            List<QueryParam> arr = new ArrayList<>(len);
            for (int i = 0; i < len; i++) {
                @SuppressWarnings("unchecked")
                Supplier<QueryParam> supplier = randomFrom(
                    () -> new QueryParam(null, randomBoolean(), DataType.BOOLEAN),
                    () -> new QueryParam(null, randomInt(), DataType.INTEGER),
                    () -> new QueryParam(null, randomLong(), DataType.LONG),
                    () -> new QueryParam(null, randomDouble(), DataType.DOUBLE),
                    () -> new QueryParam(null, null, DataType.NULL),
                    () -> new QueryParam(null, randomAlphaOfLength(10), DataType.KEYWORD)
                );
                arr.add(supplier.get());
            }
            return Collections.unmodifiableList(arr);
        }
    }

    private StringBuilder paramsString(List<QueryParam> params, boolean hasParams) {
        StringBuilder paramsString = new StringBuilder();
        if (hasParams) {
            paramsString.append(",\"params\":[");
            boolean first = true;
            for (QueryParam param : params) {
                if (first == false) {
                    paramsString.append(", ");
                }
                first = false;
                if (param.type() == DataType.KEYWORD) {
                    paramsString.append("\"");
                    paramsString.append(param.value());
                    paramsString.append("\"");
                } else if (param.type().isNumeric() || param.type() == DataType.BOOLEAN || param.type() == DataType.NULL) {
                    paramsString.append(param.value());
                }
            }
            paramsString.append("]}");
        } else {
            paramsString.append("}");
        }
        return paramsString;
    }

    private static void assertParserErrorMessage(String json, String message) {
        Exception e = expectThrows(IllegalArgumentException.class, () -> parseEsqlQueryRequestSync(json));
        assertThat(e.getMessage(), containsString(message));

        e = expectThrows(IllegalArgumentException.class, () -> parseEsqlQueryRequestAsync(json));
        assertThat(e.getMessage(), containsString(message));
    }

    static EsqlQueryRequest parseEsqlQueryRequest(String json, boolean sync) throws IOException {
        return sync ? parseEsqlQueryRequestSync(json) : parseEsqlQueryRequestAsync(json);
    }

    static EsqlQueryRequest parseEsqlQueryRequestSync(String json) throws IOException {
        var request = parseEsqlQueryRequest(json, RequestXContent::parseSync);
        assertFalse(request.async());
        return request;
    }

    static EsqlQueryRequest parseEsqlQueryRequestAsync(String json) throws IOException {
        var request = parseEsqlQueryRequest(json, RequestXContent::parseAsync);
        assertTrue(request.async());
        return request;
    }

    static EsqlQueryRequest parseEsqlQueryRequest(String json, Function<XContentParser, EsqlQueryRequest> fromXContentFunc)
        throws IOException {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        XContentParserConfiguration config = XContentParserConfiguration.EMPTY.withRegistry(
            new NamedXContentRegistry(searchModule.getNamedXContents())
        );
        try (XContentParser parser = XContentType.JSON.xContent().createParser(config, json)) {
            return fromXContentFunc.apply(parser);
        }
    }

    private static QueryBuilder randomQueryBuilder() {
        return randomFrom(
            new TermQueryBuilder(randomAlphaOfLength(5), randomAlphaOfLengthBetween(1, 10)),
            new RangeQueryBuilder(randomAlphaOfLength(5)).gt(randomIntBetween(0, 1000))
        );
    }
}

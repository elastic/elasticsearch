/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Build;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
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
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.Column;
import org.elasticsearch.xpack.esql.parser.TypedParamValue;
import org.elasticsearch.xpack.esql.version.EsqlVersion;
import org.elasticsearch.xpack.esql.version.EsqlVersionTests;
import org.elasticsearch.xpack.ql.type.DataTypes;

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
        EsqlVersion esqlVersion = randomFrom(EsqlVersion.values());

        List<TypedParamValue> params = randomParameters();
        boolean hasParams = params.isEmpty() == false;
        StringBuilder paramsString = paramsString(params, hasParams);
        String json = String.format(Locale.ROOT, """
            {
                "version": "%s",
                "query": "%s",
                "columnar": %s,
                "locale": "%s",
                "filter": %s
                %s""", esqlVersion, query, columnar, locale.toLanguageTag(), filter, paramsString);

        EsqlQueryRequest request = parseEsqlQueryRequestSync(json);

        assertEquals(esqlVersion.toString(), request.esqlVersion());
        assertEquals(query, request.query());
        assertEquals(columnar, request.columnar());
        assertEquals(locale.toLanguageTag(), request.locale().toLanguageTag());
        assertEquals(locale, request.locale());
        assertEquals(filter, request.filter());

        assertEquals(params.size(), request.params().size());
        for (int i = 0; i < params.size(); i++) {
            assertEquals(params.get(i), request.params().get(i));
        }
    }

    public void testParseFieldsForAsync() throws IOException {
        String query = randomAlphaOfLengthBetween(1, 100);
        boolean columnar = randomBoolean();
        Locale locale = randomLocale(random());
        QueryBuilder filter = randomQueryBuilder();
        EsqlVersion esqlVersion = randomFrom(EsqlVersion.values());

        List<TypedParamValue> params = randomParameters();
        boolean hasParams = params.isEmpty() == false;
        StringBuilder paramsString = paramsString(params, hasParams);
        boolean keepOnCompletion = randomBoolean();
        TimeValue waitForCompletion = randomTimeValue();
        TimeValue keepAlive = randomTimeValue();
        String json = String.format(
            Locale.ROOT,
            """
                {
                    "version": "%s",
                    "query": "%s",
                    "columnar": %s,
                    "locale": "%s",
                    "filter": %s,
                    "keep_on_completion": %s,
                    "wait_for_completion_timeout": "%s",
                    "keep_alive": "%s"
                    %s""",
            esqlVersion,
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

        assertEquals(esqlVersion.toString(), request.esqlVersion());
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
            assertEquals(params.get(i), request.params().get(i));
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

    public void testKnownStableVersionIsValid() throws IOException {
        for (EsqlVersion version : EsqlVersion.values()) {
            if (version == EsqlVersion.SNAPSHOT) {
                // Not stable, skip. Also avoids breaking the CI as this is invalid for non-SNAPSHOT builds.
                continue;
            }

            String validVersionString = randomBoolean() ? version.versionStringWithoutEmoji() : version.toString();

            String json = String.format(Locale.ROOT, """
                {
                    "version": "%s",
                    "query": "ROW x = 1"
                }
                """, validVersionString);

            EsqlQueryRequest request = parseEsqlQueryRequest(json, randomBoolean());
            assertNull(request.validate());

            request = parseEsqlQueryRequestAsync(json);
            assertNull(request.validate());
        }
    }

    public void testUnknownVersionIsNotValid() throws IOException {
        String invalidVersionString = EsqlVersionTests.randomInvalidVersionString();

        String json = String.format(Locale.ROOT, """
            {
                "version": "%s",
                "query": "ROW x = 1"
            }
            """, invalidVersionString);

        EsqlQueryRequest request = parseEsqlQueryRequest(json, randomBoolean());
        assertNotNull(request.validate());
        assertThat(
            request.validate().getMessage(),
            containsString(
                "[version] has invalid value ["
                    + invalidVersionString
                    + "], latest available version is ["
                    + EsqlVersion.latestReleased().versionStringWithoutEmoji()
                    + "]"
            )
        );
    }

    public void testSnapshotVersionIsOnlyValidOnSnapshot() throws IOException {
        String esqlVersion = randomBoolean() ? "snapshot" : "snapshot.📷";
        String json = String.format(Locale.ROOT, """
            {
                "version": "%s",
                "query": "ROW x = 1"
            }
            """, esqlVersion);
        EsqlQueryRequest request = parseEsqlQueryRequest(json, randomBoolean());

        String errorOnNonSnapshotBuilds = "[version] with value ["
            + esqlVersion
            + "] only allowed in snapshot builds, latest available version is ["
            + EsqlVersion.latestReleased().versionStringWithoutEmoji()
            + "]";

        if (Build.current().isSnapshot()) {
            assertNull(request.validate());
        } else {
            assertNotNull(request.validate());
            assertThat(request.validate().getMessage(), containsString(errorOnNonSnapshotBuilds));
        }

        request.onSnapshotBuild(true);
        assertNull(request.validate());

        request.onSnapshotBuild(false);
        assertNotNull(request.validate());
        assertThat(request.validate().getMessage(), containsString(errorOnNonSnapshotBuilds));
    }

    public void testMissingVersionIsNotValid() throws IOException {
        String missingVersion = randomBoolean() ? "" : ", \"version\": \"\"";
        String json = String.format(Locale.ROOT, """
            {
                "columnar": true,
                "query": "row x = 1"
                %s
            }""", missingVersion);

        EsqlQueryRequest request = parseEsqlQueryRequest(json, randomBoolean());
        assertNotNull(request.validate());
        assertThat(
            request.validate().getMessage(),
            containsString(
                "[version] is required, latest available version is [" + EsqlVersion.latestReleased().versionStringWithoutEmoji() + "]"
            )
        );
    }

    public void testMissingQueryIsNotValid() throws IOException {
        String json = """
            {
                "columnar": true,
                "version": "snapshot"
            }""";
        EsqlQueryRequest request = parseEsqlQueryRequest(json, randomBoolean());
        assertNotNull(request.validate());
        assertThat(request.validate().getMessage(), containsString("[query] is required"));
    }

    public void testPragmasOnlyValidOnSnapshot() throws IOException {
        String json = """
            {
                "version": "2024.04.01",
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
    }

    public void testTablesKeyword() throws IOException {
        String json = """
            {
                "version": "2024.04.01",
                "query": "ROW x = 1",
                "tables": {"a": {"c:keyword": ["a", "b", null, 1, 2.0, ["c", "d"], false]}}
            }
            """;
        EsqlQueryRequest request = parseEsqlQueryRequest(json, randomBoolean());
        Column c = request.tables().get("a").get("c");
        assertThat(c.type(), equalTo(DataTypes.KEYWORD));
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
                "version": "2024.04.01",
                "query": "ROW x = 1",
                "tables": {"a": {"c:integer": [1, 2, "3", null, [5, 6]]}}
            }
            """;

        EsqlQueryRequest request = parseEsqlQueryRequest(json, randomBoolean());
        Column c = request.tables().get("a").get("c");
        assertThat(c.type(), equalTo(DataTypes.INTEGER));
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
                "version": "2024.04.01",
                "query": "ROW x = 1",
                "tables": {"a": {"c:long": [1, 2, "3", null, [5, 6]]}}
            }
            """;

        EsqlQueryRequest request = parseEsqlQueryRequest(json, randomBoolean());
        Column c = request.tables().get("a").get("c");
        assertThat(c.type(), equalTo(DataTypes.LONG));
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

    public void testManyTables() throws IOException {
        String json = """
            {
                "version": "2024.04.01",
                "query": "ROW x = 1",
                "tables": {
                    "t1": {
                        "a:long": [1],
                        "b:long": [1],
                        "c:keyword": [1],
                        "d:long": [1]
                    },
                    "t2": {
                        "a:long": [1],
                        "b:integer": [1],
                        "c:long": [1],
                        "d:long": [1]
                    }
                }
            }
            """;

        EsqlQueryRequest request = parseEsqlQueryRequest(json, randomBoolean());
        assertThat(request.tables().keySet(), hasSize(2));
        Map<String, Column> t1 = request.tables().get("t1");
        assertThat(t1.get("a").type(), equalTo(DataTypes.LONG));
        assertThat(t1.get("b").type(), equalTo(DataTypes.LONG));
        assertThat(t1.get("c").type(), equalTo(DataTypes.KEYWORD));
        assertThat(t1.get("d").type(), equalTo(DataTypes.LONG));
        Map<String, Column> t2 = request.tables().get("t2");
        assertThat(t2.get("a").type(), equalTo(DataTypes.LONG));
        assertThat(t2.get("b").type(), equalTo(DataTypes.INTEGER));
        assertThat(t2.get("c").type(), equalTo(DataTypes.LONG));
        assertThat(t2.get("d").type(), equalTo(DataTypes.LONG));
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

    private List<TypedParamValue> randomParameters() {
        if (randomBoolean()) {
            return Collections.emptyList();
        } else {
            int len = randomIntBetween(1, 10);
            List<TypedParamValue> arr = new ArrayList<>(len);
            for (int i = 0; i < len; i++) {
                boolean hasExplicitType = randomBoolean();
                @SuppressWarnings("unchecked")
                Supplier<TypedParamValue> supplier = randomFrom(
                    () -> new TypedParamValue("boolean", randomBoolean(), hasExplicitType),
                    () -> new TypedParamValue("integer", randomInt(), hasExplicitType),
                    () -> new TypedParamValue("long", randomLong(), hasExplicitType),
                    () -> new TypedParamValue("double", randomDouble(), hasExplicitType),
                    () -> new TypedParamValue("null", null, hasExplicitType),
                    () -> new TypedParamValue("keyword", randomAlphaOfLength(10), hasExplicitType)
                );
                arr.add(supplier.get());
            }
            return Collections.unmodifiableList(arr);
        }
    }

    private StringBuilder paramsString(List<TypedParamValue> params, boolean hasParams) {
        StringBuilder paramsString = new StringBuilder();
        if (hasParams) {
            paramsString.append(",\"params\":[");
            boolean first = true;
            for (TypedParamValue param : params) {
                if (first == false) {
                    paramsString.append(", ");
                }
                first = false;
                if (param.hasExplicitType()) {
                    paramsString.append("{\"type\":\"");
                    paramsString.append(param.type);
                    paramsString.append("\",\"value\":");
                }
                switch (param.type) {
                    case "keyword" -> {
                        paramsString.append("\"");
                        paramsString.append(param.value);
                        paramsString.append("\"");
                    }
                    case "integer", "long", "boolean", "null", "double" -> {
                        paramsString.append(param.value);
                    }
                }
                if (param.hasExplicitType()) {
                    paramsString.append("}");
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

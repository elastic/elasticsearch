/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.core.async.AsyncExecutionId;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.sql.action.Protocol;
import org.elasticsearch.xpack.sql.action.SqlQueryAction;
import org.elasticsearch.xpack.sql.action.SqlQueryTask;
import org.elasticsearch.xpack.sql.proto.Mode;
import org.elasticsearch.xpack.sql.proto.SqlVersion;
import org.elasticsearch.xpack.sql.session.SqlConfiguration;
import org.elasticsearch.xpack.sql.type.SqlDataTypes;
import org.elasticsearch.xpack.sql.util.DateUtils;

import java.time.ZoneId;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomInt;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.elasticsearch.test.ESTestCase.randomNonNegativeLong;
import static org.elasticsearch.test.ESTestCase.randomZone;

public final class SqlTestUtils {

    private SqlTestUtils() {}

    public static final SqlConfiguration TEST_CFG = new SqlConfiguration(
        DateUtils.UTC,
        null,
        Protocol.FETCH_SIZE,
        Protocol.REQUEST_TIMEOUT,
        Protocol.PAGE_TIMEOUT,
        null,
        null,
        Mode.PLAIN,
        null,
        null,
        null,
        null,
        false,
        false,
        null,
        null
    );

    public static SqlConfiguration randomConfiguration(ZoneId providedZoneId, SqlVersion sqlVersion) {
        Mode mode = randomFrom(Mode.values());
        long taskId = randomNonNegativeLong();
        return new SqlConfiguration(
            providedZoneId != null ? providedZoneId : randomZone(),
            null,
            randomIntBetween(0, 1000),
            new TimeValue(randomNonNegativeLong()),
            new TimeValue(randomNonNegativeLong()),
            null,
            null,
            mode,
            randomAlphaOfLength(10),
            sqlVersion,
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            false,
            randomBoolean(),
            new TaskId(randomAlphaOfLength(10), taskId),
            randomTask(taskId, mode, sqlVersion)
        );
    }

    public static SqlConfiguration randomConfiguration() {
        return randomConfiguration(null, null);
    }

    public static SqlConfiguration randomConfiguration(ZoneId providedZoneId) {
        return randomConfiguration(providedZoneId, null);
    }

    public static SqlConfiguration randomConfiguration(SqlVersion version) {
        return randomConfiguration(null, version);
    }

    public static SqlQueryTask randomTask(long taskId, Mode mode, SqlVersion sqlVersion) {
        return new SqlQueryTask(
            taskId,
            "transport",
            SqlQueryAction.NAME,
            "",
            null,
            emptyMap(),
            emptyMap(),
            new AsyncExecutionId("", new TaskId(randomAlphaOfLength(10), 1)),
            TimeValue.timeValueDays(5),
            mode,
            sqlVersion,
            randomBoolean()
        );
    }

    public static String randomWhitespaces() {
        StringJoiner sj = new StringJoiner("");
        for (int i = 0; i < randomInt(10); i++) {
            sj.add(randomFrom(" ", "\t", "\r", "\n"));
        }
        return sj.toString();
    }

    public static Literal literal(Object value) {
        return literal(Source.EMPTY, value);
    }

    public static Literal literal(Source source, Object value) {
        if (value instanceof Literal) {
            return (Literal) value;
        }
        return new Literal(source, value, SqlDataTypes.fromJava(value));
    }

    public static String randomOrderByAndLimit(int noOfSelectArgs, Random rnd) {
        StringBuilder sb = new StringBuilder();
        if (randomBoolean()) {
            sb.append(" ORDER BY ");

            List<Integer> shuffledArgIndices = IntStream.range(1, noOfSelectArgs + 1).boxed().collect(Collectors.toList());
            Collections.shuffle(shuffledArgIndices, rnd);
            for (int i = 0; i < noOfSelectArgs; i++) {
                sb.append(shuffledArgIndices.get(i));
                switch (randomInt(2)) {
                    case 0 -> sb.append(" DESC");
                    case 1 -> sb.append(" ASC");
                }
                switch (randomInt(2)) {
                    case 0 -> sb.append(" NULLS FIRST");
                    case 1 -> sb.append(" NULLS LAST");
                }
                if (i < noOfSelectArgs - 1) {
                    sb.append(", ");
                }
            }
        }
        if (randomBoolean()) {
            sb.append(" LIMIT ").append(randomIntBetween(1, 100));
        }
        return sb.toString();
    }
}

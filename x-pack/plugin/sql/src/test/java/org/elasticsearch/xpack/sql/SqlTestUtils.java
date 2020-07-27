/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.sql.proto.Mode;
import org.elasticsearch.xpack.sql.proto.Protocol;
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

import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomInt;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.elasticsearch.test.ESTestCase.randomNonNegativeLong;
import static org.elasticsearch.test.ESTestCase.randomZone;


public final class SqlTestUtils {

    private SqlTestUtils() {}

    public static final SqlConfiguration TEST_CFG = new SqlConfiguration(DateUtils.UTC, Protocol.FETCH_SIZE,
            Protocol.REQUEST_TIMEOUT, Protocol.PAGE_TIMEOUT, null, Mode.PLAIN,
            null, null, null, false, false);

    public static SqlConfiguration randomConfiguration() {
        return new SqlConfiguration(randomZone(),
                randomIntBetween(0,  1000),
                new TimeValue(randomNonNegativeLong()),
                new TimeValue(randomNonNegativeLong()),
                null,
                randomFrom(Mode.values()),
                randomAlphaOfLength(10),
                randomAlphaOfLength(10),
                randomAlphaOfLength(10),
                false,
                randomBoolean());
    }

    public static SqlConfiguration randomConfiguration(ZoneId providedZoneId) {
        return new SqlConfiguration(providedZoneId,
                randomIntBetween(0,  1000),
                new TimeValue(randomNonNegativeLong()),
                new TimeValue(randomNonNegativeLong()),
                null,
                randomFrom(Mode.values()),
                randomAlphaOfLength(10),
                randomAlphaOfLength(10),
                randomAlphaOfLength(10),
                false,
                randomBoolean());
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
                    case 0:
                        sb.append(" DESC");
                        break;
                    case 1:
                        sb.append(" ASC");
                        break;
                }
                switch (randomInt(2)) {
                    case 0:
                        sb.append(" NULLS FIRST");
                        break;
                    case 1:
                        sb.append(" NULLS LAST");
                        break;
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

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ql;

import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.session.Configuration;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.time.ZoneId;

import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.ESTestCase.randomZone;

public final class TestUtils {

    public static final ZoneId UTC = ZoneId.of("Z");

    public static final Configuration TEST_CFG = new Configuration(UTC, null, null);

    private TestUtils() {}

    public static Configuration randomConfiguration() {
        return new Configuration(randomZone(),
                randomAlphaOfLength(10),
                randomAlphaOfLength(10));
    }

    public static Configuration randomConfiguration(ZoneId zoneId) {
        return new Configuration(zoneId,
                randomAlphaOfLength(10),
                randomAlphaOfLength(10));
    }

    /**
     * Utility method for creating 'in-line' Literals (out of values instead of expressions).
     */
    public static Literal of(Source source, Object value) {
        if (value instanceof Literal) {
            return (Literal) value;
        }
        return new Literal(source, value, DataTypes.fromJava(value));
    }
}

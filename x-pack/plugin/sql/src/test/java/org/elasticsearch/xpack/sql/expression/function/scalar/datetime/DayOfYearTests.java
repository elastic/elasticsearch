/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.expression.Literal;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.type.DataType;

import java.time.ZoneId;

import static org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeTestUtils.dateTime;
import static org.elasticsearch.xpack.sql.util.DateUtils.UTC;

public class DayOfYearTests extends ESTestCase {

    public void testAsColumnProcessor() {
        assertEquals(1, extract(dateTime(0), UTC));
        assertEquals(1, extract(dateTime(0), ZoneId.of("GMT+01:00")));
        assertEquals(365, extract(dateTime(0), ZoneId.of("GMT-01:00")));
    }

    private Object extract(Object value, ZoneId zoneId) {
        return build(value, zoneId).asPipe().asProcessor().process(value);
    }

    private DayOfYear build(Object value, ZoneId zoneId) {
        return new DayOfYear(Source.EMPTY, new Literal(Source.EMPTY, value, DataType.DATETIME), zoneId);
    }
}

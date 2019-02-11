/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Literal;
import org.elasticsearch.xpack.sql.proto.Mode;
import org.elasticsearch.xpack.sql.proto.Protocol;
import org.elasticsearch.xpack.sql.session.Configuration;
import org.elasticsearch.xpack.sql.tree.AbstractNodeTestCase;

import java.time.ZonedDateTime;

import static org.elasticsearch.xpack.sql.tree.Source.EMPTY;

public class CurrentDateTimeTests extends AbstractNodeTestCase<CurrentDateTime, Expression> {

    public static CurrentDateTime randomCurrentDateTime() {
        return new CurrentDateTime(EMPTY, Literal.of(EMPTY, randomInt(10)),
            new Configuration(randomZone(), Protocol.FETCH_SIZE,
                Protocol.REQUEST_TIMEOUT, Protocol.PAGE_TIMEOUT, null, Mode.PLAIN, null, null, null));
    }

    @Override
    protected CurrentDateTime randomInstance() {
        return randomCurrentDateTime();
    }

    @Override
    protected CurrentDateTime copy(CurrentDateTime instance) {
        return new CurrentDateTime(instance.source(), instance.precision(), instance.configuration());
    }

    @Override
    protected CurrentDateTime mutate(CurrentDateTime instance) {
        return new CurrentDateTime(instance.source(), Literal.of(EMPTY, randomInt(10)),
            new Configuration(randomZone(), Protocol.FETCH_SIZE,
                Protocol.REQUEST_TIMEOUT, Protocol.PAGE_TIMEOUT, null, Mode.PLAIN, null, null, null));
    }

    @Override
    public void testTransform() {
    }

    @Override
    public void testReplaceChildren() {
    }

    public void testNanoPrecision() {
        ZonedDateTime zdt = ZonedDateTime.parse("2018-01-23T12:34:45.123456789Z");
        assertEquals(000_000_000, CurrentDateTime.nanoPrecision(zdt, Literal.of(EMPTY, 0)).getNano());
        assertEquals(100_000_000, CurrentDateTime.nanoPrecision(zdt, Literal.of(EMPTY, 1)).getNano());
        assertEquals(120_000_000, CurrentDateTime.nanoPrecision(zdt, Literal.of(EMPTY, 2)).getNano());
        assertEquals(123_000_000, CurrentDateTime.nanoPrecision(zdt, Literal.of(EMPTY, 3)).getNano());
        assertEquals(123_400_000, CurrentDateTime.nanoPrecision(zdt, Literal.of(EMPTY, 4)).getNano());
        assertEquals(123_450_000, CurrentDateTime.nanoPrecision(zdt, Literal.of(EMPTY, 5)).getNano());
        assertEquals(123_456_000, CurrentDateTime.nanoPrecision(zdt, Literal.of(EMPTY, 6)).getNano());
        assertEquals(123_456_700, CurrentDateTime.nanoPrecision(zdt, Literal.of(EMPTY, 7)).getNano());
        assertEquals(123_456_780, CurrentDateTime.nanoPrecision(zdt, Literal.of(EMPTY, 8)).getNano());
        assertEquals(123_456_789, CurrentDateTime.nanoPrecision(zdt, Literal.of(EMPTY, 9)).getNano());
    }
}

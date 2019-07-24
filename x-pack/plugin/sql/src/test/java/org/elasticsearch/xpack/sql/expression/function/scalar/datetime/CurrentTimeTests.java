/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.TestUtils;
import org.elasticsearch.xpack.sql.analysis.analyzer.Analyzer;
import org.elasticsearch.xpack.sql.analysis.analyzer.Verifier;
import org.elasticsearch.xpack.sql.analysis.index.EsIndex;
import org.elasticsearch.xpack.sql.analysis.index.IndexResolution;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Literal;
import org.elasticsearch.xpack.sql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.sql.parser.ParsingException;
import org.elasticsearch.xpack.sql.parser.SqlParser;
import org.elasticsearch.xpack.sql.session.Configuration;
import org.elasticsearch.xpack.sql.stats.Metrics;
import org.elasticsearch.xpack.sql.tree.AbstractNodeTestCase;
import org.elasticsearch.xpack.sql.type.TypesTests;

import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.util.Objects;

import static org.elasticsearch.xpack.sql.tree.Source.EMPTY;

public class CurrentTimeTests extends AbstractNodeTestCase<CurrentTime, Expression> {

    public static CurrentTime randomCurrentTime() {
        return new CurrentTime(EMPTY, Literal.of(EMPTY, randomInt(9)), TestUtils.randomConfiguration());
    }

    @Override
    protected CurrentTime randomInstance() {
        return randomCurrentTime();
    }

    @Override
    protected CurrentTime copy(CurrentTime instance) {
        return new CurrentTime(instance.source(), instance.precision(), instance.configuration());
    }

    @Override
    protected CurrentTime mutate(CurrentTime instance) {
        ZonedDateTime now = instance.configuration().now();
        ZoneId mutatedZoneId = randomValueOtherThanMany(o -> Objects.equals(now.getOffset(), o.getRules().getOffset(now.toInstant())),
            ESTestCase::randomZone);
        return new CurrentTime(instance.source(), Literal.of(EMPTY, randomInt(9)), TestUtils.randomConfiguration(mutatedZoneId));
    }

    @Override
    public void testTransform() {
    }

    @Override
    public void testReplaceChildren() {
    }

    public void testNanoPrecision() {
        OffsetTime ot = OffsetTime.parse("12:34:45.123456789Z");
        assertEquals(000_000_000, CurrentTime.nanoPrecision(ot, Literal.of(EMPTY, 0)).getNano());
        assertEquals(100_000_000, CurrentTime.nanoPrecision(ot, Literal.of(EMPTY, 1)).getNano());
        assertEquals(120_000_000, CurrentTime.nanoPrecision(ot, Literal.of(EMPTY, 2)).getNano());
        assertEquals(123_000_000, CurrentTime.nanoPrecision(ot, Literal.of(EMPTY, 3)).getNano());
        assertEquals(123_400_000, CurrentTime.nanoPrecision(ot, Literal.of(EMPTY, 4)).getNano());
        assertEquals(123_450_000, CurrentTime.nanoPrecision(ot, Literal.of(EMPTY, 5)).getNano());
        assertEquals(123_456_000, CurrentTime.nanoPrecision(ot, Literal.of(EMPTY, 6)).getNano());
        assertEquals(123_456_700, CurrentTime.nanoPrecision(ot, Literal.of(EMPTY, 7)).getNano());
        assertEquals(123_456_780, CurrentTime.nanoPrecision(ot, Literal.of(EMPTY, 8)).getNano());
        assertEquals(123_456_789, CurrentTime.nanoPrecision(ot, Literal.of(EMPTY, 9)).getNano());
    }

    public void testDefaultPrecision() {
        Configuration configuration = TestUtils.randomConfiguration();
        // null precision means default precision
        CurrentTime ct = new CurrentTime(EMPTY, null, configuration);
        ZonedDateTime now = configuration.now();
        assertEquals(now.get(ChronoField.MILLI_OF_SECOND), ((OffsetTime) ct.fold()).get(ChronoField.MILLI_OF_SECOND));

        OffsetTime ot = OffsetTime.parse("12:34:56.123456789Z");
        assertEquals(123_000_000, CurrentTime.nanoPrecision(ot, null).getNano());
    }

    public void testInvalidPrecision() {
        SqlParser parser = new SqlParser();
        IndexResolution indexResolution = IndexResolution.valid(new EsIndex("test",
            TypesTests.loadMapping("mapping-multi-field-with-nested.json")));

        Analyzer analyzer = new Analyzer(TestUtils.TEST_CFG, new FunctionRegistry(), indexResolution, new Verifier(new Metrics()));
        ParsingException e = expectThrows(ParsingException.class, () ->
            analyzer.analyze(parser.createStatement("SELECT CURRENT_TIME(100000000000000)"), true));
        assertEquals("line 1:22: invalid precision; [100000000000000] out of [integer] range", e.getMessage());

        e = expectThrows(ParsingException.class, () ->
            analyzer.analyze(parser.createStatement("SELECT CURRENT_TIME(100)"), true));
        assertEquals("line 1:22: precision needs to be between [0-9], received [100]", e.getMessage());
    }
}

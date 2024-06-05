/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.index.IndexResolution;
import org.elasticsearch.xpack.ql.session.Configuration;
import org.elasticsearch.xpack.ql.tree.AbstractNodeTestCase;
import org.elasticsearch.xpack.sql.SqlTestUtils;
import org.elasticsearch.xpack.sql.analysis.analyzer.Analyzer;
import org.elasticsearch.xpack.sql.parser.ParsingException;
import org.elasticsearch.xpack.sql.parser.SqlParser;
import org.elasticsearch.xpack.sql.types.SqlTypesTests;

import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.util.Objects;

import static org.elasticsearch.xpack.ql.tree.Source.EMPTY;
import static org.elasticsearch.xpack.sql.SqlTestUtils.literal;
import static org.elasticsearch.xpack.sql.analysis.analyzer.AnalyzerTestUtils.analyzer;

public class CurrentTimeTests extends AbstractNodeTestCase<CurrentTime, Expression> {

    public static CurrentTime randomCurrentTime() {
        return new CurrentTime(EMPTY, literal(randomInt(9)), SqlTestUtils.randomConfiguration());
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
        ZoneId mutatedZoneId = randomValueOtherThanMany(
            o -> Objects.equals(now.getOffset(), o.getRules().getOffset(now.toInstant())),
            ESTestCase::randomZone
        );
        return new CurrentTime(instance.source(), literal(randomInt(9)), SqlTestUtils.randomConfiguration(mutatedZoneId));
    }

    @Override
    public void testTransform() {}

    @Override
    public void testReplaceChildren() {}

    public void testNanoPrecision() {
        OffsetTime ot = OffsetTime.parse("12:34:45.123456789Z");
        assertEquals(000_000_000, CurrentTime.nanoPrecision(ot, literal(0)).getNano());
        assertEquals(100_000_000, CurrentTime.nanoPrecision(ot, literal(1)).getNano());
        assertEquals(120_000_000, CurrentTime.nanoPrecision(ot, literal(2)).getNano());
        assertEquals(123_000_000, CurrentTime.nanoPrecision(ot, literal(3)).getNano());
        assertEquals(123_400_000, CurrentTime.nanoPrecision(ot, literal(4)).getNano());
        assertEquals(123_450_000, CurrentTime.nanoPrecision(ot, literal(5)).getNano());
        assertEquals(123_456_000, CurrentTime.nanoPrecision(ot, literal(6)).getNano());
        assertEquals(123_456_700, CurrentTime.nanoPrecision(ot, literal(7)).getNano());
        assertEquals(123_456_780, CurrentTime.nanoPrecision(ot, literal(8)).getNano());
        assertEquals(123_456_789, CurrentTime.nanoPrecision(ot, literal(9)).getNano());
    }

    public void testDefaultPrecision() {
        Configuration configuration = SqlTestUtils.randomConfiguration();
        // null precision means default precision
        CurrentTime ct = new CurrentTime(EMPTY, null, configuration);
        ZonedDateTime now = configuration.now();
        assertEquals(now.get(ChronoField.MILLI_OF_SECOND), ((OffsetTime) ct.fold()).get(ChronoField.MILLI_OF_SECOND));

        OffsetTime ot = OffsetTime.parse("12:34:56.123456789Z");
        assertEquals(123_000_000, CurrentTime.nanoPrecision(ot, null).getNano());
    }

    public void testInvalidPrecision() {
        SqlParser parser = new SqlParser();
        IndexResolution indexResolution = IndexResolution.valid(
            new EsIndex("test", SqlTypesTests.loadMapping("mapping-multi-field-with-nested.json"))
        );

        Analyzer analyzer = analyzer(indexResolution);
        ParsingException e = expectThrows(
            ParsingException.class,
            () -> analyzer.analyze(parser.createStatement("SELECT CURRENT_TIME(100000000000000)"), true)
        );
        assertEquals("line 1:22: invalid precision; [100000000000000] out of [integer] range", e.getMessage());

        e = expectThrows(ParsingException.class, () -> analyzer.analyze(parser.createStatement("SELECT CURRENT_TIME(100)"), true));
        assertEquals("line 1:22: precision needs to be between [0-9], received [100]", e.getMessage());
    }
}

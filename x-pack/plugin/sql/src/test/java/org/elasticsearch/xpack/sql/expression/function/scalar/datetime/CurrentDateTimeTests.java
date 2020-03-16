/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
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
import org.elasticsearch.xpack.sql.analysis.analyzer.Verifier;
import org.elasticsearch.xpack.sql.expression.function.SqlFunctionRegistry;
import org.elasticsearch.xpack.sql.parser.ParsingException;
import org.elasticsearch.xpack.sql.parser.SqlParser;
import org.elasticsearch.xpack.sql.stats.Metrics;
import org.elasticsearch.xpack.sql.types.SqlTypesTests;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.util.Objects;

import static org.elasticsearch.xpack.ql.tree.Source.EMPTY;
import static org.elasticsearch.xpack.sql.SqlTestUtils.literal;

public class CurrentDateTimeTests extends AbstractNodeTestCase<CurrentDateTime, Expression> {

    public static CurrentDateTime randomCurrentDateTime() {
        return new CurrentDateTime(EMPTY, literal(randomInt(9)), SqlTestUtils.randomConfiguration());
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
        ZonedDateTime now = instance.configuration().now();
        ZoneId mutatedZoneId = randomValueOtherThanMany(o -> Objects.equals(now.getOffset(), o.getRules().getOffset(now.toInstant())),
            ESTestCase::randomZone);
        return new CurrentDateTime(instance.source(), literal(randomInt(9)), SqlTestUtils.randomConfiguration(mutatedZoneId));
    }

    @Override
    public void testTransform() {
    }

    @Override
    public void testReplaceChildren() {
    }

    public void testNanoPrecision() {
        ZonedDateTime zdt = ZonedDateTime.parse("2018-01-23T12:34:45.123456789Z");
        assertEquals(000_000_000, CurrentDateTime.nanoPrecision(zdt, literal(0)).getNano());
        assertEquals(100_000_000, CurrentDateTime.nanoPrecision(zdt, literal(1)).getNano());
        assertEquals(120_000_000, CurrentDateTime.nanoPrecision(zdt, literal(2)).getNano());
        assertEquals(123_000_000, CurrentDateTime.nanoPrecision(zdt, literal(3)).getNano());
        assertEquals(123_400_000, CurrentDateTime.nanoPrecision(zdt, literal(4)).getNano());
        assertEquals(123_450_000, CurrentDateTime.nanoPrecision(zdt, literal(5)).getNano());
        assertEquals(123_456_000, CurrentDateTime.nanoPrecision(zdt, literal(6)).getNano());
        assertEquals(123_456_700, CurrentDateTime.nanoPrecision(zdt, literal(7)).getNano());
        assertEquals(123_456_780, CurrentDateTime.nanoPrecision(zdt, literal(8)).getNano());
        assertEquals(123_456_789, CurrentDateTime.nanoPrecision(zdt, literal(9)).getNano());
    }
    
    public void testDefaultPrecision() {
        Configuration configuration = SqlTestUtils.randomConfiguration();
        // null precision means default precision
        CurrentDateTime cdt = new CurrentDateTime(EMPTY, null, configuration);
        ZonedDateTime now = configuration.now();
        assertEquals(now.get(ChronoField.MILLI_OF_SECOND), ((ZonedDateTime) cdt.fold()).get(ChronoField.MILLI_OF_SECOND));
        
        ZonedDateTime zdt = ZonedDateTime.parse("2019-02-26T12:34:56.123456789Z");
        assertEquals(123_000_000, CurrentDateTime.nanoPrecision(zdt, null).getNano());
    }

    public void testInvalidPrecision() {
        SqlParser parser = new SqlParser();
        IndexResolution indexResolution = IndexResolution.valid(new EsIndex("test",
                SqlTypesTests.loadMapping("mapping-multi-field-with-nested.json")));

        Analyzer analyzer = new Analyzer(SqlTestUtils.TEST_CFG, new SqlFunctionRegistry(), indexResolution, new Verifier(new Metrics()));
        ParsingException e = expectThrows(ParsingException.class, () ->
            analyzer.analyze(parser.createStatement("SELECT CURRENT_TIMESTAMP(100000000000000)"), true));
        assertEquals("line 1:27: invalid precision; [100000000000000] out of [integer] range", e.getMessage());

        e = expectThrows(ParsingException.class, () ->
            analyzer.analyze(parser.createStatement("SELECT CURRENT_TIMESTAMP(100)"), true));
        assertEquals("line 1:27: precision needs to be between [0-9], received [100]", e.getMessage());
    }
}

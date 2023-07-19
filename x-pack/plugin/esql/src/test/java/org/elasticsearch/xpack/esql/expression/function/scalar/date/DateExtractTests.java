/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.expression.function.scalar.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.hamcrest.Matcher;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class DateExtractTests extends AbstractScalarFunctionTestCase {

    public void testAllChronoFields() {
        long epochMilli = 1687944333123L;
        ZonedDateTime date = Instant.ofEpochMilli(epochMilli).atZone(EsqlTestUtils.TEST_CFG.zoneId());
        for (ChronoField value : ChronoField.values()) {
            DateExtract instance = new DateExtract(
                Source.EMPTY,
                new Literal(Source.EMPTY, epochMilli, DataTypes.DATETIME),
                new Literal(Source.EMPTY, new BytesRef(value.name()), DataTypes.KEYWORD),
                EsqlTestUtils.TEST_CFG
            );

            assertThat(instance.fold(), is(date.getLong(value)));
            assertThat(
                DateExtract.process(epochMilli, new BytesRef(value.name()), EsqlTestUtils.TEST_CFG.zoneId()),
                is(date.getLong(value))
            );
        }
    }

    @Override
    protected List<Object> simpleData() {
        return List.of(1687944333000L, new BytesRef("YEAR"));
    }

    @Override
    protected Expression expressionForSimpleData() {
        return new DateExtract(Source.EMPTY, field("date", DataTypes.DATETIME), field("field", DataTypes.KEYWORD), EsqlTestUtils.TEST_CFG);
    }

    @Override
    protected Matcher<Object> resultMatcher(List<Object> data, DataType dataType) {
        return equalTo(2023L);
    }

    @Override
    protected String expectedEvaluatorSimpleToString() {
        return "DateExtractEvaluator[value=Attribute[channel=0], chronoField=Attribute[channel=1], zone=Z]";
    }

    @Override
    protected Expression constantFoldable(List<Object> data) {
        return new DateExtract(
            Source.EMPTY,
            new Literal(Source.EMPTY, data.get(0), DataTypes.DATETIME),
            new Literal(Source.EMPTY, data.get(1), DataTypes.KEYWORD),
            EsqlTestUtils.TEST_CFG
        );
    }

    @Override
    protected Expression build(Source source, List<Literal> args) {
        return new DateExtract(source, args.get(0), args.get(1), EsqlTestUtils.TEST_CFG);
    }

    @Override
    protected List<ArgumentSpec> argSpec() {
        return List.of(required(DataTypes.DATETIME), required(strings()));
    }

    @Override
    protected DataType expectedType(List<DataType> argTypes) {
        return DataTypes.LONG;
    }
}

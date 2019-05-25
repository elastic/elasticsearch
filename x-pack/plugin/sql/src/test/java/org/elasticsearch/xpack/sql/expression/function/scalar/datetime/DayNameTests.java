/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.expression.FieldAttribute;
import org.elasticsearch.xpack.sql.expression.Literal;
import org.elasticsearch.xpack.sql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.expression.literal.IntervalDayTime;
import org.elasticsearch.xpack.sql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.EsField;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;

import static org.elasticsearch.xpack.sql.tree.Source.EMPTY;
import static org.elasticsearch.xpack.sql.type.DataType.INTERVAL_DAY;
import static org.elasticsearch.xpack.sql.util.DateUtils.UTC;

public class DayNameTests extends ESTestCase {

    public void testScalarFunctionAsScript() {

        EsField timestampField = new EsField("timestamp", DataType.DATETIME, Collections.emptyMap(), true);

        FieldAttribute timestampAttribute = new FieldAttribute(EMPTY, "timestamp", timestampField);
        Literal oneDay = new Literal(Source.EMPTY, new IntervalDayTime(
            Duration.of(1, ChronoUnit.DAYS), INTERVAL_DAY),
            DataType.INTERVAL_DAY);

        Add added = new Add(Source.EMPTY, timestampAttribute, oneDay);

        DayName dayName = new DayName(Source.EMPTY, added, UTC);

        ScriptTemplate script = dayName.asScript();
        assertEquals(
            "InternalSqlScriptUtils.dayName(InternalSqlScriptUtils.add("+
            "InternalSqlScriptUtils.docValue(doc,params.%s),InternalSqlScriptUtils.intervalDayTime("+
            "params.%s,params.%s)), params.%s)",
            script.template()
        );

        assertEquals("[{v=timestamp}, {v=PT24H}, {v=INTERVAL_DAY}, {v=Z}]", script.params().toString());
    }
}





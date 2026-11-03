/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.index.FieldSemantics.Role;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class DescribeTests extends ESTestCase {

    private static final Attribute COLUMN = new ReferenceAttribute(Source.EMPTY, null, "network.bytes", DataType.LONG);

    public void testCounterUsageDependsOnTheSourceCommand() {
        assertThat(
            Describe.usage(Role.METRIC, "counter", true),
            equalTo("STATS SUM(RATE(<field>)) or SUM(INCREASE(<field>)); never SUM over time")
        );
        assertThat(Describe.usage(Role.METRIC, "counter", false), containsString("use TS"));
    }

    public void testGaugeUsage() {
        assertThat(Describe.usage(Role.METRIC, "gauge", true), containsString("LAST_OVER_TIME(<field>)"));
        assertThat(Describe.usage(Role.METRIC, "gauge", false), containsString("do not SUM across time"));
    }

    public void testUsageNamesThePlaceholderNotTheColumn() {
        // So that the hint is the same for every column of a kind and DESCRIBE | STATS ... BY usage groups them.
        for (Role role : Role.values()) {
            for (boolean timeSeries : new boolean[] { true, false }) {
                String usage = Describe.usage(role, "counter", timeSeries);
                if (usage != null) {
                    assertThat(usage, not(containsString(COLUMN.name())));
                }
            }
        }
    }

    public void testTextIsForFiltering() {
        assertThat(Describe.usage(Role.TEXT, null, false), equalTo("WHERE MATCH(<field>, \"<text>\"); not groupable"));
    }

    public void testNoHintUnlessDeclaredOrSettledByTheType() {
        // A number may be a measure or an identifier, a keyword a category or a key: the mapping does not say.
        assertThat(Describe.usage(Role.NUMERIC, null, true), nullValue());
        assertThat(Describe.usage(Role.ATTRIBUTE, null, true), nullValue());
        assertThat("the time is only certain under TS", Describe.usage(Role.TIME, null, false), nullValue());
        assertThat(Describe.usage(Role.TIME, null, true), equalTo("BY TBUCKET(<field>, <interval>)"));
        assertThat(Describe.usage(Role.MIXED, null, false), nullValue());
        assertThat(Describe.usage(null, null, false), nullValue());
    }
}

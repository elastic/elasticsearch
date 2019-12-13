/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.AbstractNodeTestCase;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.sql.TestUtils;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Objects;

public class CurrentDateTests extends AbstractNodeTestCase<CurrentDate, Expression> {

    public static CurrentDate randomCurrentDate() {
        return new CurrentDate(Source.EMPTY, TestUtils.randomConfiguration());
    }

    @Override
    protected CurrentDate randomInstance() {
        return randomCurrentDate();
    }

    @Override
    protected CurrentDate copy(CurrentDate instance) {
        return new CurrentDate(instance.source(), instance.configuration());
    }

    @Override
    protected CurrentDate mutate(CurrentDate instance) {
        ZonedDateTime now = instance.configuration().now();
        ZoneId mutatedZoneId = randomValueOtherThanMany(o -> Objects.equals(now.getOffset(), o.getRules().getOffset(now.toInstant())),
                () -> randomZone());
        return new CurrentDate(instance.source(), TestUtils.randomConfiguration(mutatedZoneId));
    }

    @Override
    public void testTransform() {
    }

    @Override
    public void testReplaceChildren() {
    }
}

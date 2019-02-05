/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.proto.Mode;
import org.elasticsearch.xpack.sql.proto.Protocol;
import org.elasticsearch.xpack.sql.session.Configuration;
import org.elasticsearch.xpack.sql.tree.AbstractNodeTestCase;
import org.elasticsearch.xpack.sql.tree.Source;

public class CurrentDateTests extends AbstractNodeTestCase<CurrentDate, Expression> {

    public static CurrentDate randomCurrentDate() {
        return new CurrentDate(Source.EMPTY, new Configuration(randomZone(), Protocol.FETCH_SIZE,
            Protocol.REQUEST_TIMEOUT, Protocol.PAGE_TIMEOUT, null, Mode.PLAIN, null, null, null));
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
        return new CurrentDate(instance.source(), new Configuration(randomZone(), Protocol.FETCH_SIZE,
            Protocol.REQUEST_TIMEOUT, Protocol.PAGE_TIMEOUT, null, Mode.PLAIN, null, null, null));
    }

    @Override
    public void testTransform() {
    }

    @Override
    public void testReplaceChildren() {
    }
}

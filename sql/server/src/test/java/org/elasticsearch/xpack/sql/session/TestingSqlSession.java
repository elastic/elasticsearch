/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.session;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.analysis.index.GetIndexResult;
import org.elasticsearch.xpack.sql.session.SqlSession.SessionContext;

public class TestingSqlSession {

    public static SessionContext ctx(GetIndexResult getIndexResult) {
        Configuration cfg = new Configuration(ESTestCase.randomDateTimeZone(), ESTestCase.between(1, 100),
                TimeValue.parseTimeValue(ESTestCase.randomPositiveTimeValue(), "test-random"),
                TimeValue.parseTimeValue(ESTestCase.randomPositiveTimeValue(), "test-random"), null);
        return new SessionContext(cfg, getIndexResult);
    }

    public static void setCurrentContext(SessionContext ctx) {
        assert SqlSession.CURRENT_CONTEXT.get() == null;
        SqlSession.CURRENT_CONTEXT.set(ctx);
    }

    public static void removeCurrentContext() {
        SqlSession.CURRENT_CONTEXT.remove();
    }
}

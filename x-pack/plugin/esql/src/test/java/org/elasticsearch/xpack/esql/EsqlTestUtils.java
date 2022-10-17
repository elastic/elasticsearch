/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.esql.plan.logical.LocalRelation;
import org.elasticsearch.xpack.esql.session.EmptyExecutable;
import org.elasticsearch.xpack.esql.session.EsqlConfiguration;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DateUtils;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.ql.TestUtils.of;

public final class EsqlTestUtils {

    public static final EsqlConfiguration TEST_CFG = new EsqlConfiguration(DateUtils.UTC, null, null, s -> emptyList(), Settings.EMPTY);

    private EsqlTestUtils() {}

    public static Literal L(Object value) {
        return of(value);
    }

    public static LogicalPlan emptySource() {
        return new LocalRelation(Source.EMPTY, new EmptyExecutable(emptyList()));
    }
}

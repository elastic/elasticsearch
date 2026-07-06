/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.plan.EsqlStatement;
import org.elasticsearch.xpack.esql.plan.QuerySetting;
import org.elasticsearch.xpack.esql.plan.QuerySettings;

import java.time.ZoneOffset;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.hamcrest.Matchers.equalTo;

public class EsqlSessionTests extends ESTestCase {

    public void testSuppliedSettingNamesCountsBothSurfaces() {
        // A setting supplied via the request body and a different one via in-query SET are both counted.
        EsqlQueryRequest request = EsqlQueryRequest.syncEsqlQueryRequest(null);
        request.set(QuerySettings.TIME_ZONE, ZoneOffset.UTC);
        QuerySetting projectRouting = new QuerySetting(EMPTY, new Alias(EMPTY, "project_routing", Literal.keyword(EMPTY, "p")));
        EsqlStatement statement = new EsqlStatement(null, List.of(projectRouting));
        assertThat(EsqlSession.suppliedSettingNames(request, statement), equalTo(Set.of("time_zone", "project_routing")));
    }
}

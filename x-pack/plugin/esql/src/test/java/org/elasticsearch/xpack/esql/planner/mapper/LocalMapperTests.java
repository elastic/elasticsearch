/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner.mapper;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.eql.EqlQuery;

import static org.hamcrest.Matchers.containsString;

public class LocalMapperTests extends ESTestCase {

    /**
     * {@link EqlQuery} is coordinator-only and must never reach the data-node mapper; the guard in
     * {@link LocalMapper#map} rejects it instead of silently lowering it to a physical node that could run on a data node.
     */
    public void testEqlQueryIsRejectedOnDataNode() {
        EqlQuery eqlQuery = new EqlQuery(Source.EMPTY, "idx", "any where true");
        EsqlIllegalArgumentException e = expectThrows(EsqlIllegalArgumentException.class, () -> LocalMapper.INSTANCE.map(eqlQuery));
        assertThat(e.getMessage(), containsString("unsupported logical plan node [EqlQuery]"));
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.plan.logical.command;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.sql.SqlTestUtils;
import org.elasticsearch.xpack.sql.expression.function.SqlFunctionRegistry;
import org.elasticsearch.xpack.sql.session.SchemaRowSet;
import org.elasticsearch.xpack.sql.session.SqlSession;

import static org.elasticsearch.action.ActionListener.wrap;

public class ShowFunctionsTests extends ESTestCase {

    public void testShowFunctions() throws Exception {
        ShowFunctions showFunctions = new ShowFunctions(Source.EMPTY, null);
        SqlSession session = new SqlSession(SqlTestUtils.TEST_CFG, null, new SqlFunctionRegistry(), null, null, null, null, null, null);

        showFunctions.execute(session, wrap(p -> {
            SchemaRowSet r = (SchemaRowSet) p.rowSet();
            assertTrue(150 <= r.size());
            assertEquals(2, r.columnCount());
            assertEquals("AVG", r.column(0));
            assertEquals("AGGREGATE", r.column(1));
        }, ex -> fail(ex.getMessage())));
    }
}

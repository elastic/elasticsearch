/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

public class QualifierTests extends AbstractStatementParserTests {

    /**
     * Test that qualifiers are parsed correctly in expressions.
     * Does not check the lookup join specifically; it only serves as an example source of qualifiers for the WHERE.
     */
    public void testQualifiersReferencedInExpressions() {
        assumeTrue("Requires qualifier support", EsqlCapabilities.Cap.NAME_QUALIFIERS.isEnabled());

        LogicalPlan plan = statement("ROW x = 1 | LOOKUP JOIN lu_idx l ON x | WHERE (lu x) > 1");
        plan = statement("ROW x = 1 | LOOKUP JOIN lu_idx AS lu ON x | WHERE lu x > 1");
    }
}

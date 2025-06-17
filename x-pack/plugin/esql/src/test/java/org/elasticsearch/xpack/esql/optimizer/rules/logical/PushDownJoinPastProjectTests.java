/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;

public class PushDownJoinPastProjectTests extends AbstractLogicalPlanOptimizerTests {
    /**
     * Expects
     *
     * Project[[languages{f}#16, emp_no{f}#13, languages{f}#16 AS language_code#6, language_name{f}#27]]
     * \_Limit[1000[INTEGER],true]
     *   \_Join[LEFT,[languages{f}#16],[languages{f}#16],[language_code{f}#26]]
     *     |_Limit[1000[INTEGER],true]
     *     | \_Join[LEFT,[languages{f}#16],[languages{f}#16],[language_code{f}#24]]
     *     |   |_Limit[1000[INTEGER],false]
     *     |   | \_EsRelation[test][_meta_field{f}#19, emp_no{f}#13, first_name{f}#14, ..]
     *     |   \_EsRelation[languages_lookup][LOOKUP][language_code{f}#24]
     *     \_EsRelation[languages_lookup][LOOKUP][language_code{f}#26, language_name{f}#27]
     */
    public void testMultipleLookupProject() {
        // TODO a test case where pushing down past the RENAME would shadow
        // analogous to
        // Project[[x{f}#1, y{f}#2 as z, $$y{r}#3 as y]]
        // \_Eval[[2 * x{f}#1 as $$y]]
        assumeTrue("Requires LOOKUP JOIN", EsqlCapabilities.Cap.JOIN_LOOKUP_V12.isEnabled());

        String query = """
            FROM test
            | KEEP languages, emp_no
            | EVAL language_code = languages
            | LOOKUP JOIN languages_lookup ON language_code
            | RENAME language_name AS foo
            | LOOKUP JOIN languages_lookup ON language_code
            | DROP foo
            """;

        var plan = optimizedPlan(query);

        // TODO: here
        assert false;
    }
}

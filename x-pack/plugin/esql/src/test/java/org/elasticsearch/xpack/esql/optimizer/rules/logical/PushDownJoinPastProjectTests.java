/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.asLimit;

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

        var project = as(plan, Project.class);
        var upperLimit = asLimit(project.child(), 1000, true);

        var join1 = as(upperLimit.child(), Join.class);
        var lookupRel1 = as(join1.right(), EsRelation.class);
        var limit1 = asLimit(join1.left(), 1000, true);

        AttributeSet lookupIndexFields1 = lookupRel1.outputSet();
        var rightKeys1 = join1.config().rightFields();
        var leftKeys1 = join1.config().leftFields();
        // Left join key should be updated to use an attribute from the main index directly
        assertTrue(leftKeys1.size() == 1 && leftKeys1.get(0).name() == "languages");
        assertTrue(rightKeys1.size() == 1 && rightKeys1.get(0).name() == "language_code");
        assertTrue(lookupIndexFields1.contains(rightKeys1.get(0)));

        var join2 = as(limit1.child(), Join.class);
        var lookupRel2 = as(join2.right(), EsRelation.class);
        var limit2 = asLimit(join2.left(), 1000, false);

        AttributeSet lookupIndexFields2 = lookupRel2.outputSet();
        var rightKeys2 = join2.config().rightFields();
        var leftKeys2 = join2.config().leftFields();
        // Left join key should be updated to use an attribute from the main index directly
        assertTrue(leftKeys2.size() == 1 && leftKeys2.get(0).name() == "languages");
        assertTrue(rightKeys2.size() == 1 && rightKeys2.get(0).name() == "language_code");
        assertTrue(lookupIndexFields2.contains(rightKeys2.get(0)));

        var mainRel = as(limit2.child(), EsRelation.class);
        AttributeSet mainFields = mainRel.outputSet();
        assertTrue(mainFields.contains(leftKeys1.get(0)));
        assertTrue(mainFields.contains(leftKeys2.get(0)));
    }
}

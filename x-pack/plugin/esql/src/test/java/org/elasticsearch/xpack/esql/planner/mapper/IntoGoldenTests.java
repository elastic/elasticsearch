/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner.mapper;

import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.optimizer.GoldenTestCase;

import java.util.EnumSet;

public class IntoGoldenTests extends GoldenTestCase {
    public void testFromInto() {
        assumeTrue("INTO is only enabled in snapshot builds", EsqlCapabilities.Cap.INTO_COMMAND.isEnabled());
        // The node-reduce plan is where a second write would show up.
        builder("FROM employees | KEEP emp_no | INTO dst").stages(
            EnumSet.of(Stage.PHYSICAL_OPTIMIZATION, Stage.LOCAL_PHYSICAL_OPTIMIZATION, Stage.NODE_REDUCE)
        ).run();
    }

    public void testFromWhereFalseInto() {
        assumeTrue("INTO is only enabled in snapshot builds", EsqlCapabilities.Cap.INTO_COMMAND.isEnabled());
        builder("FROM employees | WHERE false | INTO dst").stages(EnumSet.of(Stage.LOGICAL_OPTIMIZATION, Stage.PHYSICAL_OPTIMIZATION))
            .run();
    }
}

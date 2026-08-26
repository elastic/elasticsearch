/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.optimizer.GoldenTestCase;

import java.util.EnumSet;

/** Golden tests for {@link RestoreDocAttributeForScoring}. */
public class RestoreDocAttributeForScoringGoldenTests extends GoldenTestCase {

    @ParametersFactory(argumentFormatting = "%1$s")
    public static Iterable<Object[]> parameters() {
        return goldenModes();
    }

    public RestoreDocAttributeForScoringGoldenTests(@Name("mode") String mode) {
        super(mode);
    }

    /** A missing field with {@code MV_EXPAND} leaves a projection that must retain {@code _doc}. */
    public void testMissingFieldWithScoreAcrossMvExpand() {
        runGoldenTest("""
            FROM employees
            | EVAL x = salary
            | MV_EXPAND job_positions
            | EVAL s = SCORE(MATCH(first_name, "elasticsearch"))
            """, EnumSet.of(Stage.LOCAL_PHYSICAL_OPTIMIZATION), EsqlTestUtils.statsForMissingField("salary"));
    }
}

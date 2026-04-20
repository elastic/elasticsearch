/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.xpack.esql.action.EsqlCapabilities;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

/**
 * Golden tests for query approximation.
 */
public class ApproximationGoldenTests extends GoldenTestCase {
    private static final EnumSet<Stage> STAGES = EnumSet.of(
        Stage.LOGICAL_OPTIMIZATION,
        Stage.PHYSICAL_OPTIMIZATION,
        Stage.LOCAL_PHYSICAL_OPTIMIZATION
    );

    protected List<String> filteredWarnings() {
        List<String> warnings = new ArrayList<>(super.filteredWarnings());
        warnings.add("approximation not supported");
        return warnings;
    }

    public void testApproximationDisabled() {
        runGoldenTest("""
            SET approximation=false;
            FROM many_numbers
              | STATS SUM(sv), MEDIAN(sv)
            """, STAGES);
    }

    public void testApproximationNotSupported() {
        runGoldenTest("""
            SET approximation=true;
            FROM many_numbers
            """, STAGES);
    }

    public void testPushdown() {
        runGoldenTest("""
            SET approximation=true;
            FROM many_numbers
              | WHERE sv > 100
              | STATS COUNT()
            """, STAGES);
    }

    public void testSimpleApproximation() {
        runGoldenTest("""
            SET approximation={"rows": 10000};
            FROM many_numbers
              | STATS SUM(sv), MEDIAN(sv)
            """, STAGES);
    }

    public void testCommandsBeforeAndAfterStats() {
        runGoldenTest("""
            SET approximation={};
            FROM many_numbers
              | MV_EXPAND mv
              | EVAL plus_one = mv+1
              | STATS avg=AVG(plus_one)
              | EVAL avg_squared = avg*avg
            """, STAGES);
    }

    public void testGroupedStats() {
        runGoldenTest("""
            SET approximation={"rows": 99999};
            FROM many_numbers
              | STATS COUNT(mv) BY mv
            """, STAGES);
    }

    public void testNoConfidenceInterval() {
        runGoldenTest("""
            SET approximation={"rows": 10000, "confidence_level": null};
            FROM many_numbers
              | STATS SUM(mv), COUNT(mv)
            """, STAGES);
    }

    public void testLookupJoin() {
        assumeTrue("needs approximation lookup join", EsqlCapabilities.Cap.APPROXIMATION_LOOKUP_JOIN.isEnabled());
        runGoldenTest("""
            SET approximation=true;
            FROM many_numbers
              | EVAL language_code = sv % 4 + 1
              | LOOKUP JOIN languages_lookup ON language_code
              | EVAL length = LENGTH(language_name)
              | STATS AVG(length)
            """, STAGES);
    }
}

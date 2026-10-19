/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.join;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

import java.util.List;

import static org.hamcrest.Matchers.containsString;

/** Verifies subquery equality key types independently of LOOKUP JOIN restrictions. */
public class SubqueryJoinKeyTypeTests extends ESTestCase {

    /** Metadata dictionaries are not scalar subquery equality keys. */
    public void testMetadataKeysRemainUnsupported() {
        for (Join join : joins(DataType.TIMESERIES_DATA_TYPE, DataType.TIMESERIES_DATA_TYPE)) {
            assertThat(verify(join).toString(), containsString("is not supported"));
        }
    }

    private static List<Join> joins(DataType leftType, DataType rightType) {
        var leftKey = new ReferenceAttribute(Source.EMPTY, "left_key", leftType);
        var rightKey = new ReferenceAttribute(Source.EMPTY, "right_key", rightType);
        var left = new LocalRelation(Source.EMPTY, List.of(leftKey), EmptyLocalSupplier.EMPTY);
        var right = new LocalRelation(Source.EMPTY, List.of(rightKey), EmptyLocalSupplier.EMPTY);
        List<Attribute> leftKeys = List.of(leftKey);
        List<Attribute> rightKeys = List.of(rightKey);
        return List.of(
            new InnerJoin(Source.EMPTY, left, right, leftKeys, rightKeys, List.of(), true),
            new InlineJoin(Source.EMPTY, left, right, JoinTypes.LEFT, leftKeys, rightKeys),
            new SemiJoin(Source.EMPTY, left, right, leftKeys, rightKeys),
            new AntiJoin(Source.EMPTY, left, right, leftKeys, rightKeys),
            new MarkJoin(Source.EMPTY, left, right, leftKeys, rightKeys, new ReferenceAttribute(Source.EMPTY, "mark", DataType.BOOLEAN))
        );
    }

    private static Failures verify(Join join) {
        Failures failures = new Failures();
        join.postAnalysisVerification(failures);
        return failures;
    }
}

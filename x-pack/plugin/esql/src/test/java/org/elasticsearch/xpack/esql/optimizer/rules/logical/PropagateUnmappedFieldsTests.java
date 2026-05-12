/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.PotentiallyUnmappedKeywordEsField;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinConfig;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;
import org.elasticsearch.xpack.esql.plan.logical.join.LookupJoin;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.hamcrest.Matchers.contains;

public class PropagateUnmappedFieldsTests extends ESTestCase {

    /**
     * Regression test for https://github.com/elastic/elasticsearch/issues/142026.
     * When {@code unmapped_fields="load"} is set, the primary EsRelation gets a
     * {@link PotentiallyUnmappedKeywordEsField} for the join key. PropagateUnmappedFields
     * must not inject that field into the LOOKUP EsRelation: doing so would replace the
     * lookup's attribute (different NameId) and break the join's rightFields reference,
     * causing PlanConsistencyChecker to fail with
     * "Plan optimized incorrectly due to missing references from right hand side".
     */
    public void testSkipsLookupEsRelation() {
        var messagePUK = new FieldAttribute(EMPTY, "message", new PotentiallyUnmappedKeywordEsField("message"));
        var primaryEsr = new EsRelation(EMPTY, "primary", IndexMode.STANDARD, Map.of(), Map.of(), Map.of(), List.of(messagePUK));

        var messageRegular = new FieldAttribute(
            EMPTY,
            "message",
            new EsField("message", DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );
        var lookupEsr = new EsRelation(EMPTY, "lookup", IndexMode.LOOKUP, Map.of(), Map.of(), Map.of(), List.of(messageRegular));

        var joinConfig = new JoinConfig(JoinTypes.LEFT, List.of(messagePUK), List.of(messageRegular), null);
        var join = new LookupJoin(EMPTY, primaryEsr, lookupEsr, joinConfig, false);

        var result = new PropagateUnmappedFields().apply(join);
        var resultJoin = as(result, LookupJoin.class);
        var resultLookupEsr = as(resultJoin.right(), EsRelation.class);

        // Lookup EsRelation output must be unchanged — no PUK field injected
        assertThat(resultLookupEsr.output(), contains(messageRegular));
        // Join's rightFields must still be present in the right child's outputSet
        assertTrue(resultJoin.right().outputSet().containsAll(Expressions.references(resultJoin.config().rightFields())));
    }
}

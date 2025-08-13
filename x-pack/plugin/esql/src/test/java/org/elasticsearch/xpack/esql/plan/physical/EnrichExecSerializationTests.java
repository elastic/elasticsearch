/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.FieldAttributeTests;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class EnrichExecSerializationTests extends AbstractPhysicalPlanSerializationTests<EnrichExec> {
    public static EnrichExec randomEnrichExec(int depth) {
        Source source = randomSource();
        PhysicalPlan child = randomChild(depth);
        Enrich.Mode mode = randomFrom(Enrich.Mode.values());
        String matchType = randomAlphaOfLength(3);
        NamedExpression matchField = FieldAttributeTests.createFieldAttribute(0, false);
        String policyName = randomAlphaOfLength(4);
        String policyMatchField = randomAlphaOfLength(5);
        Map<String, String> concreteIndices = randomMap(1, 4, () -> Tuple.tuple(randomAlphaOfLength(3), randomAlphaOfLength(5)));
        List<NamedExpression> enrichFields = randomFieldAttributes(1, 4, false).stream().map(f -> (NamedExpression) f).toList();
        return new EnrichExec(source, child, mode, matchType, matchField, policyName, policyMatchField, concreteIndices, enrichFields);
    }

    @Override
    protected EnrichExec createTestInstance() {
        return randomEnrichExec(0);
    }

    @Override
    protected EnrichExec mutateInstance(EnrichExec instance) throws IOException {
        PhysicalPlan child = instance.child();
        Enrich.Mode mode = instance.mode();
        String matchType = instance.matchType();
        NamedExpression matchField = instance.matchField();
        String policyName = instance.policyName();
        String policyMatchField = instance.policyMatchField();
        Map<String, String> concreteIndices = instance.concreteIndices();
        List<NamedExpression> enrichFields = instance.enrichFields();
        switch (between(0, 7)) {
            case 0 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 1 -> mode = randomValueOtherThan(mode, () -> randomFrom(Enrich.Mode.values()));
            case 2 -> matchType = randomValueOtherThan(matchType, () -> randomAlphaOfLength(3));
            case 3 -> matchField = randomValueOtherThan(matchField, () -> FieldAttributeTests.createFieldAttribute(0, false));
            case 4 -> policyName = randomValueOtherThan(policyName, () -> randomAlphaOfLength(4));
            case 5 -> policyMatchField = randomValueOtherThan(policyMatchField, () -> randomAlphaOfLength(5));
            case 6 -> concreteIndices = randomValueOtherThan(
                concreteIndices,
                () -> randomMap(1, 4, () -> Tuple.tuple(randomAlphaOfLength(3), randomAlphaOfLength(5)))
            );
            case 7 -> enrichFields = randomValueOtherThan(
                enrichFields,
                () -> randomFieldAttributes(1, 4, false).stream().map(f -> (NamedExpression) f).toList()
            );
        }
        return new EnrichExec(
            instance.source(),
            child,
            mode,
            matchType,
            matchField,
            policyName,
            policyMatchField,
            concreteIndices,
            enrichFields
        );
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}

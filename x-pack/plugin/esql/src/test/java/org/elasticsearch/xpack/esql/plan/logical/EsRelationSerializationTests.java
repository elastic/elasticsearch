/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.index.EsIndexSerializationTests;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.index.EsIndexSerializationTests.randomIndexNameWithModes;

public class EsRelationSerializationTests extends AbstractLogicalPlanSerializationTests<EsRelation> {
    public static EsRelation randomEsRelation() {
        return new EsRelation(
            randomSource(),
            randomIdentifier(),
            randomFrom(IndexMode.values()),
            randomIndexNameWithModes(),
            randomFieldAttributes(0, 10, false)
        );
    }

    @Override
    protected EsRelation createTestInstance() {
        return randomEsRelation();
    }

    @Override
    protected EsRelation mutateInstance(EsRelation instance) throws IOException {
        String indexPattern = instance.indexPattern();
        IndexMode indexMode = instance.indexMode();
        Map<String, IndexMode> indexNameWithModes = instance.indexNameWithModes();
        List<Attribute> attributes = instance.output();
        switch (between(0, 3)) {
            case 0 -> indexPattern = randomValueOtherThan(indexPattern, ESTestCase::randomIdentifier);
            case 1 -> indexMode = randomValueOtherThan(indexMode, () -> randomFrom(IndexMode.values()));
            case 2 -> indexNameWithModes = randomValueOtherThan(indexNameWithModes, EsIndexSerializationTests::randomIndexNameWithModes);
            case 3 -> attributes = randomValueOtherThan(attributes, () -> randomFieldAttributes(0, 10, false));
            default -> throw new IllegalArgumentException();
        }
        return new EsRelation(instance.source(), indexPattern, indexMode, indexNameWithModes, attributes);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}

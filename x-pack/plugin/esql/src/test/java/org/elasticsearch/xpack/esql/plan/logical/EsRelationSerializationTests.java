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
import org.elasticsearch.xpack.esql.index.EsIndexGenerator;
import org.elasticsearch.xpack.esql.index.IndexProperties;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.index.EsIndexGenerator.randomIndexProperties;
import static org.elasticsearch.xpack.esql.index.EsIndexGenerator.randomRemotesWithIndices;

public class EsRelationSerializationTests extends AbstractLogicalPlanSerializationTests<EsRelation> {
    public static EsRelation randomEsRelation() {
        return new EsRelation(
            randomSource(),
            randomIdentifier(),
            randomFrom(IndexMode.availableModes()),
            randomRemotesWithIndices(),
            randomRemotesWithIndices(),
            randomIndexProperties(),
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
        Map<String, List<String>> originalIndices = instance.originalIndices();
        Map<String, List<String>> concreteIndices = instance.concreteIndices();
        Map<String, IndexProperties> indexProperties = instance.indexProperties();
        List<Attribute> attributes = instance.output();
        switch (between(0, 5)) {
            case 0 -> indexPattern = randomValueOtherThan(indexPattern, ESTestCase::randomIdentifier);
            case 1 -> indexMode = randomValueOtherThan(indexMode, () -> randomFrom(IndexMode.availableModes()));
            case 2 -> indexProperties = randomValueOtherThan(indexProperties, EsIndexGenerator::randomIndexProperties);
            case 3 -> originalIndices = randomValueOtherThan(originalIndices, EsIndexGenerator::randomRemotesWithIndices);
            case 4 -> concreteIndices = randomValueOtherThan(concreteIndices, EsIndexGenerator::randomRemotesWithIndices);
            case 5 -> attributes = randomValueOtherThan(attributes, () -> randomFieldAttributes(0, 10, false));
            default -> throw new IllegalArgumentException();
        }
        return new EsRelation(instance.source(), indexPattern, indexMode, originalIndices, concreteIndices, indexProperties, attributes);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}

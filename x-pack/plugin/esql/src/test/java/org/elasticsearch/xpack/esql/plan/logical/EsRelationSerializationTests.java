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
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.NestedEsField;
import org.elasticsearch.xpack.esql.index.EsIndexGenerator;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.index.EsIndexGenerator.randomIndexNameWithModes;
import static org.elasticsearch.xpack.esql.index.EsIndexGenerator.randomRemotesWithIndices;

public class EsRelationSerializationTests extends AbstractLogicalPlanSerializationTests<EsRelation> {
    public static EsRelation randomEsRelation() {
        return new EsRelation(
            randomSource(),
            randomIdentifier(),
            randomFrom(IndexMode.availableModes()),
            randomRemotesWithIndices(),
            randomRemotesWithIndices(),
            randomIndexNameWithModes(),
            randomFieldAttributes(0, 10, false),
            randomNestedFields()
        );
    }

    static Map<String, NestedEsField> randomNestedFields() {
        Map<String, NestedEsField> nestedFields = new HashMap<>();
        int count = between(0, 3);
        for (int i = 0; i < count; i++) {
            Map<String, EsField> properties = new HashMap<>();
            int subs = between(1, 3);
            for (int j = 0; j < subs; j++) {
                String sub = randomIdentifier();
                properties.put(
                    sub,
                    new EsField(
                        sub,
                        randomFrom(DataType.KEYWORD, DataType.INTEGER, DataType.LONG),
                        Map.of(),
                        true,
                        EsField.TimeSeriesFieldType.NONE
                    )
                );
            }
            String name = randomIdentifier();
            nestedFields.put(name, new NestedEsField(name, properties, false, EsField.TimeSeriesFieldType.NONE));
        }
        return nestedFields;
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
        Map<String, IndexMode> indexNameWithModes = instance.indexNameWithModes();
        List<Attribute> attributes = instance.output();
        Map<String, NestedEsField> nestedFields = instance.nestedFields();
        switch (between(0, 6)) {
            case 0 -> indexPattern = randomValueOtherThan(indexPattern, ESTestCase::randomIdentifier);
            case 1 -> indexMode = randomValueOtherThan(indexMode, () -> randomFrom(IndexMode.availableModes()));
            case 2 -> indexNameWithModes = randomValueOtherThan(indexNameWithModes, EsIndexGenerator::randomIndexNameWithModes);
            case 3 -> originalIndices = randomValueOtherThan(originalIndices, EsIndexGenerator::randomRemotesWithIndices);
            case 4 -> concreteIndices = randomValueOtherThan(concreteIndices, EsIndexGenerator::randomRemotesWithIndices);
            case 5 -> attributes = randomValueOtherThan(attributes, () -> randomFieldAttributes(0, 10, false));
            case 6 -> nestedFields = randomValueOtherThan(nestedFields, EsRelationSerializationTests::randomNestedFields);
            default -> throw new IllegalArgumentException();
        }
        return new EsRelation(
            instance.source(),
            indexPattern,
            indexMode,
            originalIndices,
            concreteIndices,
            indexNameWithModes,
            attributes,
            nestedFields
        );
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}

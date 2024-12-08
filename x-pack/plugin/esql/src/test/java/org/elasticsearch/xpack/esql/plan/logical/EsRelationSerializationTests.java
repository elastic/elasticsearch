/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.EsIndexSerializationTests;

import java.io.IOException;
import java.util.List;

public class EsRelationSerializationTests extends AbstractLogicalPlanSerializationTests<EsRelation> {
    public static EsRelation randomEsRelation() {
        Source source = randomSource();
        EsIndex index = EsIndexSerializationTests.randomEsIndex();
        List<Attribute> attributes = randomFieldAttributes(0, 10, false);
        IndexMode indexMode = randomFrom(IndexMode.values());
        boolean frozen = randomBoolean();
        return new EsRelation(source, index, attributes, indexMode, frozen);
    }

    @Override
    protected EsRelation createTestInstance() {
        return randomEsRelation();
    }

    @Override
    protected EsRelation mutateInstance(EsRelation instance) throws IOException {
        EsIndex index = instance.index();
        List<Attribute> attributes = instance.output();
        IndexMode indexMode = instance.indexMode();
        boolean frozen = instance.frozen();
        switch (between(0, 3)) {
            case 0 -> index = randomValueOtherThan(index, EsIndexSerializationTests::randomEsIndex);
            case 1 -> attributes = randomValueOtherThan(attributes, () -> randomFieldAttributes(0, 10, false));
            case 2 -> indexMode = randomValueOtherThan(indexMode, () -> randomFrom(IndexMode.values()));
            case 3 -> frozen = false == frozen;
            default -> throw new IllegalArgumentException();
        }
        return new EsRelation(instance.source(), index, attributes, indexMode, frozen);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}

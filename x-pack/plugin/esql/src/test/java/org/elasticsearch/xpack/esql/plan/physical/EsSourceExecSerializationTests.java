/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.EsIndexSerializationTests;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.plan.logical.AbstractLogicalPlanSerializationTests.randomFieldAttributes;

public class EsSourceExecSerializationTests extends AbstractPhysicalPlanSerializationTests<EsSourceExec> {
    public static EsSourceExec randomEsSourceExec() {
        Source source = randomSource();
        EsIndex index = EsIndexSerializationTests.randomEsIndex();
        List<Attribute> attributes = randomFieldAttributes(1, 10, false);
        QueryBuilder query = new TermQueryBuilder(randomAlphaOfLength(5), randomAlphaOfLength(5));
        IndexMode indexMode = randomFrom(IndexMode.values());
        return new EsSourceExec(source, index, attributes, query, indexMode);
    }

    @Override
    protected EsSourceExec createTestInstance() {
        return randomEsSourceExec();
    }

    @Override
    protected EsSourceExec mutateInstance(EsSourceExec instance) throws IOException {
        EsIndex index = instance.index();
        List<Attribute> attributes = instance.output();
        QueryBuilder query = instance.query();
        IndexMode indexMode = instance.indexMode();
        switch (between(0, 3)) {
            case 0 -> index = randomValueOtherThan(index, EsIndexSerializationTests::randomEsIndex);
            case 1 -> attributes = randomValueOtherThan(attributes, () -> randomFieldAttributes(1, 10, false));
            case 2 -> query = randomValueOtherThan(query, () -> new TermQueryBuilder(randomAlphaOfLength(5), randomAlphaOfLength(5)));
            case 3 -> indexMode = randomValueOtherThan(indexMode, () -> randomFrom(IndexMode.values()));
            default -> throw new IllegalStateException();
        }
        return new EsSourceExec(instance.source(), index, attributes, query, indexMode);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}

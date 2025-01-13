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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;

import java.io.IOException;
import java.util.List;

public class EsSourceExecSerializationTests extends AbstractPhysicalPlanSerializationTests<EsSourceExec> {
    public static EsSourceExec randomEsSourceExec() {
        return new EsSourceExec(
            randomSource(),
            randomIdentifier(),
            randomFrom(IndexMode.values()),
            new TermQueryBuilder(randomAlphaOfLength(5), randomAlphaOfLength(5)),
            randomFieldAttributes(1, 10, false)
        );
    }

    @Override
    protected EsSourceExec createTestInstance() {
        return randomEsSourceExec();
    }

    @Override
    protected EsSourceExec mutateInstance(EsSourceExec instance) throws IOException {
        String indexName = instance.indexName();
        IndexMode indexMode = instance.indexMode();
        QueryBuilder query = instance.query();
        List<Attribute> attributes = instance.output();
        switch (between(0, 3)) {
            case 0 -> indexName = randomValueOtherThan(indexName, ESTestCase::randomIdentifier);
            case 1 -> indexMode = randomValueOtherThan(indexMode, () -> randomFrom(IndexMode.values()));
            case 2 -> query = randomValueOtherThan(query, () -> new TermQueryBuilder(randomAlphaOfLength(5), randomAlphaOfLength(5)));
            case 3 -> attributes = randomValueOtherThan(attributes, () -> randomFieldAttributes(1, 10, false));
            default -> throw new IllegalStateException();
        }
        return new EsSourceExec(instance.source(), indexName, indexMode, query, attributes);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}

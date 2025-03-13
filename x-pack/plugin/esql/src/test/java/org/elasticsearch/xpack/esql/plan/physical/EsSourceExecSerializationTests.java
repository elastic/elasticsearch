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
import org.elasticsearch.xpack.esql.index.EsIndexSerializationTests;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.index.EsIndexSerializationTests.randomIndexNameWithModes;

public class EsSourceExecSerializationTests extends AbstractPhysicalPlanSerializationTests<EsSourceExec> {
    public static EsSourceExec randomEsSourceExec() {
        return new EsSourceExec(
            randomSource(),
            randomIdentifier(),
            randomFrom(IndexMode.values()),
            randomIndexNameWithModes(),
            randomFieldAttributes(1, 10, false),
            new TermQueryBuilder(randomAlphaOfLength(5), randomAlphaOfLength(5))
        );
    }

    @Override
    protected EsSourceExec createTestInstance() {
        return randomEsSourceExec();
    }

    @Override
    protected EsSourceExec mutateInstance(EsSourceExec instance) throws IOException {
        String indexPattern = instance.indexPattern();
        IndexMode indexMode = instance.indexMode();
        Map<String, IndexMode> indexNameWithModes = instance.indexNameWithModes();
        List<Attribute> attributes = instance.output();
        QueryBuilder query = instance.query();
        switch (between(0, 4)) {
            case 0 -> indexPattern = randomValueOtherThan(indexPattern, ESTestCase::randomIdentifier);
            case 1 -> indexMode = randomValueOtherThan(indexMode, () -> randomFrom(IndexMode.values()));
            case 2 -> indexNameWithModes = randomValueOtherThan(indexNameWithModes, EsIndexSerializationTests::randomIndexNameWithModes);
            case 3 -> attributes = randomValueOtherThan(attributes, () -> randomFieldAttributes(1, 10, false));
            case 4 -> query = randomValueOtherThan(query, () -> new TermQueryBuilder(randomAlphaOfLength(5), randomAlphaOfLength(5)));
            default -> throw new IllegalStateException();
        }
        return new EsSourceExec(instance.source(), indexPattern, indexMode, indexNameWithModes, attributes, query);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}

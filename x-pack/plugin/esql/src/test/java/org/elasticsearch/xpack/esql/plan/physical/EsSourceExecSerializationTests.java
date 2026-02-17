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
            randomFieldAttributes(1, 10, false),
            new TermQueryBuilder(randomAlphaOfLength(5), randomAlphaOfLength(5)),
            randomBoolean() ? -1 : randomLongBetween(0, 1_000_000)
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
        List<Attribute> attributes = instance.output();
        QueryBuilder query = instance.query();
        long avgRowsPerShard = instance.avgRowsPerShard();
        switch (between(0, 4)) {
            case 0 -> indexPattern = randomValueOtherThan(indexPattern, ESTestCase::randomIdentifier);
            case 1 -> indexMode = randomValueOtherThan(indexMode, () -> randomFrom(IndexMode.values()));
            case 2 -> attributes = randomValueOtherThan(attributes, () -> randomFieldAttributes(1, 10, false));
            case 3 -> query = randomValueOtherThan(query, () -> new TermQueryBuilder(randomAlphaOfLength(5), randomAlphaOfLength(5)));
            case 4 -> avgRowsPerShard = randomValueOtherThan(avgRowsPerShard, () -> randomBoolean() ? -1 : randomLongBetween(0, 1_000_000));
            default -> throw new IllegalStateException();
        }
        return new EsSourceExec(instance.source(), indexPattern, indexMode, attributes, query, avgRowsPerShard);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}

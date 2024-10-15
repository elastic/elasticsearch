/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.EsIndexSerializationTests;

import java.io.IOException;
import java.util.List;

public class EsQueryExecSerializationTests extends AbstractPhysicalPlanSerializationTests<EsQueryExec> {
    public static EsQueryExec randomEsQueryExec() {
        Source source = randomSource();
        EsIndex index = EsIndexSerializationTests.randomEsIndex();
        IndexMode indexMode = randomFrom(IndexMode.values());
        List<Attribute> attrs = randomFieldAttributes(1, 10, false);
        QueryBuilder query = randomQuery();
        Expression limit = new Literal(randomSource(), between(0, Integer.MAX_VALUE), DataType.INTEGER);
        Integer estimatedRowSize = randomEstimatedRowSize();
        return new EsQueryExec(source, index, indexMode, attrs, query, limit, EsQueryExec.NO_SORTS, estimatedRowSize);
    }

    public static QueryBuilder randomQuery() {
        return randomBoolean() ? new MatchAllQueryBuilder() : new TermQueryBuilder(randomAlphaOfLength(4), randomAlphaOfLength(4));
    }

    @Override
    protected EsQueryExec createTestInstance() {
        return randomEsQueryExec();
    }

    @Override
    protected EsQueryExec mutateInstance(EsQueryExec instance) throws IOException {
        EsIndex index = instance.index();
        IndexMode indexMode = instance.indexMode();
        List<Attribute> attrs = instance.attrs();
        QueryBuilder query = instance.query();
        Expression limit = instance.limit();
        Integer estimatedRowSize = instance.estimatedRowSize();
        switch (between(0, 5)) {
            case 0 -> index = randomValueOtherThan(index, EsIndexSerializationTests::randomEsIndex);
            case 1 -> indexMode = randomValueOtherThan(indexMode, () -> randomFrom(IndexMode.values()));
            case 2 -> attrs = randomValueOtherThan(attrs, () -> randomFieldAttributes(1, 10, false));
            case 3 -> query = randomValueOtherThan(query, EsQueryExecSerializationTests::randomQuery);
            case 4 -> limit = randomValueOtherThan(
                limit,
                () -> new Literal(randomSource(), between(0, Integer.MAX_VALUE), DataType.INTEGER)
            );
            case 5 -> estimatedRowSize = randomValueOtherThan(
                estimatedRowSize,
                AbstractPhysicalPlanSerializationTests::randomEstimatedRowSize
            );
        }
        return new EsQueryExec(instance.source(), index, indexMode, attrs, query, limit, EsQueryExec.NO_SORTS, estimatedRowSize);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}

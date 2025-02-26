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
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.index.EsIndexSerializationTests;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.index.EsIndexSerializationTests.randomIndexNameWithModes;

public class EsQueryExecSerializationTests extends AbstractPhysicalPlanSerializationTests<EsQueryExec> {
    public static EsQueryExec randomEsQueryExec() {
        return new EsQueryExec(
            randomSource(),
            randomIdentifier(),
            randomFrom(IndexMode.values()),
            randomIndexNameWithModes(),
            randomFieldAttributes(1, 10, false),
            randomQuery(),
            new Literal(randomSource(), between(0, Integer.MAX_VALUE), DataType.INTEGER),
            EsQueryExec.NO_SORTS,
            randomEstimatedRowSize()
        );
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
        String indexPattern = instance.indexPattern();
        IndexMode indexMode = instance.indexMode();
        Map<String, IndexMode> indexNameWithModes = instance.indexNameWithModes();
        List<Attribute> attrs = instance.attrs();
        QueryBuilder query = instance.query();
        Expression limit = instance.limit();
        Integer estimatedRowSize = instance.estimatedRowSize();
        switch (between(0, 6)) {
            case 0 -> indexPattern = randomValueOtherThan(indexPattern, EsIndexSerializationTests::randomIdentifier);
            case 1 -> indexMode = randomValueOtherThan(indexMode, () -> randomFrom(IndexMode.values()));
            case 2 -> indexNameWithModes = randomValueOtherThan(indexNameWithModes, EsIndexSerializationTests::randomIndexNameWithModes);
            case 3 -> attrs = randomValueOtherThan(attrs, () -> randomFieldAttributes(1, 10, false));
            case 4 -> query = randomValueOtherThan(query, EsQueryExecSerializationTests::randomQuery);
            case 5 -> limit = randomValueOtherThan(
                limit,
                () -> new Literal(randomSource(), between(0, Integer.MAX_VALUE), DataType.INTEGER)
            );
            case 6 -> estimatedRowSize = randomValueOtherThan(
                estimatedRowSize,
                AbstractPhysicalPlanSerializationTests::randomEstimatedRowSize
            );
        }
        return new EsQueryExec(
            instance.source(),
            indexPattern,
            indexMode,
            indexNameWithModes,
            attrs,
            query,
            limit,
            EsQueryExec.NO_SORTS,
            estimatedRowSize
        );
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}

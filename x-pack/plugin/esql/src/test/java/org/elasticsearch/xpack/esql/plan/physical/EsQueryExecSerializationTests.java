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
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.FieldAttributeTests;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.EsIndexSerializationTests;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_POINT;

public class EsQueryExecSerializationTests extends AbstractPhysicalPlanSerializationTests<EsQueryExec> {
    public static EsQueryExec randomEsQueryExec() {
        Source source = randomSource();
        EsIndex index = EsIndexSerializationTests.randomEsIndex();
        IndexMode indexMode = randomFrom(IndexMode.values());
        List<Attribute> attrs = randomFieldAttributes(1, 10, false);
        QueryBuilder query = randomQuery();
        Expression limit = new Literal(randomSource(), between(0, Integer.MAX_VALUE), DataType.INTEGER);
        List<EsQueryExec.Sort> sorts = randomSorts();
        Integer estimatedRowSize = randomEstimatedRowSize();
        return new EsQueryExec(source, index, indexMode, attrs, query, limit, sorts, estimatedRowSize);
    }

    public static QueryBuilder randomQuery() {
        return randomBoolean() ? new MatchAllQueryBuilder() : new TermQueryBuilder(randomAlphaOfLength(4), randomAlphaOfLength(4));
    }

    public static List<EsQueryExec.Sort> randomSorts() {
        return randomList(0, 4, EsQueryExecSerializationTests::randomSort);
    }

    public static EsQueryExec.Sort randomSort() {
        return randomBoolean() ? randomFieldSort() : randomGeoDistanceSort();
    }

    public static EsQueryExec.FieldSort randomFieldSort() {
        FieldAttribute field = FieldAttributeTests.createFieldAttribute(0, false);
        Order.OrderDirection direction = randomFrom(Order.OrderDirection.values());
        Order.NullsPosition nulls = randomFrom(Order.NullsPosition.values());
        return new EsQueryExec.FieldSort(field, direction, nulls);
    }

    public static EsQueryExec.GeoDistanceSort randomGeoDistanceSort() {
        String name = randomAlphaOfLength(16);
        FieldAttribute field = new FieldAttribute(Source.EMPTY, name, new EsField(name, GEO_POINT, Collections.emptyMap(), false));
        Order.OrderDirection direction = randomFrom(Order.OrderDirection.values());
        double lat = randomDoubleBetween(-90, 90, false);
        double lon = randomDoubleBetween(-180, 180, false);
        return new EsQueryExec.GeoDistanceSort(field, direction, lat, lon);
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
        List<EsQueryExec.Sort> sorts = instance.sorts();
        Integer estimatedRowSize = instance.estimatedRowSize();
        switch (between(0, 6)) {
            case 0 -> index = randomValueOtherThan(index, EsIndexSerializationTests::randomEsIndex);
            case 1 -> indexMode = randomValueOtherThan(indexMode, () -> randomFrom(IndexMode.values()));
            case 2 -> attrs = randomValueOtherThan(attrs, () -> randomFieldAttributes(1, 10, false));
            case 3 -> query = randomValueOtherThan(query, EsQueryExecSerializationTests::randomQuery);
            case 4 -> limit = randomValueOtherThan(
                limit,
                () -> new Literal(randomSource(), between(0, Integer.MAX_VALUE), DataType.INTEGER)
            );
            case 5 -> sorts = randomValueOtherThan(sorts, EsQueryExecSerializationTests::randomSorts);
            case 6 -> estimatedRowSize = randomValueOtherThan(
                estimatedRowSize,
                AbstractPhysicalPlanSerializationTests::randomEstimatedRowSize
            );
        }
        return new EsQueryExec(instance.source(), index, indexMode, attrs, query, limit, sorts, estimatedRowSize);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}

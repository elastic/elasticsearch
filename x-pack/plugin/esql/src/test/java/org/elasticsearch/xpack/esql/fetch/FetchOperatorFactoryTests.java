/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FetchExec;
import org.elasticsearch.xpack.esql.plan.physical.FetchSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

public class FetchOperatorFactoryTests extends ESTestCase {
    /**
     * The nodes that own the documents load the fetched columns without an extraction preference, so a point arrives as
     * its well known binary and not as the long of its doc values. The fetch plan only loads.
     */
    public void testLoadsPlainValues() {
        Attribute doc = new FieldAttribute(Source.EMPTY, null, null, EsQueryExec.DOC_ID_FIELD.getName(), EsQueryExec.DOC_ID_FIELD);
        List<Attribute> fetched = List.of(field("message", DataType.KEYWORD), field("location", DataType.GEO_POINT));
        FetchExec exec = new FetchExec(
            Source.EMPTY,
            new ExchangeSourceExec(Source.EMPTY, List.of(doc), false),
            doc,
            fetched,
            "idx",
            List.of("idx"),
            null
        );

        FetchOperator.Factory factory = FetchOperator.Factory.forExec(exec, 0, null, new FetchFailures(false));
        assertThat(factory.fetchedTypes(), equalTo(List.of(ElementType.BYTES_REF, ElementType.BYTES_REF)));
        assertThat(factory.fetchPlan(), instanceOf(ProjectExec.class));
        ProjectExec project = (ProjectExec) factory.fetchPlan();
        assertThat(project.output(), equalTo(fetched));
        assertThat(project.child(), instanceOf(FieldExtractExec.class));
        FieldExtractExec extract = (FieldExtractExec) project.child();
        assertThat(extract.attributesToExtract(), equalTo(fetched));
        for (Attribute attribute : fetched) {
            assertThat(extract.fieldExtractPreference(attribute), equalTo(MappedFieldType.FieldExtractPreference.NONE));
        }
        assertThat(extract.child(), instanceOf(FetchSourceExec.class));
        FetchSourceExec source = (FetchSourceExec) extract.child();
        assertTrue(EsQueryExec.isDocAttribute(source.doc()));
        assertThat("the data node sizes its pages for the fetched columns", source.estimatedRowSize(), notNullValue());
    }

    /**
     * A {@code TopN} emits its cut once its input ended, so the fetch waits for the whole cut.
     */
    public void testATopNCutIsFetchedWhole() {
        Attribute doc = docAttribute();

        FetchOperator.Factory factory = FetchOperator.Factory.forExec(fetchOver(topNOver(doc), doc), 0, null, new FetchFailures(false));

        assertThat(factory.batching(), equalTo(new FetchOperator.Batching.WholeCut(FetchOperator.MAX_PAGES_HELD)));
    }

    /**
     * A filter between the {@code TopN} and the fetch only drops rows of the pages it passes on, so the cut is still
     * complete when the first page arrives.
     */
    public void testATopNCutUnderAFilterIsFetchedWhole() {
        Attribute doc = docAttribute();
        PhysicalPlan filtered = new FilterExec(Source.EMPTY, topNOver(doc), Literal.TRUE);

        FetchOperator.Factory factory = FetchOperator.Factory.forExec(fetchOver(filtered, doc), 0, null, new FetchFailures(false));

        assertThat(factory.batching(), equalTo(new FetchOperator.Batching.WholeCut(FetchOperator.MAX_PAGES_HELD)));
    }

    /**
     * A {@code LIMIT} passes on its pages as they arrive, so the fetch streams them.
     */
    public void testALimitCutIsStreamed() {
        Attribute doc = docAttribute();
        Literal limit = new Literal(Source.EMPTY, 10, DataType.INTEGER);
        PhysicalPlan cut = new LimitExec(Source.EMPTY, new ExchangeSourceExec(Source.EMPTY, List.of(doc), false), limit, null);
        FetchExec exec = fetchOver(cut, doc);

        FetchOperator.Factory factory = FetchOperator.Factory.forExec(exec, 0, null, new FetchFailures(false));

        assertThat(factory.batching(), equalTo(FetchOperator.Batching.Streaming.forFetched(exec.fetchedAttributes())));
    }

    private static FetchExec fetchOver(PhysicalPlan cut, Attribute doc) {
        return new FetchExec(Source.EMPTY, cut, doc, List.of(field("message", DataType.KEYWORD)), "idx", List.of("idx"), null);
    }

    private static TopNExec topNOver(Attribute doc) {
        Literal limit = new Literal(Source.EMPTY, 10, DataType.INTEGER);
        Order order = new Order(Source.EMPTY, doc, Order.OrderDirection.ASC, Order.NullsPosition.LAST);
        return new TopNExec(Source.EMPTY, new ExchangeSourceExec(Source.EMPTY, List.of(doc), false), List.of(order), limit, null);
    }

    private static Attribute docAttribute() {
        return new FieldAttribute(Source.EMPTY, null, null, EsQueryExec.DOC_ID_FIELD.getName(), EsQueryExec.DOC_ID_FIELD);
    }

    private static FieldAttribute field(String name, DataType type) {
        return new FieldAttribute(Source.EMPTY, name, new EsField(name, type, Map.of(), true, EsField.TimeSeriesFieldType.NONE));
    }
}

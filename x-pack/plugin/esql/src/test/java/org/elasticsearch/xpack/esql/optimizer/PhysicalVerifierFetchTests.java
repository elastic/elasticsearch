/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.FetchExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

/**
 * Documents leave the node that read them only for a {@link FetchExec} above the exchange, which loads their columns from
 * the nodes that own them, and they never reach the results.
 */
public class PhysicalVerifierFetchTests extends ESTestCase {
    private final Attribute doc = new FieldAttribute(
        Source.EMPTY,
        null,
        null,
        EsQueryExec.DOC_ID_FIELD.getName(),
        EsQueryExec.DOC_ID_FIELD
    );
    private final FieldAttribute a = field("a", DataType.LONG);
    private final FieldAttribute b = field("b", DataType.KEYWORD);

    public void testFetchAboveTheExchange() {
        PhysicalPlan plan = new ProjectExec(Source.EMPTY, fetch(exchange(doc, a), doc), List.of(a, b));
        Failures failures = verify(plan);
        assertFalse(failures.toString(), failures.hasFailures());
    }

    public void testDocumentsCrossAnExchangeOnlyForAFetch() {
        PhysicalPlan plan = new ProjectExec(Source.EMPTY, exchange(doc, a), List.of(a));
        assertThat(verify(plan).toString(), containsString("document identity [_doc] can only cross an exchange for a fetch"));
    }

    public void testDocumentsAreNotReturned() {
        PhysicalPlan plan = fetch(exchange(doc, a), doc);
        assertThat(verify(plan).toString(), containsString("document identity [_doc] cannot be returned"));
    }

    public void testFetchReadsDocuments() {
        PhysicalPlan plan = new ProjectExec(Source.EMPTY, fetch(exchange(doc, a), a), List.of(a, b));
        String failures = verify(plan).toString();
        assertThat(failures, containsString("must read a [_doc] attribute"));
        assertThat("the documents cross for nothing", failures, containsString("can only cross an exchange for a fetch"));
    }

    private FetchExec fetch(PhysicalPlan child, Attribute documents) {
        return new FetchExec(Source.EMPTY, child, documents, List.of(b), "idx", List.of("idx"), null);
    }

    private ExchangeExec exchange(Attribute... output) {
        EsRelation relation = new EsRelation(Source.EMPTY, "idx", IndexMode.STANDARD, Map.of(), Map.of(), Map.of(), List.of(doc, a, b));
        FragmentExec fragment = new FragmentExec(new Project(Source.EMPTY, relation, List.of(output)));
        return new ExchangeExec(Source.EMPTY, List.of(output), false, fragment);
    }

    private static Failures verify(PhysicalPlan plan) {
        return PhysicalVerifier.INSTANCE.verify(plan, plan.output());
    }

    private static FieldAttribute field(String name, DataType type) {
        return new FieldAttribute(Source.EMPTY, name, new EsField(name, type, Map.of(), true, EsField.TimeSeriesFieldType.NONE));
    }
}

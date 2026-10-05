/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.plan.physical.DocRefEncodeExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FetchExec;
import org.elasticsearch.xpack.esql.plan.physical.FetchSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

/**
 * The fetch plan runs on the nodes that own the documents exactly as the coordinator wrote it, so the coordinator
 * checks its shape before shipping it.
 */
public class PhysicalVerifierFetchTests extends ESTestCase {

    private final FieldAttribute fetched = field("a", DataType.LONG);
    private final ReferenceAttribute docRef = new ReferenceAttribute(
        Source.EMPTY,
        null,
        "$$doc_ref",
        DataType.DOC_REF,
        Nullability.FALSE,
        null,
        true
    );
    private final PhysicalPlan rows = new ExchangeSourceExec(Source.EMPTY, List.of(docRef), false);

    public void testValidFetch() {
        FetchExec fetch = fetch(fetchPlan(List.of(fetched)));
        Failures failures = verify(new ProjectExec(Source.EMPTY, fetch, List.of(fetched)));
        assertFalse(failures.toString(), failures.hasFailures());
    }

    public void testFetchPlanMustProduceTheFetchedColumns() {
        FieldAttribute other = field("b", DataType.LONG);
        FetchExec fetch = fetch(fetchPlan(List.of(other)));
        assertThat(verify(new ProjectExec(Source.EMPTY, fetch, List.of(fetched))).toString(), containsString("does not match the fetched"));
    }

    public void testFetchPlanOnlyLoads() {
        Attribute doc = doc();
        FieldExtractExec extract = new FieldExtractExec(
            Source.EMPTY,
            new FetchSourceExec(Source.EMPTY, doc, null),
            List.of(fetched),
            MappedFieldType.FieldExtractPreference.NONE
        );
        Alias computed = new Alias(Source.EMPTY, "c", new Literal(Source.EMPTY, 1L, DataType.LONG));
        PhysicalPlan withEval = new ProjectExec(Source.EMPTY, new EvalExec(Source.EMPTY, extract, List.of(computed)), List.of(fetched));
        FetchExec fetch = fetch(withEval);
        assertThat(
            verify(new ProjectExec(Source.EMPTY, fetch, List.of(fetched))).toString(),
            containsString("[EvalExec] cannot run in a fetch plan")
        );
    }

    public void testDocRefEncodeTypes() {
        Attribute doc = doc();
        PhysicalPlan child = new ExchangeSourceExec(Source.EMPTY, List.of(doc), false);
        ReferenceAttribute notADocRef = new ReferenceAttribute(Source.EMPTY, null, "r", DataType.KEYWORD, Nullability.FALSE, null, true);
        DocRefEncodeExec encode = new DocRefEncodeExec(Source.EMPTY, child, doc, notADocRef);
        assertThat(PhysicalVerifier.LOCAL_INSTANCE.verify(encode, encode.output()).toString(), containsString("[DOC_REF] attribute"));
    }

    private FetchExec fetch(PhysicalPlan fetchPlan) {
        return new FetchExec(Source.EMPTY, rows, fetchPlan, docRef, List.of(fetched), 1, "idx", List.of("idx"), null);
    }

    private static PhysicalPlan fetchPlan(List<Attribute> fields) {
        FieldExtractExec extract = new FieldExtractExec(
            Source.EMPTY,
            new FetchSourceExec(Source.EMPTY, doc(), null),
            fields,
            MappedFieldType.FieldExtractPreference.NONE
        );
        return new ProjectExec(Source.EMPTY, extract, fields);
    }

    private static Failures verify(PhysicalPlan plan) {
        return PhysicalVerifier.INSTANCE.verify(plan, plan.output());
    }

    private static Attribute doc() {
        return new FieldAttribute(Source.EMPTY, null, null, EsQueryExec.DOC_ID_FIELD.getName(), EsQueryExec.DOC_ID_FIELD);
    }

    private static FieldAttribute field(String name, DataType type) {
        return new FieldAttribute(Source.EMPTY, name, new EsField(name, type, Map.of(), true, EsField.TimeSeriesFieldType.NONE));
    }
}

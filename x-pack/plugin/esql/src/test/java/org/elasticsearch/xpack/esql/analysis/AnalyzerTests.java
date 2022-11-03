/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.ql.expression.Alias;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.ReferenceAttribute;
import org.elasticsearch.xpack.ql.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.index.IndexResolution;
import org.elasticsearch.xpack.ql.plan.TableIdentifier;
import org.elasticsearch.xpack.ql.plan.logical.EsRelation;
import org.elasticsearch.xpack.ql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.ql.session.Configuration;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.type.TypesTests;

import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.ql.tree.Source.EMPTY;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class AnalyzerTests extends ESTestCase {
    public void testIndexResolution() {
        EsIndex idx = new EsIndex("idx", Map.of());
        Analyzer analyzer = newAnalyzer(IndexResolution.valid(idx));

        assertEquals(
            new EsRelation(EMPTY, idx, false),
            analyzer.analyze(new UnresolvedRelation(EMPTY, new TableIdentifier(EMPTY, null, "idx"), null, false))
        );
    }

    public void testFailOnUnresolvedIndex() {
        Analyzer analyzer = newAnalyzer(IndexResolution.invalid("Unknown index [idx]"));

        VerificationException e = expectThrows(
            VerificationException.class,
            () -> analyzer.analyze(new UnresolvedRelation(EMPTY, new TableIdentifier(EMPTY, null, "idx"), null, false))
        );

        assertThat(e.getMessage(), containsString("Unknown index [idx]"));
    }

    public void testIndexWithClusterResolution() {
        EsIndex idx = new EsIndex("cluster:idx", Map.of());
        Analyzer analyzer = newAnalyzer(IndexResolution.valid(idx));

        assertEquals(
            new EsRelation(EMPTY, idx, false),
            analyzer.analyze(new UnresolvedRelation(EMPTY, new TableIdentifier(EMPTY, "cluster", "idx"), null, false))
        );
    }

    public void testAttributeResolution() {
        EsIndex idx = new EsIndex("idx", TypesTests.loadMapping("mapping-one-field.json"));
        Analyzer analyzer = newAnalyzer(IndexResolution.valid(idx));

        Eval eval = (Eval) analyzer.analyze(
            new Eval(
                EMPTY,
                new UnresolvedRelation(EMPTY, new TableIdentifier(EMPTY, null, "idx"), null, false),
                List.of(new Alias(EMPTY, "e", new UnresolvedAttribute(EMPTY, "emp_no")))
            )
        );

        assertEquals(1, eval.fields().size());
        assertEquals(new Alias(EMPTY, "e", new FieldAttribute(EMPTY, "emp_no", idx.mapping().get("emp_no"))), eval.fields().get(0));

        assertEquals(2, eval.output().size());
        Attribute empNo = eval.output().get(0);
        assertEquals("emp_no", empNo.name());
        assertThat(empNo, instanceOf(FieldAttribute.class));
        Attribute e = eval.output().get(1);
        assertEquals("e", e.name());
        assertThat(e, instanceOf(ReferenceAttribute.class));
    }

    public void testAttributeResolutionOfChainedReferences() {
        EsIndex idx = new EsIndex("idx", TypesTests.loadMapping("mapping-one-field.json"));
        Analyzer analyzer = newAnalyzer(IndexResolution.valid(idx));

        Eval eval = (Eval) analyzer.analyze(
            new Eval(
                EMPTY,
                new Eval(
                    EMPTY,
                    new UnresolvedRelation(EMPTY, new TableIdentifier(EMPTY, null, "idx"), null, false),
                    List.of(new Alias(EMPTY, "e", new UnresolvedAttribute(EMPTY, "emp_no")))
                ),
                List.of(new Alias(EMPTY, "ee", new UnresolvedAttribute(EMPTY, "e")))
            )
        );

        assertEquals(1, eval.fields().size());
        Alias eeField = (Alias) eval.fields().get(0);
        assertEquals("ee", eeField.name());
        assertEquals("e", ((ReferenceAttribute) eeField.child()).name());

        assertEquals(3, eval.output().size());
        Attribute empNo = eval.output().get(0);
        assertEquals("emp_no", empNo.name());
        assertThat(empNo, instanceOf(FieldAttribute.class));
        Attribute e = eval.output().get(1);
        assertEquals("e", e.name());
        assertThat(e, instanceOf(ReferenceAttribute.class));
        Attribute ee = eval.output().get(2);
        assertEquals("ee", ee.name());
        assertThat(ee, instanceOf(ReferenceAttribute.class));
    }

    public void testRowAttributeResolution() {
        EsIndex idx = new EsIndex("idx", Map.of());
        Analyzer analyzer = newAnalyzer(IndexResolution.valid(idx));

        Eval eval = (Eval) analyzer.analyze(
            new Eval(
                EMPTY,
                new Row(EMPTY, List.of(new Alias(EMPTY, "emp_no", new Literal(EMPTY, 1, DataTypes.INTEGER)))),
                List.of(new Alias(EMPTY, "e", new UnresolvedAttribute(EMPTY, "emp_no")))
            )
        );

        assertEquals(1, eval.fields().size());
        assertEquals(new Alias(EMPTY, "e", new ReferenceAttribute(EMPTY, "emp_no", DataTypes.INTEGER)), eval.fields().get(0));

        assertEquals(2, eval.output().size());
        Attribute empNo = eval.output().get(0);
        assertEquals("emp_no", empNo.name());
        assertThat(empNo, instanceOf(ReferenceAttribute.class));
        Attribute e = eval.output().get(1);
        assertEquals("e", e.name());
        assertThat(e, instanceOf(ReferenceAttribute.class));

        Row row = (Row) eval.child();
        ReferenceAttribute rowEmpNo = (ReferenceAttribute) row.output().get(0);
        assertEquals(rowEmpNo.id(), empNo.id());
    }

    public void testUnresolvableAttribute() {
        EsIndex idx = new EsIndex("idx", TypesTests.loadMapping("mapping-one-field.json"));
        Analyzer analyzer = newAnalyzer(IndexResolution.valid(idx));

        VerificationException ve = expectThrows(
            VerificationException.class,
            () -> analyzer.analyze(
                new Eval(
                    EMPTY,
                    new UnresolvedRelation(EMPTY, new TableIdentifier(EMPTY, null, "idx"), null, false),
                    List.of(new Alias(EMPTY, "e", new UnresolvedAttribute(EMPTY, "emp_nos")))
                )
            )
        );

        assertThat(ve.getMessage(), containsString("Unknown column [emp_nos], did you mean [emp_no]?"));
    }

    private Analyzer newAnalyzer(IndexResolution indexResolution) {
        Configuration configuration = new Configuration(ZoneOffset.UTC, null, null, x -> Collections.emptySet());
        return new Analyzer(indexResolution, new EsqlFunctionRegistry(), new Verifier(), configuration);
    }
}

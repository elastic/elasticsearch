/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.FlattenedEsField;
import org.elasticsearch.xpack.esql.core.type.KeywordEsField;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.JsonExtract;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;

public class FlattenedJsonExtractOptimizerTests extends AbstractLogicalPlanOptimizerTests {

    /** Non-empty so {@code SkipQueryOnEmptyMappings} does not replace {@link EsRelation} with an empty local relation. */
    private static final Map<String, IndexMode> TEST_INDEX_MODES = Map.of("test-idx", IndexMode.STANDARD);

    public void testJsonExtractOnFlattenedRootRewritesToSyntheticField() {
        assumeTrue("json_extract_flattened_field", EsqlCapabilities.Cap.JSON_EXTRACT_FLATTENED_FIELD.isEnabled());
        Source src = Source.EMPTY;
        FieldAttribute root = new FieldAttribute(src, "resource.attributes", new FlattenedEsField("resource.attributes", true));
        EsRelation relation = new EsRelation(src, "test-*", IndexMode.STANDARD, Map.of(), Map.of(), TEST_INDEX_MODES, List.of(root));
        JsonExtract extract = new JsonExtract(src, root, Literal.keyword(src, "host.name"));
        Eval eval = new Eval(src, relation, List.of(new Alias(src, "h", extract)));
        LogicalPlan optimized = logicalOptimizer.optimize(eval);
        assertThat(optimized, instanceOf(Eval.class));
        Eval e = (Eval) optimized;
        assertThat(e.fields().get(0).child(), instanceOf(FieldAttribute.class));
        FieldAttribute sub = (FieldAttribute) e.fields().get(0).child();
        assertTrue(sub.synthetic());
        assertEquals("resource.attributes.host.name", sub.name());
        assertThat(sub.field(), instanceOf(KeywordEsField.class));
    }

    public void testBracketPathDoesNotRewriteJsonExtract() {
        assumeTrue("json_extract_flattened_field", EsqlCapabilities.Cap.JSON_EXTRACT_FLATTENED_FIELD.isEnabled());
        Source src = Source.EMPTY;
        FieldAttribute root = new FieldAttribute(src, "resource.attributes", new FlattenedEsField("resource.attributes", true));
        EsRelation relation = new EsRelation(src, "test-*", IndexMode.STANDARD, Map.of(), Map.of(), TEST_INDEX_MODES, List.of(root));
        JsonExtract extract = new JsonExtract(src, root, Literal.keyword(src, "['host.name']"));
        Eval eval = new Eval(src, relation, List.of(new Alias(src, "h", extract)));
        LogicalPlan optimized = logicalOptimizer.optimize(eval);
        assertThat(optimized, instanceOf(Eval.class));
        Eval e = (Eval) optimized;
        assertThat(e.fields().get(0).child(), instanceOf(JsonExtract.class));
    }

    public void testPropagateFlattenedSubfieldIntoEsRelationOutput() {
        assumeTrue("json_extract_flattened_field", EsqlCapabilities.Cap.JSON_EXTRACT_FLATTENED_FIELD.isEnabled());
        Source src = Source.EMPTY;
        FieldAttribute root = new FieldAttribute(src, "resource.attributes", new FlattenedEsField("resource.attributes", true));
        EsRelation relation = new EsRelation(src, "test-*", IndexMode.STANDARD, Map.of(), Map.of(), TEST_INDEX_MODES, List.of(root));
        JsonExtract extract = new JsonExtract(src, root, Literal.keyword(src, "host.name"));
        Eval eval = new Eval(src, relation, List.of(new Alias(src, "h", extract)));
        LogicalPlan optimized = logicalOptimizer.optimize(eval);
        EsRelation er = firstEsRelation(optimized);
        assertNotNull(er);
        List<String> names = er.output().stream().map(Attribute::name).toList();
        assertThat(names, hasItem("resource.attributes"));
        assertThat(names, hasItem("resource.attributes.host.name"));
    }

    private static EsRelation firstEsRelation(LogicalPlan plan) {
        if (plan instanceof EsRelation er) {
            return er;
        }
        for (LogicalPlan child : plan.children()) {
            EsRelation found = firstEsRelation(child);
            if (found != null) {
                return found;
            }
        }
        return null;
    }
}

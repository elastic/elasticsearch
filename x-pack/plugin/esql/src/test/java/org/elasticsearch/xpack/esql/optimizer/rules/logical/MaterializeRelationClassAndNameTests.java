/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.ExternalMetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.SimpleSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class MaterializeRelationClassAndNameTests extends AbstractLogicalPlanOptimizerTests {

    private static final String DATASET = "class_a";
    private static final String RESOURCE = "s3://bucket/class_a.parquet";
    private static final List<Attribute> SCHEMA = List.of(
        new ReferenceAttribute(Source.EMPTY, "emp_no", DataType.INTEGER),
        new ReferenceAttribute(Source.EMPTY, "salary", DataType.INTEGER)
    );

    public MaterializeRelationClassAndNameTests(VersionMode versionMode) {
        super(versionMode);
    }

    /**
     * An index answers "index" for _class, as a literal rather than anything read from a shard.
     */
    public void testClassFoldsToALiteralOnAnIndex() {
        Eval eval = firstEval(plan("FROM test METADATA _class | KEEP _class"));
        Alias relationClass = aliasNamed(eval, MetadataAttribute.RELATION_CLASS);
        Literal value = as(relationClass.child(), Literal.class);
        assertThat(BytesRefs.toString(value.value()), equalTo("index"));
    }

    /**
     * _name on an index is the concrete index a row came from, so it points at _index rather than
     * folding: one EsRelation covers an index pattern and the rows do not share a single name.
     */
    public void testNameAliasesIndexOnAnIndex() {
        Eval eval = firstEval(plan("FROM test METADATA _name | KEEP _name"));
        Alias relationName = aliasNamed(eval, MetadataAttribute.RELATION_NAME);
        Attribute target = as(relationName.child(), Attribute.class);
        assertThat(target.name(), equalTo(MetadataAttribute.INDEX));
    }

    /**
     * The query above never asked for _index, so the rule has to put it on the relation itself —
     * and must not leak it into what the user sees.
     */
    public void testIndexIsAddedToTheRelationButNotProjected() {
        LogicalPlan optimized = plan("FROM test METADATA _name | KEEP _name");
        assertThat(Expressions.names(relationUnder(optimized).output()), hasItem(MetadataAttribute.INDEX));
        assertThat(Expressions.names(optimized.output()), contains(MetadataAttribute.RELATION_NAME));
    }

    /**
     * Neither name is left on the relation: no data source is asked for a column it cannot answer.
     */
    public void testBothNamesLeaveTheRelationOutput() {
        LogicalPlan optimized = plan("FROM test METADATA _class, _name | KEEP _class, _name");
        List<String> names = Expressions.names(relationUnder(optimized).output());
        assertThat(names, not(hasItem(MetadataAttribute.RELATION_CLASS)));
        assertThat(names, not(hasItem(MetadataAttribute.RELATION_NAME)));
    }

    /**
     * The Eval does not stay glued to the relation. A per-row constant neither adds nor drops rows,
     * so limit pushdown moves the implicit LIMIT below it and the relation is no longer the Eval's
     * direct child. Pinned because an assertion on that child is an assertion about pushdown rather
     * than about this rule, and reads as a failure of this rule when pushdown changes.
     */
    public void testTheImplicitLimitIsPushedBelowTheEval() {
        LogicalPlan optimized = plan("FROM test METADATA _class | KEEP _class");
        Limit limit = as(firstEval(optimized).child(), Limit.class);
        as(limit.child(), EsRelation.class);
    }

    /**
     * A query that asks for neither is left exactly as it was, so nothing pays for the rule and the
     * pushdowns gated on a leaf-shaped UnionAll keep firing.
     */
    public void testQueryWithoutEitherNameIsUntouched() {
        LogicalPlan optimized = plan("FROM test METADATA _index | KEEP _index");
        assertThat(optimized.collectFirstChildren(p -> p instanceof Eval), hasSize(0));
    }

    /**
     * _index already in the output is reused rather than added a second time, and _name points at that
     * same attribute. A second _index would leave the relation carrying two columns of the same name
     * disagreeing by id, which resolves arbitrarily downstream.
     */
    public void testAnExistingIndexAttributeIsReused() {
        LogicalPlan optimized = plan("FROM test METADATA _index, _name | KEEP _index, _name");
        Attribute target = as(aliasNamed(firstEval(optimized), MetadataAttribute.RELATION_NAME).child(), Attribute.class);
        List<Attribute> indexAttributes = relationUnder(optimized).output()
            .stream()
            .filter(a -> MetadataAttribute.INDEX.equals(a.name()))
            .toList();
        assertThat(indexAttributes, hasSize(1));
        assertThat(target.id(), equalTo(indexAttributes.get(0).id()));
    }

    /**
     * A dataset answers both columns as literals: it has exactly one kind and exactly one name, so
     * unlike an index neither needs a per-row attribute to point at.
     */
    public void testBothColumnsFoldToLiteralsOnADataset() {
        Eval eval = firstEval(datasetPlan(datasetQuery(), DATASET, RESOURCE, SCHEMA));
        assertThat(literalOf(eval, MetadataAttribute.RELATION_CLASS), equalTo("dataset"));
        assertThat(literalOf(eval, MetadataAttribute.RELATION_NAME), equalTo(DATASET));
    }

    /**
     * Neither name is left on a dataset's output either. This is the ExternalRelation arm of the same
     * strip the index tests cover, and it is what keeps a format reader from being handed a column
     * that exists in no file.
     */
    public void testBothNamesLeaveTheDatasetOutput() {
        LogicalPlan optimized = datasetPlan(datasetQuery(), DATASET, RESOURCE, SCHEMA);
        List<LogicalPlan> relations = optimized.collectFirstChildren(p -> p instanceof ExternalRelation);
        assertThat("expected exactly one ExternalRelation", relations, hasSize(1));
        List<String> names = Expressions.names(relations.get(0).output());
        assertThat(names, not(hasItem(MetadataAttribute.RELATION_CLASS)));
        assertThat(names, not(hasItem(MetadataAttribute.RELATION_NAME)));
    }

    /**
     * A dataset deserialized from a node older than {@code esql_external_dataset_name} carries no name,
     * because that node never wrote one. _name is then null rather than a guess or an exception. The
     * relation is built directly here since the wire is the only way to reach that state.
     */
    public void testADatasetWithoutANameAnswersNull() {
        List<Attribute> output = List.of(
            new ReferenceAttribute(Source.EMPTY, "emp_no", DataType.INTEGER),
            new ExternalMetadataAttribute(Source.EMPTY, MetadataAttribute.RELATION_NAME, DataType.KEYWORD)
        );
        SourceMetadata metadata = new SimpleSourceMetadata(output, "test", RESOURCE);
        // The six-argument constructor is the one the wire takes below esql_external_dataset_name: no name.
        ExternalRelation unnamed = new ExternalRelation(Source.EMPTY, RESOURCE, metadata, output, FileList.UNRESOLVED, Map.of());
        assertThat(unnamed.datasetName(), nullValue());

        Project rewritten = as(new MaterializeRelationClassAndName().apply(unnamed), Project.class);
        Literal name = as(aliasNamed(as(rewritten.child(), Eval.class), MetadataAttribute.RELATION_NAME).child(), Literal.class);
        assertThat(name.value(), nullValue());
        assertThat(name.dataType(), equalTo(DataType.KEYWORD));
    }

    private static Eval firstEval(LogicalPlan plan) {
        List<LogicalPlan> evals = plan.collectFirstChildren(p -> p instanceof Eval);
        assertThat("expected exactly one Eval", evals, hasSize(1));
        return as(evals.get(0), Eval.class);
    }

    private static EsRelation relationUnder(LogicalPlan plan) {
        List<LogicalPlan> relations = plan.collectFirstChildren(p -> p instanceof EsRelation);
        assertThat("expected exactly one EsRelation", relations, hasSize(1));
        return as(relations.get(0), EsRelation.class);
    }

    private static String datasetQuery() {
        return "FROM " + DATASET + " METADATA _class, _name | KEEP emp_no, _class, _name";
    }

    private static String literalOf(Eval eval, String name) {
        return BytesRefs.toString(as(aliasNamed(eval, name).child(), Literal.class).value());
    }

    private static Alias aliasNamed(Eval eval, String name) {
        return eval.fields()
            .stream()
            .filter(a -> a.name().equals(name))
            .findFirst()
            .orElseThrow(() -> new AssertionError("no alias named " + name + " in " + eval.fields()));
    }
}

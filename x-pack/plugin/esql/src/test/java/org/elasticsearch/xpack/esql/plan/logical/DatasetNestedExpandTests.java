/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.DeclaredReadSpec;
import org.elasticsearch.xpack.esql.datasources.FileMetadataColumns;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.SimpleSourceMetadata;
import org.elasticsearch.xpack.esql.expression.function.scalar.approximate.Random;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.plan.logical.inference.Completion;
import org.elasticsearch.xpack.esql.plan.logical.inference.Rerank;
import org.elasticsearch.xpack.esql.planner.mapper.Mapper;
import org.elasticsearch.xpack.esql.session.Versioned;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * The refusals of a {@code NESTED_EXPAND} over a dataset, made by the {@link Mapper} on the coordinator over the logical
 * plan. Each is a {@link VerificationException}, a 400, raised while the plan is mapped and so before any data node reads
 * a file: {@code allow_partial_results} turns only a data node's failure into a partial result, so it cannot turn one of
 * these into one.
 */
public class DatasetNestedExpandTests extends ESTestCase {

    private static final FieldAttribute ID = field("id", DataType.LONG);
    private static final FieldAttribute PRICE = field("price", DataType.LONG);
    private static final FieldAttribute SKU = field("items.sku", DataType.KEYWORD);

    public void testRefusesSample() {
        assertRefused(expand(new Sample(new Source(1, 1, "SAMPLE 0.5"), literal(0.5), relation())), "cannot follow [SAMPLE 0.5]");
    }

    /**
     * Nodes that read more than their own row, refused here and only here: the physical rule keeps no list of its own, so
     * this is the one place a node is allowed or refused under the expansion.
     */
    public void testRefusesNodesThatReadMoreThanTheirRow() {
        ReferenceAttribute out = new ReferenceAttribute(Source.EMPTY, "out", DataType.KEYWORD);
        assertRefusedBeforeMapping(
            expand(new Rerank(new Source(1, 1, "RERANK q"), relation(), literal(10), Literal.keyword(Source.EMPTY, "q"), List.of(), out)),
            "cannot follow [RERANK q]"
        );
        assertRefusedBeforeMapping(
            expand(new Completion(new Source(1, 1, "COMPLETION p"), relation(), literal(10), Literal.keyword(Source.EMPTY, "p"), out)),
            "cannot follow [COMPLETION p]"
        );
        assertRefusedBeforeMapping(
            expand(new ChangePoint(new Source(1, 1, "CHANGE_POINT price ON id"), relation(), PRICE, ID, out, out, List.of())),
            "cannot follow [CHANGE_POINT price ON id]"
        );
    }

    public void testRefusesNonDeterministicEvalAndWhere() {
        Alias random = new Alias(Source.EMPTY, "r", new Random(Source.EMPTY, literal(10)));
        assertRefused(
            expand(new Eval(new Source(1, 1, "EVAL r = RANDOM(10)"), relation(), List.of(random))),
            "cannot follow [EVAL r = RANDOM(10)]"
        );
        GreaterThan randomCondition = new GreaterThan(Source.EMPTY, new Random(Source.EMPTY, literal(10)), literal(5));
        assertRefused(
            expand(new Filter(new Source(1, 1, "WHERE RANDOM(10) > 5"), relation(), randomCondition)),
            "cannot follow [WHERE RANDOM(10) > 5]"
        );
    }

    /** The positive control: a deterministic evaluation and filter below the expansion map. */
    public void testMapsDeterministicNodesBelow() {
        Alias doubled = new Alias(Source.EMPTY, "doubled", new Mul(Source.EMPTY, PRICE, literal(2L)));
        Filter filter = new Filter(Source.EMPTY, relation(), new GreaterThan(Source.EMPTY, PRICE, literal(100L)));
        LogicalPlan plan = expand(new Eval(Source.EMPTY, filter, List.of(doubled)));
        DatasetNestedExpand.refuseUnsupported(plan);
        assertNotNull(map(plan));
    }

    public void testRefusesASecondExpansion() {
        FieldAttribute part = field("parts.name", DataType.KEYWORD);
        NestedExpand parts = new NestedExpand(
            new Source(1, 1, "NESTED_EXPAND parts"),
            expand(relation()),
            null,
            "parts",
            List.of(part),
            List.of("name")
        );
        assertRefused(parts, "reads one collection per scan, so [NESTED_EXPAND parts] cannot also expand [items]");
    }

    public void testRefusesARecordReference() {
        List<Attribute> output = new ArrayList<>(List.of(ID, PRICE));
        output.add(new ReferenceAttribute(Source.EMPTY, FileMetadataColumns.RECORD_REF, DataType.LONG));
        assertRefused(expand(relation(output, DeclaredReadSpec.NONE)), "cannot read [" + FileMetadataColumns.RECORD_REF + "]");
    }

    public void testRefusesARenameOfTheCollection() {
        for (Map<String, String> renames : List.of(Map.of("items", "legacy_items"), Map.of("orders", "items"))) {
            DeclaredReadSpec spec = DeclaredReadSpec.of(renames, Map.of(), Set.of());
            assertRefused(expand(relation(List.of(ID, PRICE), spec)), "cannot expand [items]: the dataset declares a rename");
        }
        // A rename of another column is no concern of the expansion
        DatasetNestedExpand.refuseUnsupported(
            expand(relation(List.of(ID, PRICE), DeclaredReadSpec.of(Map.of("cost", "price"), Map.of(), Set.of())))
        );
    }

    public void testRefusesANameClash() {
        assertRefused(
            expand(relation(List.of(ID, PRICE, SKU), DeclaredReadSpec.NONE)),
            "cannot expand [items]: the dataset has a column named [items.sku]"
        );
        Alias shadow = new Alias(Source.EMPTY, SKU.name(), literal(1L));
        assertRefused(
            expand(new Eval(new Source(1, 1, "EVAL items.sku = 1"), relation(), List.of(shadow))),
            "cannot follow [EVAL items.sku = 1], which defines [items.sku]"
        );
    }

    /**
     * An expansion over a branch plan that reads a dataset beside an index has no one scan to read the elements, and is
     * refused on the coordinator, never left to the data node's verifier. Where the branches meet on the coordinator the
     * {@link Mapper} refuses it first, as on an index; the check refuses one whose branches stay below it too.
     */
    public void testRefusesABranchThatReadsADataset() {
        EsRelation index = new EsRelation(Source.EMPTY, "idx", IndexMode.STANDARD, Map.of(), Map.of(), Map.of(), List.of(ID, PRICE));
        UnionAll union = new UnionAll(new Source(1, 1, "FROM idx, orders"), List.of(index, relation()), List.of(ID, PRICE));
        VerificationException e = expectThrows(VerificationException.class, () -> DatasetNestedExpand.refuseUnsupported(expand(union)));
        assertThat(e.getMessage(), containsString("NESTED_EXPAND over a dataset cannot follow [FROM idx, orders]"));
        e = expectThrows(VerificationException.class, () -> map(expand(union)));
        assertThat(e.getMessage(), containsString("cannot follow [FROM idx, orders]"));
        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
    }

    /** An index's expansion is read through its child documents and is none of this check's business. */
    public void testLeavesAnIndexExpansionAlone() {
        EsRelation index = new EsRelation(Source.EMPTY, "idx", IndexMode.STANDARD, Map.of(), Map.of(), Map.of(), List.of(ID, PRICE));
        DatasetNestedExpand.refuseUnsupported(expand(new Sample(Source.EMPTY, literal(0.5), index)));
    }

    private static void assertRefused(LogicalPlan plan, String message) {
        VerificationException e = expectThrows(VerificationException.class, () -> map(plan));
        assertThat(e.getMessage(), containsString("NESTED_EXPAND over a dataset " + message));
        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
    }

    /** The refusal itself, for a node the {@link Mapper} cannot map in a unit test without its coordinator services. */
    private static void assertRefusedBeforeMapping(LogicalPlan plan, String message) {
        VerificationException e = expectThrows(VerificationException.class, () -> DatasetNestedExpand.refuseUnsupported(plan));
        assertThat(e.getMessage(), containsString("NESTED_EXPAND over a dataset " + message));
    }

    private static Object map(LogicalPlan plan) {
        return new Mapper().map(new Versioned<>(plan, TransportVersion.current()));
    }

    private static NestedExpand expand(LogicalPlan child) {
        return new NestedExpand(new Source(1, 1, "NESTED_EXPAND items"), child, null, "items", List.of(SKU), List.of("sku"));
    }

    private static ExternalRelation relation() {
        return relation(List.of(ID, PRICE), DeclaredReadSpec.NONE);
    }

    private static ExternalRelation relation(List<Attribute> output, DeclaredReadSpec spec) {
        String path = "s3://bucket/orders.parquet";
        SimpleSourceMetadata metadata = new SimpleSourceMetadata(output, "parquet", path, null, null, Map.of(), Map.of());
        return new ExternalRelation(Source.EMPTY, path, metadata, output, FileList.UNRESOLVED, Map.of(), null, List.of(), spec);
    }

    private static FieldAttribute field(String name, DataType type) {
        return new FieldAttribute(Source.EMPTY, name, new EsField(name, type, Map.of(), false, EsField.TimeSeriesFieldType.NONE));
    }

    private static Literal literal(Object value) {
        DataType type = switch (value) {
            case Integer i -> DataType.INTEGER;
            case Long l -> DataType.LONG;
            case Double d -> DataType.DOUBLE;
            default -> throw new IllegalArgumentException("unexpected literal [" + value + "]");
        };
        return new Literal(Source.EMPTY, value, type);
    }
}

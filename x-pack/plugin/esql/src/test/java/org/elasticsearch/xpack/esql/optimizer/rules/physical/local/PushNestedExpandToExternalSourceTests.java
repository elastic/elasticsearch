/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.DeclaredReadSpec;
import org.elasticsearch.xpack.esql.datasources.FileMetadataColumns;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.expression.function.scalar.approximate.Random;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalVerifier;
import org.elasticsearch.xpack.esql.plan.physical.ChangePointExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.MvExpandExec;
import org.elasticsearch.xpack.esql.plan.physical.NestedExpandExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.SampleExec;
import org.elasticsearch.xpack.esql.plan.physical.inference.CompletionExec;
import org.elasticsearch.xpack.esql.plan.physical.inference.RerankExec;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * The plan shapes {@link PushNestedExpandToExternalSource} produces from a {@code NESTED_EXPAND} over a dataset scan,
 * built by hand as the local physical optimizer receives them.
 */
public class PushNestedExpandToExternalSourceTests extends ESTestCase {

    private static final Source SRC = new Source(1, 1, "NESTED_EXPAND items");
    private static final FieldAttribute ID = field("id", DataType.LONG);
    private static final FieldAttribute PRICE = field("price", DataType.LONG);
    private static final FieldAttribute SKU = field("items.sku", DataType.KEYWORD);
    private static final FieldAttribute QTY = field("items.qty", DataType.LONG);

    public void testExpansionFoldsIntoTheScan() {
        ExternalSourceExec scan = scan();
        PhysicalPlan result = apply(expand(scan));

        ExternalSourceExec folded = as(result, ExternalSourceExec.class);
        assertThat(
            folded.expansion(),
            equalTo(new ExternalSourceExec.Expansion("items", List.of(SKU, QTY), List.of("sku", "qty"), FormatReader.NO_LIMIT))
        );
        assertThat(folded.attributes(), equalTo(scan.attributes()));
        assertThat(folded.output(), equalTo(List.of(ID, PRICE, SKU, QTY)));
        assertTrue(PhysicalVerifier.LOCAL_INSTANCE.verify(result, expand(scan).output()).hasFailures() == false);
    }

    /** The expansion moves below the per-row nodes; a projection keeps the expanded columns it would have had above it. */
    public void testPushesThroughPerRowNodes() {
        ExternalSourceExec scan = scan();
        Alias doubled = new Alias(Source.EMPTY, "doubled", new Mul(Source.EMPTY, PRICE, literal(2L)));
        FilterExec filter = new FilterExec(Source.EMPTY, scan, new GreaterThan(Source.EMPTY, PRICE, literal(100L)));
        EvalExec eval = new EvalExec(Source.EMPTY, filter, List.of(doubled));
        ProjectExec project = new ProjectExec(Source.EMPTY, eval, List.of(ID, doubled.toAttribute()));
        ReferenceAttribute tag = new ReferenceAttribute(Source.EMPTY, "tag", DataType.KEYWORD);
        MvExpandExec mvExpand = new MvExpandExec(Source.EMPTY, project, ID, tag);

        PhysicalPlan result = apply(expand(mvExpand));

        MvExpandExec newMvExpand = as(result, MvExpandExec.class);
        ProjectExec newProject = as(newMvExpand.child(), ProjectExec.class);
        assertThat(newProject.projections(), equalTo(List.<NamedExpression>of(ID, doubled.toAttribute(), SKU, QTY)));
        EvalExec newEval = as(newProject.child(), EvalExec.class);
        FilterExec newFilter = as(newEval.child(), FilterExec.class);
        ExternalSourceExec folded = as(newFilter.child(), ExternalSourceExec.class);
        assertNotNull(folded.expansion());
        assertThat(folded.expansion().parentLimit(), equalTo(FormatReader.NO_LIMIT));
        assertTrue(result.output().containsAll(List.of(SKU, QTY)));
    }

    /** N1: a limit right above the scan bounds the parents; the scan's own pushed limit counted parents too. */
    public void testLimitOnTheScanBecomesTheParentLimit() {
        ExternalSourceExec scan = scan().withPushedLimit(7);
        LimitExec limit = new LimitExec(Source.EMPTY, scan, literal(5), null);
        EvalExec eval = new EvalExec(Source.EMPTY, limit, List.of(new Alias(Source.EMPTY, "k", new Mul(Source.EMPTY, ID, literal(2L)))));

        // The expanded columns now come before the evaluated one, so a projection restores the expansion's output order
        ProjectExec reordered = as(apply(expand(eval)), ProjectExec.class);
        assertThat(reordered.output(), equalTo(expand(eval).output()));
        EvalExec result = as(reordered.child(), EvalExec.class);

        ExternalSourceExec folded = as(result.child(), ExternalSourceExec.class);
        assertThat(folded.expansion().parentLimit(), equalTo(5));
        assertThat("the scan's rows are elements now", folded.pushedLimit(), equalTo(FormatReader.NO_LIMIT));

        // A limit above a projection or a deterministic evaluation still bounds the parents
        LimitExec overEval = new LimitExec(Source.EMPTY, new EvalExec(Source.EMPTY, scan(), List.of()), literal(4), null);
        EvalExec kept = as(apply(expand(overEval)), EvalExec.class);
        assertThat(as(kept.child(), ExternalSourceExec.class).expansion().parentLimit(), equalTo(4));
    }

    /**
     * N1: a limit over a filter that stays above the scan is not a parent limit, since the parents it would keep could all
     * fail the filter; it is removed, being a copy of the limit above the expansion.
     */
    public void testLimitOverAFilterIsRemovedNotConverted() {
        FilterExec filter = new FilterExec(
            Source.EMPTY,
            scan(),
            new GreaterThan(Source.EMPTY, new Mul(Source.EMPTY, PRICE, literal(2L)), literal(200L))
        );
        LimitExec limit = new LimitExec(Source.EMPTY, filter, literal(3), null);

        FilterExec result = as(apply(expand(limit)), FilterExec.class);

        ExternalSourceExec folded = as(result.child(), ExternalSourceExec.class);
        assertThat(folded.expansion().parentLimit(), equalTo(FormatReader.NO_LIMIT));
        assertFalse(result.anyMatch(LimitExec.class::isInstance));
    }

    public void testRefusesNodesThatReadMoreThanTheirRow() {
        ReferenceAttribute out = new ReferenceAttribute(Source.EMPTY, "out", DataType.KEYWORD);
        assertRefused(new SampleExec(new Source(1, 1, "SAMPLE 0.5"), scan(), literal(0.5)), "SAMPLE 0.5");
        assertRefused(
            new RerankExec(
                new Source(1, 1, "RERANK \"q\""),
                scan(),
                Literal.keyword(Source.EMPTY, "id"),
                Literal.keyword(Source.EMPTY, "q"),
                List.of(),
                out
            ),
            "RERANK \"q\""
        );
        assertRefused(
            new CompletionExec(
                new Source(1, 1, "COMPLETION p"),
                scan(),
                Literal.keyword(Source.EMPTY, "id"),
                Literal.keyword(Source.EMPTY, "p"),
                out,
                null
            ),
            "COMPLETION p"
        );
        assertRefused(
            new ChangePointExec(new Source(1, 1, "CHANGE_POINT price ON id"), scan(), PRICE, ID, out, out, List.of()),
            "CHANGE_POINT price ON id"
        );
        Alias random = new Alias(Source.EMPTY, "r", new Random(Source.EMPTY, literal(10)));
        assertRefused(new EvalExec(new Source(1, 1, "EVAL r = RANDOM(10)"), scan(), List.of(random)), "EVAL r = RANDOM(10)");
        assertRefused(
            new FilterExec(
                new Source(1, 1, "WHERE RANDOM(10) > 5"),
                scan(),
                new GreaterThan(Source.EMPTY, new Random(Source.EMPTY, literal(10)), literal(5))
            ),
            "WHERE RANDOM(10) > 5"
        );
    }

    /** A read expands one collection, so a second expansion over the same scan is refused rather than run on elements. */
    public void testRefusesASecondExpansion() {
        FieldAttribute part = field("parts.name", DataType.KEYWORD);
        NestedExpandExec parts = new NestedExpandExec(
            new Source(1, 1, "NESTED_EXPAND parts"),
            expand(scan()),
            "parts",
            List.of(part),
            List.of("name")
        );
        VerificationException e = expectThrows(VerificationException.class, () -> apply(parts));
        assertThat(
            e.getMessage(),
            containsString(
                "NESTED_EXPAND over a dataset reads one collection per query, so [NESTED_EXPAND parts] cannot also expand [items]"
            )
        );
    }

    public void testRefusesARecordReference() {
        List<Attribute> attributes = new ArrayList<>(scan().attributes());
        attributes.add(new ReferenceAttribute(Source.EMPTY, FileMetadataColumns.RECORD_REF, DataType.LONG));
        VerificationException e = expectThrows(VerificationException.class, () -> apply(expand(scan().withAttributes(attributes))));
        assertThat(e.getMessage(), containsString("cannot read [" + FileMetadataColumns.RECORD_REF + "]"));
    }

    /** W5: the path is the collection's name in the files; a declared rename to or from it is refused rather than guessed. */
    public void testRefusesARenameOfTheCollection() {
        for (Map<String, String> renames : List.of(Map.of("items", "legacy_items"), Map.of("orders", "items"))) {
            ExternalSourceExec scan = scan().withDeclaredReadSpec(DeclaredReadSpec.of(renames, Map.of(), Set.of()));
            VerificationException e = expectThrows(VerificationException.class, () -> apply(expand(scan)));
            assertThat(e.getMessage(), containsString("NESTED_EXPAND over a dataset cannot expand [items]: the dataset declares a rename"));
        }
        // A rename of another column is no concern of the expansion
        ExternalSourceExec scan = scan().withDeclaredReadSpec(DeclaredReadSpec.of(Map.of("cost", "price"), Map.of(), Set.of()));
        as(apply(expand(scan)), ExternalSourceExec.class);
    }

    /** An index's expansion is read through its child documents, by NestedExpandOperator, and is left alone. */
    public void testLeavesAnIndexExpansionAlone() {
        EsQueryExec index = new EsQueryExec(Source.EMPTY, "idx", IndexMode.STANDARD, List.of(ID), null, null, null, List.of());
        NestedExpandExec expand = new NestedExpandExec(SRC, index, "items", List.of(SKU, QTY), List.of("sku", "qty"));
        assertSame(expand, apply(expand));
    }

    /**
     * The expansion's attributes are in a record the attribute-rewriting rules do not descend into, so the rule runs after
     * all of them; the rules that run after it leave the plan as it is.
     */
    public void testRulesAfterItLeaveThePlanAsItIs() {
        PhysicalPlan folded = apply(new LimitExec(Source.EMPTY, expand(scan()), literal(10), null));
        PhysicalPlan after = new InjectRowPositionForRecordRef().apply(
            new InsertExternalFieldExtraction().apply(folded, context()),
            context()
        );
        assertThat(after, equalTo(folded));
    }

    /** The verifier refuses a NESTED_EXPAND left over a dataset scan, which nothing below it could read. */
    public void testVerifierRefusesAnExpansionOverADataset() {
        NestedExpandExec expand = expand(scan());
        Failures failures = PhysicalVerifier.LOCAL_INSTANCE.verify(expand, expand.output());
        assertTrue(failures.hasFailures());
        assertThat(failures.toString(), containsString("over a dataset must be read by its scan"));
    }

    private static void assertRefused(PhysicalPlan between, String text) {
        VerificationException e = expectThrows(VerificationException.class, () -> apply(expand(between)));
        assertThat(e.getMessage(), containsString("NESTED_EXPAND over a dataset cannot follow [" + text + "]"));
    }

    private static ExternalSourceExec scan() {
        return new ExternalSourceExec(Source.EMPTY, "file:///orders.parquet", "parquet", List.of(ID, PRICE), Map.of(), Map.of(), null);
    }

    private static NestedExpandExec expand(PhysicalPlan child) {
        return new NestedExpandExec(SRC, child, "items", List.of(SKU, QTY), List.of("sku", "qty"));
    }

    private static PhysicalPlan apply(PhysicalPlan plan) {
        return new PushNestedExpandToExternalSource().apply(plan, context());
    }

    private static LocalPhysicalOptimizerContext context() {
        return new LocalPhysicalOptimizerContext(PlannerSettings.DEFAULTS, new EsqlFlags(true), null, FoldContext.small(), null);
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

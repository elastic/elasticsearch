/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.dissect.DissectParser;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
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
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalVerifier;
import org.elasticsearch.xpack.esql.plan.logical.Dissect;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.physical.DissectExec;
import org.elasticsearch.xpack.esql.plan.physical.EnrichExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.ExpandingExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.LookupJoinExec;
import org.elasticsearch.xpack.esql.plan.physical.MergeExec;
import org.elasticsearch.xpack.esql.plan.physical.MvExpandExec;
import org.elasticsearch.xpack.esql.plan.physical.NestedExpandExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.RegisteredDomainExec;
import org.elasticsearch.xpack.esql.plan.physical.SampleExec;
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

        ExpandingExternalSourceExec folded = as(result, ExpandingExternalSourceExec.class);
        assertThat(
            folded.expansion(),
            equalTo(new ExpandingExternalSourceExec.Expansion("items", List.of("sku", "qty"), FormatReader.NO_LIMIT))
        );
        assertThat(folded.expanded(), equalTo(List.of(SKU, QTY)));
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
        ExpandingExternalSourceExec folded = as(newFilter.child(), ExpandingExternalSourceExec.class);
        assertThat(folded.expansion().parentLimit(), equalTo(FormatReader.NO_LIMIT));
        assertTrue(result.output().containsAll(List.of(SKU, QTY)));
    }

    /**
     * The expansion moves down the left side of a lookup join, the rows being read, and never into the lookup side;
     * an enrichment and a per-row enrichment pass it through as they are.
     */
    public void testPushesThroughJoinEnrichAndPerRowEnrichment() {
        FieldAttribute key = field("key", DataType.LONG);
        FieldAttribute name = field("name", DataType.KEYWORD);
        EsQueryExec lookup = new EsQueryExec(Source.EMPTY, "lookup", IndexMode.LOOKUP, List.of(key, name), null, null, null, List.of());
        LookupJoinExec join = new LookupJoinExec(Source.EMPTY, scan(), lookup, List.of(ID), List.of(key), List.of(name), null);
        LookupJoinExec newJoin = as(belowReorder(apply(expand(join))), LookupJoinExec.class);
        as(newJoin.left(), ExpandingExternalSourceExec.class);
        assertThat(newJoin.right(), equalTo(lookup));
        assertTrue(newJoin.output().containsAll(List.of(SKU, QTY, name)));

        ReferenceAttribute region = new ReferenceAttribute(Source.EMPTY, "region", DataType.KEYWORD);
        EnrichExec enrich = enrich(scan(), region);
        EnrichExec newEnrich = as(belowReorder(apply(expand(enrich))), EnrichExec.class);
        as(newEnrich.child(), ExpandingExternalSourceExec.class);
        assertTrue(newEnrich.output().containsAll(List.of(ID, PRICE, SKU, QTY, region)));

        ReferenceAttribute domain = new ReferenceAttribute(Source.EMPTY, "domain", DataType.KEYWORD);
        RegisteredDomainExec perRow = new RegisteredDomainExec(Source.EMPTY, scan(), ID, List.of("domain"), List.of(domain));
        RegisteredDomainExec newPerRow = as(belowReorder(apply(expand(perRow))), RegisteredDomainExec.class);
        as(newPerRow.child(), ExpandingExternalSourceExec.class);
        assertTrue(newPerRow.output().containsAll(List.of(SKU, QTY, domain)));
    }

    /**
     * A node below the expansion that defines a column named like an expanded one would shadow it once the expansion
     * sits under that node. The coordinator refuses such a query ({@code DatasetNestedExpandTests}); a plan that reaches
     * the rule anyway is a planner bug, as is one with a dataset column of that name.
     */
    public void testRefusesANodeThatDefinesAnExpandedColumnName() {
        ReferenceAttribute shadow = new ReferenceAttribute(Source.EMPTY, SKU.name(), DataType.KEYWORD);
        String pattern = "%{items.sku}-%{n}";
        DissectExec dissect = new DissectExec(
            new Source(1, 1, "DISSECT msg"),
            scan(),
            PRICE,
            new Dissect.Parser(pattern, "", new DissectParser(pattern, "")),
            List.of(shadow)
        );
        assertShadowRefused(dissect, "DISSECT msg");
        EnrichExec enrich = enrich(scan(), shadow);
        assertShadowRefused(enrich, "ENRICH p");
        RegisteredDomainExec perRow = new RegisteredDomainExec(
            new Source(1, 1, "REGISTERED_DOMAIN d = msg"),
            scan(),
            ID,
            List.of(SKU.name()),
            List.of(shadow)
        );
        assertShadowRefused(perRow, "REGISTERED_DOMAIN d = msg");

        List<Attribute> withShadow = new ArrayList<>(scan().attributes());
        withShadow.add(shadow);
        assertInvariant(expand(scan().withAttributes(withShadow)), "a dataset column named like an expanded one");
    }

    /** The plan under the projection the rule adds when the expanded columns moved in the output, or the plan itself. */
    private static PhysicalPlan belowReorder(PhysicalPlan plan) {
        return plan instanceof ProjectExec project ? project.child() : plan;
    }

    private static void assertShadowRefused(PhysicalPlan between, String text) {
        assertInvariant(expand(between), "[" + between.nodeName() + "] defining a column named like an expanded one");
    }

    private static void assertInvariant(PhysicalPlan plan, String what) {
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> apply(plan));
        assertThat(e.getMessage(), equalTo("[NESTED_EXPAND items] over a dataset reached the data node with " + what));
    }

    private static EnrichExec enrich(PhysicalPlan child, Attribute field) {
        return new EnrichExec(
            new Source(1, 1, "ENRICH p"),
            child,
            Enrich.Mode.ANY,
            "match",
            ID,
            "p",
            "id",
            Map.of("", "idx"),
            List.<NamedExpression>of(field)
        );
    }

    /** A limit right above the scan bounds the parents; the scan's own pushed limit counted parents too. */
    public void testLimitOnTheScanBecomesTheParentLimit() {
        ExternalSourceExec scan = scan().withPushedLimit(7);
        LimitExec limit = new LimitExec(Source.EMPTY, scan, literal(5), null);
        EvalExec eval = new EvalExec(Source.EMPTY, limit, List.of(new Alias(Source.EMPTY, "k", new Mul(Source.EMPTY, ID, literal(2L)))));

        // The expanded columns now come before the evaluated one, so a projection restores the expansion's output order
        ProjectExec reordered = as(apply(expand(eval)), ProjectExec.class);
        assertThat(reordered.output(), equalTo(expand(eval).output()));
        EvalExec result = as(reordered.child(), EvalExec.class);

        ExpandingExternalSourceExec folded = as(result.child(), ExpandingExternalSourceExec.class);
        assertThat(folded.expansion().parentLimit(), equalTo(5));
        assertThat("the scan's rows are elements now", folded.pushedLimit(), equalTo(FormatReader.NO_LIMIT));

        // A limit above a projection or a deterministic evaluation still bounds the parents
        LimitExec overEval = new LimitExec(Source.EMPTY, new EvalExec(Source.EMPTY, scan(), List.of()), literal(4), null);
        EvalExec kept = as(apply(expand(overEval)), EvalExec.class);
        assertThat(as(kept.child(), ExpandingExternalSourceExec.class).expansion().parentLimit(), equalTo(4));
    }

    /**
     * A limit over a filter that stays above the scan is not a parent limit, since the parents it would keep could all
     * fail the filter. It is not folded, so it stays where it was: only a limit folded into the parent limit leaves the
     * chain, and a limit smaller than the one above the expansion can never be dropped. A folded limit below the filter
     * leaves the chain in the same plan.
     */
    public void testLimitOverAFilterIsKeptNotConverted() {
        LimitExec below = new LimitExec(Source.EMPTY, scan(), literal(9), null);
        FilterExec filter = new FilterExec(
            Source.EMPTY,
            below,
            new GreaterThan(Source.EMPTY, new Mul(Source.EMPTY, PRICE, literal(2L)), literal(200L))
        );
        LimitExec limit = new LimitExec(Source.EMPTY, filter, literal(3), null);

        LimitExec kept = as(apply(expand(limit)), LimitExec.class);
        assertThat(kept.limit(), equalTo(literal(3)));
        FilterExec result = as(kept.child(), FilterExec.class);
        ExpandingExternalSourceExec folded = as(result.child(), ExpandingExternalSourceExec.class);
        assertThat(folded.expansion().parentLimit(), equalTo(9));
    }

    /**
     * Which nodes may sit under the expansion is decided once, on the coordinator ({@code DatasetNestedExpandTests}), and
     * the rule keeps no list of its own: it rebuilds any unary node over the expanding scan as it is, even one the
     * coordinator refuses, such as a sample.
     */
    public void testRebuildsAnyUnaryNodeWithoutAListOfItsOwn() {
        SampleExec sample = new SampleExec(new Source(1, 1, "SAMPLE 0.5"), scan(), literal(0.5));
        SampleExec result = as(belowReorder(apply(expand(sample))), SampleExec.class);
        as(result.child(), ExpandingExternalSourceExec.class);
        assertThat(result.probability(), equalTo(sample.probability()));
    }

    /** A read expands one collection; the coordinator refuses a second expansion, and the rule asserts it never meets one. */
    public void testRefusesASecondExpansion() {
        FieldAttribute part = field("parts.name", DataType.KEYWORD);
        NestedExpandExec parts = new NestedExpandExec(
            new Source(1, 1, "NESTED_EXPAND parts"),
            expand(scan()),
            "parts",
            List.of(part),
            List.of("name")
        );
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> apply(parts));
        assertThat(
            e.getMessage(),
            equalTo("[NESTED_EXPAND parts] over a dataset reached the data node with a second expansion over [items]")
        );
    }

    public void testRefusesARecordReference() {
        List<Attribute> attributes = new ArrayList<>(scan().attributes());
        attributes.add(new ReferenceAttribute(Source.EMPTY, FileMetadataColumns.RECORD_REF, DataType.LONG));
        assertInvariant(expand(scan().withAttributes(attributes)), "a record reference");
    }

    /** The path is the collection's name in the files; a declared rename to or from it is refused rather than guessed. */
    public void testRefusesARenameOfTheCollection() {
        for (Map<String, String> renames : List.of(Map.of("items", "legacy_items"), Map.of("orders", "items"))) {
            ExternalSourceExec scan = scan().withDeclaredReadSpec(DeclaredReadSpec.of(renames, Map.of(), Set.of()));
            assertInvariant(expand(scan), "a rename of the collection");
        }
        // A rename of another column is no concern of the expansion
        ExternalSourceExec scan = scan().withDeclaredReadSpec(DeclaredReadSpec.of(Map.of("cost", "price"), Map.of(), Set.of()));
        as(apply(expand(scan)), ExpandingExternalSourceExec.class);
    }

    /** An index's expansion is read through its child documents, by NestedExpandOperator, and is left alone. */
    public void testLeavesAnIndexExpansionAlone() {
        EsQueryExec index = new EsQueryExec(Source.EMPTY, "idx", IndexMode.STANDARD, List.of(ID), null, null, null, List.of());
        NestedExpandExec expand = new NestedExpandExec(SRC, index, "items", List.of(SKU, QTY), List.of("sku", "qty"));
        assertSame(expand, apply(expand));
    }

    /** The rules that run after it do not match the expanding scan it leaves, and leave the plan as it is. */
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

    /** An expansion left over a scan that already expands is refused like one over a scan that does not: none reads it. */
    public void testVerifierRefusesAnExpansionOverAnExpandingScan() {
        FieldAttribute part = field("parts.name", DataType.KEYWORD);
        PhysicalPlan expanding = apply(expand(scan()));
        as(expanding, ExpandingExternalSourceExec.class);
        NestedExpandExec parts = new NestedExpandExec(SRC, expanding, "parts", List.of(part), List.of("name"));
        assertThat(PhysicalVerifier.LOCAL_INSTANCE.verify(parts, parts.output()).toString(), containsString("must be read by its scan"));
    }

    /**
     * The check covers the expansion's whole subtree, so an expansion above a branch plan that reads a dataset in any
     * branch, beside an index, is refused with the same message.
     */
    public void testVerifierRefusesAnExpansionAboveABranchThatReadsADataset() {
        EsQueryExec index = new EsQueryExec(Source.EMPTY, "idx", IndexMode.STANDARD, List.of(ID, PRICE), null, null, null, List.of());
        MergeExec union = new MergeExec(Source.EMPTY, List.of(index, scan()), List.of(ID, PRICE), MergeExec.Kind.UNION);
        NestedExpandExec expand = expand(union);
        Failures failures = PhysicalVerifier.LOCAL_INSTANCE.verify(expand, expand.output());
        assertThat(failures.toString(), containsString("over a dataset must be read by its scan"));
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

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.WildcardPattern;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.CoalescedSplit;
import org.elasticsearch.xpack.esql.datasources.DeclaredReadSpec;
import org.elasticsearch.xpack.esql.datasources.ExternalSchema;
import org.elasticsearch.xpack.esql.datasources.FileSplit;
import org.elasticsearch.xpack.esql.datasources.FormatReaderRegistry;
import org.elasticsearch.xpack.esql.datasources.SchemaReconciliation;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.FilterPushdownSupport;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.NoConfigFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.PassThroughRowPositionStrategy;
import org.elasticsearch.xpack.esql.datasources.spi.RowPositionStrategy;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceResidualFilter;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvInRange;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvLike;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvRLike;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.WildcardLike;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.optimizer.ExternalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Filter pushdown onto an {@link ExternalSourceExec} keys the reader on {@code sourceType}, not a last-dot
 * of {@code sourcePath()}. Comprehensive format-name resolution tests live in
 * {@link org.elasticsearch.xpack.esql.datasources.FormatNameResolverTests}.
 */
public class PushFiltersToSourceTests extends ESTestCase {

    /**
     * A compressed CSV path last-dots to {@code gz}. The plan's {@code sourceType} is still {@code csv}, so
     * pushdown must look up the csv reader — the same origin as {@code InsertExternalFieldExtraction}.
     */
    public void testPushesCompressedCsvUsingSourceTypeNotPathSuffix() {
        FilterExec filterExec = filterOverExternalSource("file:///hits.csv.gz", "csv", "null_field", Set.of());

        PhysicalPlan result = applyRule(filterExec, registry(true));

        assertThat(result, instanceOf(ExternalSourceExec.class));
        assertNotNull(((ExternalSourceExec) result).pushedFilter());
    }

    /**
     * {@code sourceType=parquet} still pushes when the path uses the {@code .parq} alias. The rule keys
     * the reader on {@code sourceType}, not a last-dot of {@code sourcePath()}.
     */
    public void testPushesParquetSourceTypeForParqAliasPaths() {
        for (String path : List.of("file:///data/*.parq", "file:///data/file.parq")) {
            FilterExec filterExec = filterOverExternalSource(path, "parquet", "null_field", Set.of());
            PhysicalPlan result = applyRule(filterExec, registry(true));
            assertThat(path, result, instanceOf(ExternalSourceExec.class));
            assertNotNull(path, ((ExternalSourceExec) result).pushedFilter());
        }
    }

    // -- referencesAnyColumn: partition/data conjunct split --

    public void testReferencesAnyColumnReturnsTrueForPartitionColumn() {
        Expression expr = new Equals(SRC, fieldAttr("lang"), intLiteral(3));
        assertTrue(PushFiltersToSource.referencesAnyColumn(expr, Set.of("lang")));
    }

    public void testReferencesAnyColumnReturnsFalseForDataColumn() {
        Expression expr = new Equals(SRC, fieldAttr("salary"), intLiteral(100));
        assertFalse(PushFiltersToSource.referencesAnyColumn(expr, Set.of("lang")));
    }

    public void testReferencesAnyColumnReturnsTrueForCompoundExpressionWithPartitionColumn() {
        // A conjunct spanning both a partition column and a data column must be kept in FilterExec,
        // not pushed to the format reader (which has no partition column in its payload).
        Expression mixed = new And(
            SRC,
            new Equals(SRC, fieldAttr("lang"), intLiteral(3)),
            new Equals(SRC, fieldAttr("salary"), intLiteral(100))
        );
        assertTrue(PushFiltersToSource.referencesAnyColumn(mixed, Set.of("lang")));
    }

    public void testReferencesAnyColumnReturnsFalseForEmptyColumnSet() {
        Expression expr = new Equals(SRC, fieldAttr("lang"), intLiteral(3));
        assertFalse(PushFiltersToSource.referencesAnyColumn(expr, Set.of()));
    }

    public void testReferencesAnyColumnReturnsFalseForLiteralWithNoReferences() {
        Expression lit = new Literal(SRC, 3, DataType.INTEGER);
        assertFalse(PushFiltersToSource.referencesAnyColumn(lit, Set.of("lang")));
    }

    // -- reconciliation-cast columns: a conjunct over a column some file stores at another type is pushed AND kept --

    /**
     * {@code union_by_name} widened {@code c} to keyword because one file stores it as a string and another as int64.
     * The cast to keyword runs above the reader, so on the int64 file the reader's LIKE evaluator sees a long block
     * and answers "no constraint". The stub pushdown says YES for everything (it would drop the FilterExec); the rule
     * must still keep the conjunct in a FilterExec so the cast page is re-checked, while pushing it for the files
     * that store the column as a string.
     */
    public void testKeepsLikeOnReconciliationWidenedColumnInFilterExec() {
        FieldAttribute c = fieldAttr("c", DataType.KEYWORD);
        Expression like = likeOn(c, "1*");
        FilterExec filterExec = new FilterExec(
            SRC,
            parquetSourceWithSplits(List.of(c), splitReading("n.parquet", c.name(), DataType.LONG)),
            like
        );

        PhysicalPlan result = applyRule(filterExec, registry(true));

        assertThat(result, instanceOf(FilterExec.class));
        FilterExec kept = (FilterExec) result;
        assertEquals(like, kept.condition());
        assertThat(kept.child(), instanceOf(ExternalSourceExec.class));
        ExternalSourceExec pushed = (ExternalSourceExec) kept.child();
        assertNotNull("the conjunct is still pushed for the files that store the column as a string", pushed.pushedFilter());
        assertEquals(List.of(like), pushed.pushedExpressions());
    }

    /**
     * {@code sku == "a"} stays in {@code FilterExec} because the source only rechecks it. {@code qty * price > 100}
     * is exact on the source page, so it leaves {@code FilterExec}.
     */
    public void testASourceResidualLeavesTheRecheckInFilterExec() {
        FieldAttribute sku = fieldAttr("sku", DataType.KEYWORD);
        FieldAttribute qty = fieldAttr("qty", DataType.LONG);
        FieldAttribute price = fieldAttr("price", DataType.LONG);
        Expression skuEquals = new Equals(SRC, sku, new Literal(SRC, new BytesRef("a"), DataType.KEYWORD), null);
        Expression product = new GreaterThan(SRC, new Mul(SRC, qty, price), new Literal(SRC, 100L, DataType.LONG), null);
        FilterExec filterExec = new FilterExec(SRC, externalSource(sku, qty, price), new And(SRC, skuEquals, product));

        FilterExec kept = as(applyRule(filterExec, residualRegistry()), FilterExec.class);

        assertEquals(skuEquals, kept.condition());
        SourceResidualFilter filter = as(((ExternalSourceExec) kept.child()).pushedFilter(), SourceResidualFilter.class);
        assertEquals(List.of(product), filter.sourceResidual());
    }

    /**
     * A widened column is cast after the read. The residual would see the file's type, so it stays in {@code FilterExec}
     * and the source does not evaluate it.
     */
    public void testASourceResidualOnAWidenedColumnStaysInFilterExec() {
        FieldAttribute sku = fieldAttr("sku", DataType.KEYWORD);
        FieldAttribute qty = fieldAttr("qty", DataType.LONG);
        FieldAttribute price = fieldAttr("price", DataType.LONG);
        Expression skuEquals = new Equals(SRC, sku, new Literal(SRC, new BytesRef("a"), DataType.KEYWORD), null);
        Expression product = new GreaterThan(SRC, new Mul(SRC, qty, price), new Literal(SRC, 100L, DataType.LONG), null);
        FilterExec filterExec = new FilterExec(
            SRC,
            parquetSourceWithSplits(List.of(sku, qty, price), splitReading("n.parquet", "price", DataType.INTEGER)),
            new And(SRC, skuEquals, product)
        );

        FilterExec kept = as(applyRule(filterExec, residualRegistry()), FilterExec.class);

        assertEquals(Set.copyOf(List.of(skuEquals, product)), Set.copyOf(Predicates.splitAnd(kept.condition())));
        SourceResidualFilter filter = as(((ExternalSourceExec) kept.child()).pushedFilter(), SourceResidualFilter.class);
        assertEquals(List.of(), filter.sourceResidual());
    }

    /** Every file stores {@code c} at the query's type: the YES conjunct drops the FilterExec, as before. */
    public void testDropsFilterExecForLikeOnUnwidenedColumn() {
        FieldAttribute c = fieldAttr("c", DataType.KEYWORD);
        Expression like = likeOn(c, "1*");
        FilterExec filterExec = new FilterExec(
            SRC,
            parquetSourceWithSplits(List.of(c), splitReading("s.parquet", c.name(), DataType.KEYWORD)),
            like
        );

        PhysicalPlan result = applyRule(filterExec, registry(true));

        assertThat(result, instanceOf(ExternalSourceExec.class));
        assertNotNull(((ExternalSourceExec) result).pushedFilter());
    }

    /**
     * Only the conjunct over the widened column is held back; a sibling conjunct over a column every file stores at
     * the query's type is pushed without a re-check.
     */
    public void testHoldsBackOnlyConjunctsOverWidenedColumns() {
        FieldAttribute c = fieldAttr("c", DataType.KEYWORD);
        FieldAttribute name = fieldAttr("name", DataType.KEYWORD);
        Expression likeC = likeOn(c, "1*");
        Expression likeName = likeOn(name, "row_*");
        ExternalSourceExec source = parquetSourceWithSplits(
            List.of(c, name),
            FileSplit.withReadSchema(
                "parquet",
                StoragePath.of("file:///data/n.parquet"),
                0,
                1,
                "parquet",
                Map.of(),
                Map.of(),
                null,
                List.of(fieldAttr(c.name(), DataType.LONG), fieldAttr(name.name(), DataType.KEYWORD))
            )
        );
        FilterExec filterExec = new FilterExec(SRC, source, new And(SRC, likeC, likeName));

        PhysicalPlan result = applyRule(filterExec, registry(true));

        assertThat(result, instanceOf(FilterExec.class));
        assertEquals(likeC, ((FilterExec) result).condition());
        ExternalSourceExec pushed = (ExternalSourceExec) ((FilterExec) result).child();
        assertEquals("both conjuncts are still pushed", Set.of(likeC, likeName), Set.copyOf(pushed.pushedExpressions()));
    }

    /**
     * The coordinator's zero-split multi-file read keeps no {@code FileSplit}s on the exec but still casts above the
     * reader, from the per-file {@code schemaMap}; the hold-back must be decided from that map then.
     */
    public void testKeepsLikeOnWidenedColumnFromSchemaMapWhenNoSplits() {
        FieldAttribute c = fieldAttr("c", DataType.KEYWORD);
        Expression like = likeOn(c, "1*");
        ExternalSourceExec source = parquetSourceWithSplits(List.of(c)).withSchemaMap(
            Map.of(
                StoragePath.of("file:///data/s.parquet"),
                new SchemaReconciliation.FileSchemaInfo(new ExternalSchema(List.of(fieldAttr(c.name(), DataType.KEYWORD))), null, null),
                StoragePath.of("file:///data/n.parquet"),
                new SchemaReconciliation.FileSchemaInfo(new ExternalSchema(List.of(fieldAttr(c.name(), DataType.LONG))), null, null)
            )
        );
        FilterExec filterExec = new FilterExec(SRC, source, like);

        PhysicalPlan result = applyRule(filterExec, registry(true));

        assertThat(result, instanceOf(FilterExec.class));
        assertEquals(like, ((FilterExec) result).condition());
    }

    /** A file that lacks the column altogether null-fills it rather than casting it: nothing to re-check. */
    public void testDoesNotHoldBackLikeOnColumnAbsentFromAFile() {
        FieldAttribute c = fieldAttr("c", DataType.KEYWORD);
        FieldAttribute name = fieldAttr("name", DataType.KEYWORD);
        ExternalSourceExec source = parquetSourceWithSplits(
            List.of(c, name),
            splitReading("s.parquet", c.name(), DataType.KEYWORD),
            splitReading("no_c.parquet", name.name(), DataType.KEYWORD)
        );
        FilterExec filterExec = new FilterExec(SRC, source, likeOn(c, "1*"));

        PhysicalPlan result = applyRule(filterExec, registry(true));

        assertThat(result, instanceOf(ExternalSourceExec.class));
    }

    /**
     * Past {@code SplitCoalescer}'s threshold the node receives {@code CoalescedSplit}s; the widened file's read schema
     * must be found through the wrapper, or a large dataset would silently lose the re-check.
     */
    public void testKeepsLikeOnWidenedColumnInsideCoalescedSplit() {
        FieldAttribute c = fieldAttr("c", DataType.KEYWORD);
        Expression like = likeOn(c, "1*");
        ExternalSplit coalesced = new CoalescedSplit(
            "parquet",
            List.of(splitReading("s.parquet", c.name(), DataType.KEYWORD), splitReading("n.parquet", c.name(), DataType.LONG))
        );
        FilterExec filterExec = new FilterExec(SRC, parquetSourceWithSplits(List.of(c), coalesced), like);

        PhysicalPlan result = applyRule(filterExec, registry(true));

        assertThat(result, instanceOf(FilterExec.class));
        assertEquals(like, ((FilterExec) result).condition());
    }

    /**
     * Only some splits carry a read schema: the unstamped file's types are unknown on this node, so it might store
     * {@code c} at another type and the conjunct must be re-checked, even though every stamped file matches the query.
     */
    public void testKeepsLikeWhenSomeSplitLacksReadSchema() {
        FieldAttribute c = fieldAttr("c", DataType.KEYWORD);
        Expression like = likeOn(c, "1*");
        ExternalSourceExec source = parquetSourceWithSplits(
            List.of(c),
            splitReading("s.parquet", c.name(), DataType.KEYWORD),
            unstampedSplit("unknown.parquet")
        );
        FilterExec filterExec = new FilterExec(SRC, source, like);

        PhysicalPlan result = applyRule(filterExec, registry(true));

        assertThat(result, instanceOf(FilterExec.class));
        FilterExec kept = (FilterExec) result;
        assertEquals(like, kept.condition());
        assertEquals(List.of(like), ((ExternalSourceExec) kept.child()).pushedExpressions());
    }

    /**
     * No split carries a read schema (an older coordinator) and the data node's {@code schemaMap} is empty: nothing is
     * known to be cast, so the YES conjunct drops the FilterExec, as before.
     */
    public void testDropsFilterExecWhenNoSplitCarriesReadSchema() {
        FieldAttribute c = fieldAttr("c", DataType.KEYWORD);
        ExternalSourceExec source = parquetSourceWithSplits(List.of(c), unstampedSplit("a.parquet"), unstampedSplit("b.parquet"));
        FilterExec filterExec = new FilterExec(SRC, source, likeOn(c, "1*"));

        PhysicalPlan result = applyRule(filterExec, registry(true));

        assertThat(result, instanceOf(ExternalSourceExec.class));
        assertNotNull(((ExternalSourceExec) result).pushedFilter());
    }

    /** The widened file's read schema must be found through nested {@code CoalescedSplit}s too. */
    public void testKeepsLikeOnWidenedColumnInsideNestedCoalescedSplit() {
        FieldAttribute c = fieldAttr("c", DataType.KEYWORD);
        Expression like = likeOn(c, "1*");
        ExternalSplit nested = new CoalescedSplit(
            "parquet",
            List.of(
                splitReading("s.parquet", c.name(), DataType.KEYWORD),
                new CoalescedSplit("parquet", List.of(splitReading("n.parquet", c.name(), DataType.LONG)))
            )
        );
        FilterExec filterExec = new FilterExec(SRC, parquetSourceWithSplits(List.of(c), nested), like);

        PhysicalPlan result = applyRule(filterExec, registry(true));

        assertThat(result, instanceOf(FilterExec.class));
        assertEquals(like, ((FilterExec) result).condition());
    }

    private static Expression likeOn(FieldAttribute field, String pattern) {
        return new WildcardLike(SRC, field, new WildcardPattern(pattern), false);
    }

    /** A whole-file split whose coordinator-inferred read schema has the single column {@code column} at {@code type}. */
    private static FileSplit splitReading(String fileName, String column, DataType type) {
        return FileSplit.withReadSchema(
            "parquet",
            StoragePath.of("file:///data/" + fileName),
            0,
            1,
            "parquet",
            Map.of(),
            Map.of(),
            null,
            List.of(fieldAttr(column, type))
        );
    }

    /** A whole-file split stamped with no read schema, as an older coordinator (or a file with no schema entry) sends. */
    private static FileSplit unstampedSplit(String fileName) {
        return new FileSplit("parquet", StoragePath.of("file:///data/" + fileName), 0, 1, "parquet", Map.of(), Map.of());
    }

    private static ExternalSourceExec parquetSourceWithSplits(List<Attribute> output, ExternalSplit... splits) {
        return new ExternalSourceExec(
            SRC,
            "file:///data/*.parquet",
            "parquet",
            output,
            Map.of(ErrorPolicy.CONFIG_ERROR_MODE, "null_field"),
            Map.of(),
            /* pushedFilter = */ null,
            /* estimatedRowSize = */ null
        ).withDeclaredReadSpec(DeclaredReadSpec.of(Map.of(), Map.of(), Set.of())).withSplits(List.of(splits));
    }

    // -- skip_row row-drop guard: readers that cannot drop rows once filtered must not receive the filter --

    /**
     * A pushed filter is the only signal Parquet keys late materialization off, and that path emits pages without
     * the row-drop compaction — so a coercion failure there would null the cell and keep the row, silently serving
     * {@code null_field} semantics for a {@code skip_row} read. The rule must leave the predicate in the FilterExec.
     * <p>
     * This has to be decided here, at the mint. The operator factory cannot undo it later: for a
     * {@code Pushability.YES} conjunct the FilterExec is already gone, so suppressing the filter downstream would
     * leak unfiltered rows instead.
     */
    public void testDoesNotPushWhenReaderCannotDropRowsUnderPushedFilter() {
        FilterExec filterExec = filterOverExternalSource("skip_row", Set.of("salary"));

        PhysicalPlan result = applyRule(filterExec, registry(/* dropsRowsUnderPushedFilter = */ false));

        assertSame("the filter must stay above the source, unpushed", filterExec, result);
    }

    /**
     * The stub above reaches {@code false} by saying nothing, which is the point: the SPI default is the conservative
     * answer, so a reader written without a thought for {@code skip_row} loses the pushdown rather than the row-drop.
     * Pinned separately from the rule so a flip of the default is a failure here and not only a silent correctness
     * regression in whichever reader forgot to opt out.
     */
    public void testReaderSilentAboutRowDropTakesTheConservativeDefault() {
        assertFalse(new StubReader().dropsRowsUnderPushedFilter());
    }

    /** The same read on a reader that does drop rows on its filtered path (ORC) keeps the pushdown. */
    public void testPushesWhenReaderDropsRowsUnderPushedFilter() {
        FilterExec filterExec = filterOverExternalSource("skip_row", Set.of("salary"));

        PhysicalPlan result = applyRule(filterExec, registry(/* dropsRowsUnderPushedFilter = */ true));

        assertThat(result, instanceOf(ExternalSourceExec.class));
        assertNotNull(((ExternalSourceExec) result).pushedFilter());
    }

    /**
     * Invert emits {@code ts >= start AND ts < next} as two comparisons, not a {@code Range} node.
     * Pushdown must accept that DATETIME pair through FilterExec → ExternalSourceExec.
     */
    public void testPushesDatetimeHalfOpenInequalityPair() {
        FieldAttribute ts = datetimeField("ts");
        long start = java.time.Instant.parse("1986-01-01T00:00:00Z").toEpochMilli();
        long next = java.time.Instant.parse("1987-01-01T00:00:00Z").toEpochMilli();
        Expression range = new And(
            SRC,
            new GreaterThanOrEqual(SRC, ts, new Literal(SRC, start, DataType.DATETIME), null),
            new LessThan(SRC, ts, new Literal(SRC, next, DataType.DATETIME), null)
        );
        ExternalSourceExec source = new ExternalSourceExec(
            SRC,
            "file:///test.parquet",
            "parquet",
            List.of(ts),
            Map.of(ErrorPolicy.CONFIG_ERROR_MODE, "null_field"),
            Map.of(),
            null,
            null
        ).withDeclaredReadSpec(DeclaredReadSpec.of(Map.of(), Map.of(), Set.of()));
        FilterExec filterExec = new FilterExec(SRC, source, range);

        PhysicalPlan result = applyRule(filterExec, registry(true));

        assertThat(result, instanceOf(ExternalSourceExec.class));
        assertNotNull(((ExternalSourceExec) result).pushedFilter());
    }

    /** No declared column types means nothing can fail to coerce, so no row is ever dropped: pushdown stays on
     *  even for a reader that cannot drop rows, and skip_row costs nothing. */
    public void testPushesUnderSkipRowWithoutDeclaredTypeColumns() {
        FilterExec filterExec = filterOverExternalSource("skip_row", Set.of());

        PhysicalPlan result = applyRule(filterExec, registry(false));

        assertThat(result, instanceOf(ExternalSourceExec.class));
        assertNotNull(((ExternalSourceExec) result).pushedFilter());
    }

    /** Declared column types under a mode that keeps every row are equally harmless. */
    public void testPushesUnderNullFieldWithDeclaredTypeColumns() {
        FilterExec filterExec = filterOverExternalSource("null_field", Set.of("salary"));

        PhysicalPlan result = applyRule(filterExec, registry(false));

        assertThat(result, instanceOf(ExternalSourceExec.class));
        assertNotNull(((ExternalSourceExec) result).pushedFilter());
    }

    // -- prefix-bearing mv_like / mv_rlike: the implied range is offered to the reader beside the pattern --

    /**
     * A prefix-bearing {@code mv_like} has no statistics form, so the reader is handed the {@code mv_in_range} it
     * implies. The bound is a pruning hint only: the retained {@code FilterExec} carries the original pattern, and
     * not the bound, which the pattern implies.
     */
    public void testPrefixBearingMvPatternPushesTheImpliedRange() {
        FieldAttribute region = fieldAttr("region", DataType.KEYWORD);
        for (Expression pattern : List.of(
            new MvLike(SRC, region, keyword("E*")),
            new MvLike(SRC, region, keyword("E*U")),
            new MvRLike(SRC, region, keyword("E.*"))
        )) {
            PhysicalPlan result = applyRule(new FilterExec(SRC, externalSource(region), pattern), rangeOnlyRegistry());

            FilterExec filter = as(result, FilterExec.class);
            assertEquals("the retained filter is the original pattern alone", pattern, filter.condition());
            ExternalSourceExec source = as(filter.child(), ExternalSourceExec.class);
            assertNotNull(source.pushedFilter());
            MvInRange bound = as(source.pushedExpressions().getFirst(), MvInRange.class);
            assertEquals(1, source.pushedExpressions().size());
            assertEquals(region, bound.field());
            assertEquals(new BytesRef("E"), bound.lower().fold(FoldContext.small()));
            assertEquals(new BytesRef("F"), bound.upper().fold(FoldContext.small()));
            assertFalse(bound.includeUpper());
        }
    }

    /** Beside other conjuncts the bound is added for the pattern only, and the other conjuncts keep their place. */
    public void testImpliedRangeIsAddedBesideOtherConjuncts() {
        FieldAttribute region = fieldAttr("region", DataType.KEYWORD);
        FieldAttribute salary = fieldAttr("salary");
        Expression pattern = new MvLike(SRC, region, keyword("E*"));
        Expression other = new Equals(SRC, salary, intLiteral(100));
        FilterExec filterExec = new FilterExec(SRC, externalSource(region, salary), new And(SRC, pattern, other));

        FilterExec filter = as(applyRule(filterExec, rangeOnlyRegistry()), FilterExec.class);

        assertEquals(Set.copyOf(List.of(pattern, other)), Set.copyOf(Predicates.splitAnd(filter.condition())));
        ExternalSourceExec source = as(filter.child(), ExternalSourceExec.class);
        assertEquals(1, source.pushedExpressions().size());
        assertThat(source.pushedExpressions().getFirst(), instanceOf(MvInRange.class));
    }

    /**
     * No bound is derived where none is implied: a pattern without a literal prefix, and any pattern under a
     * {@code NOT}, where the superset range would become an under-match. With nothing else to push, the plan is
     * left as it was.
     */
    public void testNoImpliedRangeWithoutAPrefixOrUnderNot() {
        FieldAttribute region = fieldAttr("region", DataType.KEYWORD);
        for (Expression condition : List.of(
            new MvLike(SRC, region, keyword("*E")),
            new MvRLike(SRC, region, keyword(".*E")),
            new Not(SRC, new MvLike(SRC, region, keyword("E*"))),
            new Or(SRC, new MvLike(SRC, region, keyword("E*")), new MvLike(SRC, region, keyword("A*")))
        )) {
            FilterExec filterExec = new FilterExec(SRC, externalSource(region), condition);
            List<Expression> offered = new ArrayList<>();
            assertSame(condition.toString(), filterExec, applyRule(filterExec, rangeOnlyRegistry(offered)));
            assertEquals("only the original condition is offered", List.of(condition), offered);
        }
    }

    private static final Source SRC = Source.EMPTY;

    private static ExternalSourceExec externalSource(FieldAttribute... attributes) {
        return new ExternalSourceExec(
            SRC,
            "file:///test.parquet",
            "parquet",
            List.of(attributes),
            Map.of(ErrorPolicy.CONFIG_ERROR_MODE, "null_field"),
            Map.of(),
            /* pushedFilter = */ null,
            /* estimatedRowSize = */ null
        ).withDeclaredReadSpec(DeclaredReadSpec.of(Map.of(), Map.of(), Set.of()));
    }

    private static Literal keyword(String value) {
        return new Literal(SRC, new BytesRef(value), DataType.KEYWORD);
    }

    /**
     * A parquet reader whose support pushes {@code mv_in_range} as RECHECK (pushed, and kept in the remainder) and
     * nothing else -- the classification the Parquet support gives these shapes, where {@code mv_like} and
     * {@code mv_rlike} have no statistics form. A stub, because the real support lives in a plugin this module
     * cannot depend on.
     */
    private static FormatReaderRegistry rangeOnlyRegistry() {
        return rangeOnlyRegistry(new ArrayList<>());
    }

    /** As {@link #rangeOnlyRegistry()}, recording into {@code offered} every expression the rule hands the support. */
    private static FormatReaderRegistry rangeOnlyRegistry(List<Expression> offered) {
        FormatReaderRegistry registry = new FormatReaderRegistry(null);
        FormatReader parquet = new DroppingStubReader() {
            @Override
            public FilterPushdownSupport filterPushdownSupport() {
                return filters -> {
                    offered.addAll(filters);
                    List<Expression> pushed = filters.stream().filter(MvInRange.class::isInstance).toList();
                    return pushed.isEmpty()
                        ? FilterPushdownSupport.PushdownResult.none(filters)
                        : new FilterPushdownSupport.PushdownResult("opaque", pushed, filters);
                };
            }
        };
        registry.registerLazy("parquet", (settings, blockFactory) -> parquet, null, null);
        return registry;
    }

    private static FieldAttribute fieldAttr(String name) {
        return fieldAttr(name, DataType.INTEGER);
    }

    private static FieldAttribute datetimeField(String name) {
        return fieldAttr(name, DataType.DATETIME);
    }

    private static FieldAttribute fieldAttr(String name, DataType type) {
        return new FieldAttribute(SRC, name, new EsField(name, type, Map.of(), false, EsField.TimeSeriesFieldType.NONE));
    }

    private static Literal intLiteral(int value) {
        return new Literal(SRC, value, DataType.INTEGER);
    }

    private static FilterExec filterOverExternalSource(String errorMode, Set<String> declaredTypeColumns) {
        return filterOverExternalSource("file:///test.parquet", "parquet", errorMode, declaredTypeColumns);
    }

    private static FilterExec filterOverExternalSource(
        String sourcePath,
        String sourceType,
        String errorMode,
        Set<String> declaredTypeColumns
    ) {
        FieldAttribute salary = fieldAttr("salary");
        ExternalSourceExec source = new ExternalSourceExec(
            SRC,
            sourcePath,
            sourceType,
            List.of(salary),
            Map.of(ErrorPolicy.CONFIG_ERROR_MODE, errorMode),
            Map.of(),
            /* pushedFilter = */ null,
            /* estimatedRowSize = */ null
        ).withDeclaredReadSpec(DeclaredReadSpec.of(Map.of(), Map.of(), declaredTypeColumns));
        return new FilterExec(SRC, source, new Equals(SRC, salary, intLiteral(100)));
    }

    private static PhysicalPlan applyRule(FilterExec filterExec, FormatReaderRegistry registry) {
        LocalPhysicalOptimizerContext ctx = new LocalPhysicalOptimizerContext(
            null,
            null,
            null,
            FoldContext.small(),
            null,
            new ExternalOptimizerContext(registry)
        );
        return new PushFiltersToSource().apply(filterExec, ctx);
    }

    /** Equals stays a recheck. A greater-than is a source residual, the shape parquet gives {@code qty * price > 100}. */
    private static FormatReaderRegistry residualRegistry() {
        FormatReaderRegistry registry = new FormatReaderRegistry(null);
        registry.registerLazy("parquet", (settings, blockFactory) -> new ResidualStubReader(), null, null);
        return registry;
    }

    private static FormatReaderRegistry registry(boolean dropsRowsUnderPushedFilter) {
        FormatReaderRegistry registry = new FormatReaderRegistry(null);
        FormatReader parquet = dropsRowsUnderPushedFilter ? new DroppingStubReader() : new StubReader();
        registry.registerLazy("parquet", (settings, blockFactory) -> parquet, null, null);
        // csv is registered so a compressed csv sourceType still finds a pushdown-capable reader.
        registry.registerLazy("csv", (settings, blockFactory) -> new DroppingStubReader(), null, null);
        return registry;
    }

    /**
     * Reader stub whose only interesting behaviour is the pair the rule consults: it always offers pushdown (via a
     * support object that swallows every conjunct) and says nothing at all about
     * {@link FormatReader#dropsRowsUnderPushedFilter()} — the shape a newly written reader has, so it takes the SPI
     * default. The remaining {@link NoConfigFormatReader} methods stay unimplemented so accidental use during a rule
     * pass is loud.
     */
    private static class StubReader implements NoConfigFormatReader {

        @Override
        public FilterPushdownSupport filterPushdownSupport() {
            return filters -> new FilterPushdownSupport.PushdownResult("opaque", filters, List.of());
        }

        @Override
        public RowPositionStrategy rowPositionStrategy() {
            return PassThroughRowPositionStrategy.INSTANCE;
        }

        @Override
        public SourceMetadata metadata(StorageObject object) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String formatName() {
            return "parquet";
        }

        @Override
        public List<String> fileExtensions() {
            return List.of(".parquet");
        }

        @Override
        public void close() {}
    }

    private static class ResidualStubReader extends StubReader {
        @Override
        public FilterPushdownSupport filterPushdownSupport() {
            return filters -> {
                List<Expression> pushed = new ArrayList<>();
                List<Expression> remainder = new ArrayList<>();
                List<Expression> residual = new ArrayList<>();
                for (Expression filter : filters) {
                    if (filter instanceof GreaterThan) {
                        residual.add(filter);
                    } else {
                        pushed.add(filter);
                        remainder.add(filter);
                    }
                }
                if (pushed.isEmpty()) {
                    return FilterPushdownSupport.PushdownResult.none(filters);
                }
                return new FilterPushdownSupport.PushdownResult(new HoldingResidual(residual), pushed, remainder);
            };
        }
    }

    private record HoldingResidual(List<Expression> sourceResidual) implements SourceResidualFilter {
        @Override
        public Object withoutSourceResidual(Collection<Expression> heldBack) {
            List<Expression> kept = new ArrayList<>();
            for (Expression residual : sourceResidual) {
                if (heldBack.contains(residual) == false) {
                    kept.add(residual);
                }
            }
            return new HoldingResidual(kept);
        }
    }

    /** The opt-in half: a reader that does declare the row-drop on its filtered path, as ORC does. */
    private static class DroppingStubReader extends StubReader {
        @Override
        public boolean dropsRowsUnderPushedFilter() {
            return true;
        }
    }
}

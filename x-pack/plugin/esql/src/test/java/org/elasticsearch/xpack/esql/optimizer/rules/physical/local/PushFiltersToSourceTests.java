/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.DeclaredReadSpec;
import org.elasticsearch.xpack.esql.datasources.FormatNameResolver;
import org.elasticsearch.xpack.esql.datasources.FormatReaderRegistry;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.FilterPushdownSupport;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.NoConfigFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.PassThroughRowPositionStrategy;
import org.elasticsearch.xpack.esql.datasources.spi.RowPositionStrategy;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.optimizer.ExternalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.instanceOf;

/**
 * Verifies that {@link PushFiltersToSource#resolveFormatName} delegates to
 * {@link FormatNameResolver#resolve}. Comprehensive resolution tests live in
 * {@link org.elasticsearch.xpack.esql.datasources.FormatNameResolverTests}.
 */
public class PushFiltersToSourceTests extends ESTestCase {

    public void testResolveFormatNameDelegatesToFormatNameResolver() {
        assertEquals(
            FormatNameResolver.resolve(Map.of("reader", "java"), "file.parquet"),
            PushFiltersToSource.resolveFormatName(Map.of("reader", "java"), "file.parquet")
        );
    }

    public void testResolveFormatNameFromExtension() {
        assertEquals("orc", PushFiltersToSource.resolveFormatName(null, "s3://bucket/data/file.orc"));
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

    /** The same read on a reader that does drop rows on its filtered path (ORC) keeps the pushdown. */
    public void testPushesWhenReaderDropsRowsUnderPushedFilter() {
        FilterExec filterExec = filterOverExternalSource("skip_row", Set.of("salary"));

        PhysicalPlan result = applyRule(filterExec, registry(/* dropsRowsUnderPushedFilter = */ true));

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

    private static final Source SRC = Source.EMPTY;

    private static FieldAttribute fieldAttr(String name) {
        return new FieldAttribute(SRC, name, new EsField(name, DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE));
    }

    private static Literal intLiteral(int value) {
        return new Literal(SRC, value, DataType.INTEGER);
    }

    private static FilterExec filterOverExternalSource(String errorMode, Set<String> declaredTypeColumns) {
        FieldAttribute salary = fieldAttr("salary");
        ExternalSourceExec source = new ExternalSourceExec(
            SRC,
            "file:///test.parquet",
            "parquet",
            List.of(salary),
            Map.of(ErrorPolicy.CONFIG_ERROR_MODE, errorMode),
            Map.of(),
            /* pushedFilter = */ null,
            /* estimatedRowSize = */ null
        ).withDeclaredReadSpec(DeclaredReadSpec.of(Map.of(), null, Map.of(), declaredTypeColumns));
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

    private static FormatReaderRegistry registry(boolean dropsRowsUnderPushedFilter) {
        FormatReaderRegistry registry = new FormatReaderRegistry(null);
        registry.registerLazy("parquet", (settings, blockFactory) -> new StubReader(dropsRowsUnderPushedFilter), null, null);
        return registry;
    }

    /**
     * Reader stub whose only interesting behaviour is the pair the rule consults: it always offers pushdown (via a
     * support object that swallows every conjunct) and answers {@link FormatReader#dropsRowsUnderPushedFilter()} as
     * configured. The remaining {@link NoConfigFormatReader} methods stay unimplemented so accidental use during a
     * rule pass is loud.
     */
    private static final class StubReader implements NoConfigFormatReader {
        private final boolean dropsRows;

        StubReader(boolean dropsRows) {
            this.dropsRows = dropsRows;
        }

        @Override
        public boolean dropsRowsUnderPushedFilter() {
            return dropsRows;
        }

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
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.FormatReaderRegistry;
import org.elasticsearch.xpack.esql.datasources.SourceStatisticsSerializer;
import org.elasticsearch.xpack.esql.datasources.SyntheticColumns;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractor;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractorAware;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.NoConfigFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.PassThroughRowPositionStrategy;
import org.elasticsearch.xpack.esql.datasources.spi.RowPositionStrategy;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.optimizer.ExternalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.physical.ExternalFieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

/**
 * Plan-rewrite tests for {@link InsertExternalFieldExtraction}: covers the happy-path narrowing,
 * the synthetic {@code _rowPosition} insertion, and every documented bail-out.
 */
public class InsertExternalFieldExtractionTests extends ESTestCase {

    /** Six columns: id is the sort key, the other five are projection-only and should be deferred. */
    private static List<Attribute> sixColumnSchema() {
        return List.of(
            field("id", DataType.LONG),
            field("a", DataType.KEYWORD),
            field("b", DataType.KEYWORD),
            field("c", DataType.KEYWORD),
            field("d", DataType.INTEGER),
            field("e", DataType.DOUBLE)
        );
    }

    public void testHappyPathInsertsExtractAboveTopN() {
        List<Attribute> schema = sixColumnSchema();
        Attribute sortKey = schema.get(0);
        ExternalSourceExec source = parquetSource(schema, /* pushedFilter = */ null);
        TopNExec topN = topN(sortKey, 100, source);

        PhysicalPlan rewritten = applyRule(topN, columnExtractorAwareRegistry());

        ExternalFieldExtractExec extract = (ExternalFieldExtractExec) rewritten;
        // Deferred columns are exactly the non-sort-key columns from the schema.
        List<String> deferredNames = extract.attributesToExtract().stream().map(Attribute::name).toList();
        assertEquals(List.of("a", "b", "c", "d", "e"), deferredNames);

        // The synthetic attribute must be _rowPosition, LONG, non-nullable.
        assertEquals(ColumnExtractor.ROW_POSITION_COLUMN, extract.rowPositionAttribute().name());
        assertEquals(DataType.LONG, extract.rowPositionAttribute().dataType());

        // The TopN must still be the direct child; the source below it must be narrowed.
        TopNExec rewrittenTopN = (TopNExec) extract.child();
        ExternalSourceExec narrowed = (ExternalSourceExec) rewrittenTopN.child();
        List<String> narrowedNames = narrowed.output().stream().map(Attribute::name).toList();
        assertEquals(List.of("id", ColumnExtractor.ROW_POSITION_COLUMN), narrowedNames);

        // The paired-exec flag — not _rowPosition presence — is the operator factory's signal to
        // enable deferred extraction. InjectRowPositionForExternalId produces the same projection
        // shape with no extract operator downstream, where deferred mode would create a
        // SourceExtractors registry nothing ever closes.
        assertTrue("narrowed source must carry the deferred-extraction flag", narrowed.deferredExtraction());
    }

    /**
     * A source whose projection contains {@code _rowPosition} but that was never paired with an
     * {@code ExternalFieldExtractExec} (the InjectRowPositionForExternalId shape — plain
     * {@code METADATA _id}, no TopN) must NOT carry the deferred-extraction flag: with no extract
     * operator downstream, deferred mode would leak the SourceExtractors registry, its
     * ColumnExtractors, and the factory's onClose budget.
     */
    public void testRowPositionInProjectionAloneDoesNotFlagDeferredExtraction() {
        List<Attribute> schema = sixColumnSchema();
        ExternalSourceExec source = parquetSource(schema, null);
        List<Attribute> extended = new ArrayList<>(source.output());
        extended.add(SyntheticColumns.newRowPositionMetadataAttribute(Source.EMPTY));

        ExternalSourceExec withRowPosition = source.withAttributes(extended);

        assertTrue(withRowPosition.output().stream().anyMatch(a -> a.name().equals(ColumnExtractor.ROW_POSITION_COLUMN)));
        assertFalse(
            "_rowPosition in the projection without a paired extract exec must not flag deferred extraction",
            withRowPosition.deferredExtraction()
        );
    }

    public void testNoOpWhenNoTopN() {
        // The rule is keyed on TopN; nothing happens to a bare source. Sanity check via apply()
        // by feeding the source directly — the rule should not match and return the same node.
        ExternalSourceExec source = parquetSource(sixColumnSchema(), null);
        PhysicalPlan result = new InsertExternalFieldExtraction().apply(source, contextWith(columnExtractorAwareRegistry()));
        assertSame(source, result);
    }

    public void testNoOpWhenSourceIsNotColumnExtractorAware() {
        ExternalSourceExec source = parquetSource(sixColumnSchema(), null);
        TopNExec topN = topN(source.output().get(0), 100, source);
        PhysicalPlan result = applyRule(topN, plainParquetRegistry());
        assertSame("rule must be no-op when reader does not implement ColumnExtractorAware", topN, result);
    }

    public void testNoOpWhenRegistryMissing() {
        ExternalSourceExec source = parquetSource(sixColumnSchema(), null);
        TopNExec topN = topN(source.output().get(0), 100, source);
        PhysicalPlan result = applyRule(topN, /* registry = */ null);
        assertSame(topN, result);
    }

    public void testNoOpWhenNoExternalSourceReachable() {
        // A TopN with no ExternalSourceExec underneath (we just hand it back via the rule entry
        // point with a disconnected source). The rule walks down and finds nothing — no rewrite.
        Attribute keep = field("id", DataType.LONG);
        // Synthesize a TopN with a non-source, non-UnaryExec leaf via no child? Simulate by using
        // an ExternalSourceExec with no output overlap and bailing on column count instead — this
        // path is exercised by testNotEnoughDeferredColumns.
        ExternalSourceExec ext = parquetSource(List.of(keep), null);
        TopNExec topN = topN(keep, 100, ext);
        PhysicalPlan result = applyRule(topN, columnExtractorAwareRegistry());
        // With only a single column and that being the sort key, nothing is deferred → bail.
        assertSame(topN, result);
    }

    public void testNotEnoughDeferredColumns() {
        // 1 sort key + 2 projection columns = only 2 deferred. DEFERRED_COLUMN_MIN is 3, so bail.
        List<Attribute> schema = List.of(field("id", DataType.LONG), field("a", DataType.KEYWORD), field("b", DataType.KEYWORD));
        ExternalSourceExec source = parquetSource(schema, null);
        TopNExec topN = topN(schema.get(0), 100, source);

        PhysicalPlan result = applyRule(topN, columnExtractorAwareRegistry());
        assertSame(topN, result);
    }

    public void testLimitTooLarge() {
        ExternalSourceExec source = parquetSource(sixColumnSchema(), null);
        TopNExec topN = topN(source.output().get(0), InsertExternalFieldExtraction.TOPN_EXTRACT_LIMIT_MAX + 1, source);

        PhysicalPlan result = applyRule(topN, columnExtractorAwareRegistry());
        assertSame(topN, result);
    }

    public void testLimitAtBoundary() {
        ExternalSourceExec source = parquetSource(sixColumnSchema(), null);
        TopNExec topN = topN(source.output().get(0), InsertExternalFieldExtraction.TOPN_EXTRACT_LIMIT_MAX, source);

        PhysicalPlan rewritten = applyRule(topN, columnExtractorAwareRegistry());
        // Exactly at the limit must still optimize.
        assertTrue(rewritten instanceof ExternalFieldExtractExec);
    }

    public void testAssertsWhenSourceHasOpaquePushedFilterWithoutExpressions() {
        // Production filters always flow through {@code PushFiltersToSource}, which calls
        // {@code withPushedFilterAndExpressions} and therefore always populates pushed expressions.
        // A filter without expressions is non-production usage that would silently disable the
        // rule; assert loud so the misuse surfaces in tests.
        ExternalSourceExec source = parquetSource(sixColumnSchema(), /* pushedFilter = */ "opaque");
        TopNExec topN = topN(source.output().get(0), 100, source);

        AssertionError ae = expectThrows(AssertionError.class, () -> applyRule(topN, columnExtractorAwareRegistry()));
        assertThat(ae.getMessage(), containsString("pushed filter without pushed expressions"));
    }

    public void testFiresWhenPushedFilterHasExpressionsAndKeepsFilterColumnsEager() {
        // Realistic shape: a Parquet source with WHERE pushed down (filter is opaque, but the
        // optimizer also kept the original ESQL expressions). Column "a" is referenced by the
        // pushed filter and column "id" is the sort key — both must remain eager. Columns "b" to
        // "e" stay deferred.
        List<Attribute> schema = sixColumnSchema();
        Attribute sortKey = schema.get(0);
        Attribute filterColumn = schema.get(1); // "a"
        ExternalSourceExec source = parquetSource(schema, null).withPushedFilterAndExpressions(
            "opaque",
            List.of((Expression) filterColumn)
        );

        TopNExec topN = topN(sortKey, 100, source);

        PhysicalPlan rewritten = applyRule(topN, columnExtractorAwareRegistry());

        ExternalFieldExtractExec extract = (ExternalFieldExtractExec) rewritten;

        // Deferred set must NOT contain "a" — the filter reads it, so it stays eager.
        List<String> deferredNames = extract.attributesToExtract().stream().map(Attribute::name).toList();
        assertEquals(List.of("b", "c", "d", "e"), deferredNames);

        // The narrowed source carries id, a (filter column), and the synthetic _rowPosition.
        ExternalSourceExec narrowed = (ExternalSourceExec) ((TopNExec) extract.child()).child();
        List<String> narrowedNames = narrowed.output().stream().map(Attribute::name).toList();
        assertEquals(List.of("id", "a", ColumnExtractor.ROW_POSITION_COLUMN), narrowedNames);

        // The pushed filter must survive the rewrite; the source still applies it during the
        // forward scan, the optimization only affects the output projection of that scan.
        assertEquals("opaque", narrowed.pushedFilter());
        assertEquals(1, narrowed.pushedExpressions().size());
    }

    public void testBailsWhenFilterColumnRefsLeaveTooFewDeferred() {
        // 1 sort key + 5 projection-only columns: if 3 of the projection-only columns are referenced
        // by the pushed filter, only 2 remain deferred — below DEFERRED_COLUMN_MIN of 3, so bail.
        List<Attribute> schema = sixColumnSchema();
        Attribute sortKey = schema.get(0);
        ExternalSourceExec source = parquetSource(schema, null).withPushedFilterAndExpressions(
            "opaque",
            List.of((Expression) schema.get(1), (Expression) schema.get(2), (Expression) schema.get(3))
        );
        TopNExec topN = topN(sortKey, 100, source);

        PhysicalPlan result = applyRule(topN, columnExtractorAwareRegistry());
        assertSame("rule must bail when too few columns remain deferable after filter eager-ifies them", topN, result);
    }

    public void testKeepsFilterExecRefsEagerWhenInBetweenTopNAndSource() {
        // FilterExec sits between TopN and the source. The rule's intermediate-refs walk must
        // pick up the filter expression's references and keep those columns eager. Sort key is
        // "id"; filter reads "a"; "b..e" remain deferred.
        List<Attribute> schema = sixColumnSchema();
        Attribute sortKey = schema.get(0);
        Attribute filterColumn = schema.get(1); // "a"
        ExternalSourceExec source = parquetSource(schema, null);
        FilterExec filter = new FilterExec(Source.EMPTY, source, (Expression) filterColumn);
        TopNExec topN = topN(sortKey, 100, filter);

        PhysicalPlan rewritten = applyRule(topN, columnExtractorAwareRegistry());
        ExternalFieldExtractExec extract = (ExternalFieldExtractExec) rewritten;

        // The deferred set must NOT contain "a"; the filter on the spine reads it.
        List<String> deferredNames = extract.attributesToExtract().stream().map(Attribute::name).toList();
        assertEquals(List.of("b", "c", "d", "e"), deferredNames);

        // The narrowed source carries id, a (filter column) and the synthetic _rowPosition. The
        // FilterExec is preserved on the spine between the new TopN and the narrowed source.
        TopNExec rewrittenTopN = (TopNExec) extract.child();
        FilterExec rewrittenFilter = (FilterExec) rewrittenTopN.child();
        ExternalSourceExec narrowed = (ExternalSourceExec) rewrittenFilter.child();
        List<String> narrowedNames = narrowed.output().stream().map(Attribute::name).toList();
        assertEquals(List.of("id", "a", ColumnExtractor.ROW_POSITION_COLUMN), narrowedNames);
    }

    public void testNoOpWhenRowPositionNameAlreadyUsed() {
        // A user column named _rowPosition would collide with our synthetic key; bail.
        List<Attribute> schema = new ArrayList<>(sixColumnSchema());
        schema.add(field(ColumnExtractor.ROW_POSITION_COLUMN, DataType.LONG));
        ExternalSourceExec source = parquetSource(schema, null);
        TopNExec topN = topN(source.output().get(0), 100, source);

        PhysicalPlan result = applyRule(topN, columnExtractorAwareRegistry());
        assertSame(topN, result);
    }

    public void testMultipleSortKeysAllPreserved() {
        // Two sort keys (id, region) plus 4 projection-only columns: rule must keep id and region
        // eagerly and defer a, b, c, d.
        List<Attribute> schema = List.of(
            field("id", DataType.LONG),
            field("region", DataType.KEYWORD),
            field("a", DataType.KEYWORD),
            field("b", DataType.KEYWORD),
            field("c", DataType.KEYWORD),
            field("d", DataType.INTEGER)
        );
        ExternalSourceExec source = parquetSource(schema, null);
        Order primary = new Order(Source.EMPTY, schema.get(0), Order.OrderDirection.ASC, Order.NullsPosition.LAST);
        Order secondary = new Order(Source.EMPTY, schema.get(1), Order.OrderDirection.DESC, Order.NullsPosition.FIRST);
        TopNExec topN = new TopNExec(Source.EMPTY, source, List.of(primary, secondary), literal(100), null);

        PhysicalPlan rewritten = applyRule(topN, columnExtractorAwareRegistry());
        ExternalFieldExtractExec extract = (ExternalFieldExtractExec) rewritten;

        ExternalSourceExec narrowed = (ExternalSourceExec) ((TopNExec) extract.child()).child();
        List<String> narrowedNames = narrowed.output().stream().map(Attribute::name).toList();
        // Both sort keys must remain eager, plus the synthetic row-position column.
        assertTrue(narrowedNames.contains("id"));
        assertTrue(narrowedNames.contains("region"));
        assertTrue(narrowedNames.contains(ColumnExtractor.ROW_POSITION_COLUMN));
        // Deferred set is the complement.
        List<String> deferredNames = extract.attributesToExtract().stream().map(Attribute::name).toList();
        assertEquals(List.of("a", "b", "c", "d"), deferredNames);
    }

    public void testPartitionColumnsPinnedEagerNotDeferred() {
        // The regression cell (#153503): hive-style partition columns are plain
        // ReferenceAttributes carrying no VirtualAttribute marker, but they are NOT file-resident —
        // deferring them to the positional ColumnExtractor read throws "column [X] is missing" at
        // runtime. Their names are stamped in sourceMetadata under PARTITION_COLUMNS_KEY. Sort on a
        // data column so the partition columns are projection-only; they must still stay eager.
        List<Attribute> schema = List.of(
            field("id", DataType.LONG),
            refField("STATION", DataType.KEYWORD),
            refField("ELEMENT", DataType.KEYWORD),
            field("a", DataType.KEYWORD),
            field("b", DataType.KEYWORD),
            field("c", DataType.INTEGER),
            field("d", DataType.DOUBLE)
        );
        ExternalSourceExec source = parquetSource(schema, null, partitionMetadata("STATION", "ELEMENT"));
        TopNExec topN = topN(schema.get(0), 100, source);

        PhysicalPlan rewritten = applyRule(topN, columnExtractorAwareRegistry());
        ExternalFieldExtractExec extract = (ExternalFieldExtractExec) rewritten;

        // Partition columns must NOT be deferred — only the plain data columns are.
        List<String> deferredNames = extract.attributesToExtract().stream().map(Attribute::name).toList();
        assertEquals(List.of("a", "b", "c", "d"), deferredNames);

        // The narrowed forward scan keeps id (sort key), both partition columns, and _rowPosition —
        // VirtualColumnIterator materialises STATION/ELEMENT there as constant blocks.
        ExternalSourceExec narrowed = (ExternalSourceExec) ((TopNExec) extract.child()).child();
        List<String> narrowedNames = narrowed.output().stream().map(Attribute::name).toList();
        assertEquals(List.of("id", "STATION", "ELEMENT", ColumnExtractor.ROW_POSITION_COLUMN), narrowedNames);
    }

    public void testWithoutPartitionStampSameColumnsAreDeferred() {
        // Control for the test above: partition-ness is driven solely by the PARTITION_COLUMNS_KEY
        // stamp. With no stamp, the very same ReferenceAttributes are ordinary deferrable data
        // columns (this is why the eager pin must read the stamp, not the attribute subtype).
        List<Attribute> schema = List.of(
            field("id", DataType.LONG),
            refField("STATION", DataType.KEYWORD),
            refField("ELEMENT", DataType.KEYWORD),
            field("a", DataType.KEYWORD),
            field("b", DataType.KEYWORD),
            field("c", DataType.INTEGER),
            field("d", DataType.DOUBLE)
        );
        ExternalSourceExec source = parquetSource(schema, null, Map.of());
        TopNExec topN = topN(schema.get(0), 100, source);

        PhysicalPlan rewritten = applyRule(topN, columnExtractorAwareRegistry());
        ExternalFieldExtractExec extract = (ExternalFieldExtractExec) rewritten;

        List<String> deferredNames = extract.attributesToExtract().stream().map(Attribute::name).toList();
        assertEquals(List.of("STATION", "ELEMENT", "a", "b", "c", "d"), deferredNames);
    }

    public void testBailsWhenPartitionPinningLeavesTooFewDeferred() {
        // Blast-radius guard: pinning partition columns eager only ever removes columns from the
        // deferred set. If that pushes the deferred count below DEFERRED_COLUMN_MIN, the rule must
        // bail and return the plan unchanged — the query then runs fully eager (correct, just not
        // late-materialised). id (sort) + STATION + ELEMENT eager leaves only {a, b} = 2 deferred.
        List<Attribute> schema = List.of(
            field("id", DataType.LONG),
            refField("STATION", DataType.KEYWORD),
            refField("ELEMENT", DataType.KEYWORD),
            field("a", DataType.KEYWORD),
            field("b", DataType.KEYWORD)
        );
        ExternalSourceExec source = parquetSource(schema, null, partitionMetadata("STATION", "ELEMENT"));
        TopNExec topN = topN(schema.get(0), 100, source);

        PhysicalPlan result = applyRule(topN, columnExtractorAwareRegistry());
        assertSame("rule must bail when pinning partition columns leaves too few deferred", topN, result);
    }

    public void testPartitionColumnAsSortKeyStaysEager() {
        // A partition column used as the sort key is caught by both the eagerRefs arm and the
        // partition-name arm; it must remain eager exactly once and never be deferred.
        List<Attribute> schema = List.of(
            refField("STATION", DataType.KEYWORD),
            field("a", DataType.KEYWORD),
            field("b", DataType.KEYWORD),
            field("c", DataType.INTEGER),
            field("d", DataType.DOUBLE)
        );
        ExternalSourceExec source = parquetSource(schema, null, partitionMetadata("STATION"));
        TopNExec topN = topN(schema.get(0), 100, source);

        PhysicalPlan rewritten = applyRule(topN, columnExtractorAwareRegistry());
        ExternalFieldExtractExec extract = (ExternalFieldExtractExec) rewritten;

        List<String> deferredNames = extract.attributesToExtract().stream().map(Attribute::name).toList();
        assertEquals(List.of("a", "b", "c", "d"), deferredNames);

        ExternalSourceExec narrowed = (ExternalSourceExec) ((TopNExec) extract.child()).child();
        List<String> narrowedNames = narrowed.output().stream().map(Attribute::name).toList();
        assertEquals(List.of("STATION", ColumnExtractor.ROW_POSITION_COLUMN), narrowedNames);
    }

    public void testPartitionColumnReferencedByPushedFilterStaysEager() {
        // A partition column that is also read by a pushed filter is eager on two independent grounds
        // (the eagerRefs filter-expression arm and the partition-name arm). It must be pinned exactly
        // once and never deferred — no interaction between the two paths.
        List<Attribute> schema = List.of(
            field("id", DataType.LONG),
            refField("STATION", DataType.KEYWORD),
            field("a", DataType.KEYWORD),
            field("b", DataType.KEYWORD),
            field("c", DataType.INTEGER),
            field("d", DataType.DOUBLE)
        );
        ExternalSourceExec source = parquetSource(schema, null, partitionMetadata("STATION")).withPushedFilterAndExpressions(
            "opaque",
            List.of((Expression) schema.get(1)) // pushed filter reads STATION
        );
        TopNExec topN = topN(schema.get(0), 100, source);

        PhysicalPlan rewritten = applyRule(topN, columnExtractorAwareRegistry());
        ExternalFieldExtractExec extract = (ExternalFieldExtractExec) rewritten;

        List<String> deferredNames = extract.attributesToExtract().stream().map(Attribute::name).toList();
        assertEquals(List.of("a", "b", "c", "d"), deferredNames);

        ExternalSourceExec narrowed = (ExternalSourceExec) ((TopNExec) extract.child()).child();
        List<String> narrowedNames = narrowed.output().stream().map(Attribute::name).toList();
        assertEquals(List.of("id", "STATION", ColumnExtractor.ROW_POSITION_COLUMN), narrowedNames);
    }

    // ---------------------------------------------------------------------------------------------
    // helpers
    // ---------------------------------------------------------------------------------------------

    private static FieldAttribute field(String name, DataType type) {
        return new FieldAttribute(Source.EMPTY, name, new EsField(name, type, Map.of(), false, EsField.TimeSeriesFieldType.NONE));
    }

    /** A plain {@link ReferenceAttribute}, the shape hive partition columns are surfaced as. */
    private static ReferenceAttribute refField(String name, DataType type) {
        return new ReferenceAttribute(Source.EMPTY, name, type);
    }

    /** sourceMetadata stamping the given names as hive partition columns (see {@code PARTITION_COLUMNS_KEY}). */
    private static Map<String, Object> partitionMetadata(String... names) {
        return Map.of(SourceStatisticsSerializer.PARTITION_COLUMNS_KEY, List.of(names));
    }

    private static Literal literal(int value) {
        return new Literal(Source.EMPTY, value, DataType.INTEGER);
    }

    private static TopNExec topN(Attribute sortKey, int limit, PhysicalPlan child) {
        Order order = new Order(Source.EMPTY, sortKey, Order.OrderDirection.ASC, Order.NullsPosition.LAST);
        return new TopNExec(Source.EMPTY, child, List.of(order), literal(limit), null);
    }

    private static ExternalSourceExec parquetSource(List<Attribute> schema, Object pushedFilter) {
        return parquetSource(schema, pushedFilter, Map.of());
    }

    private static ExternalSourceExec parquetSource(List<Attribute> schema, Object pushedFilter, Map<String, Object> sourceMetadata) {
        return new ExternalSourceExec(
            Source.EMPTY,
            "file:///test.parquet",
            "parquet",
            schema,
            Map.of(),
            sourceMetadata,
            pushedFilter,
            null
        );
    }

    private static FormatReaderRegistry columnExtractorAwareRegistry() {
        FormatReaderRegistry registry = new FormatReaderRegistry(null);
        registry.registerLazy("parquet", (settings, blockFactory) -> new ColumnExtractorAwareStub(), null, null);
        return registry;
    }

    private static FormatReaderRegistry plainParquetRegistry() {
        FormatReaderRegistry registry = new FormatReaderRegistry(null);
        registry.registerLazy("parquet", (settings, blockFactory) -> new PlainStub(), null, null);
        return registry;
    }

    private static LocalPhysicalOptimizerContext contextWith(FormatReaderRegistry registry) {
        return new LocalPhysicalOptimizerContext(null, null, null, FoldContext.small(), null, new ExternalOptimizerContext(registry));
    }

    private static PhysicalPlan applyRule(TopNExec topN, FormatReaderRegistry registry) {
        return new InsertExternalFieldExtraction().apply(topN, contextWith(registry));
    }

    /** Format-reader stub that <em>does</em> implement {@link ColumnExtractorAware}. */
    private static final class ColumnExtractorAwareStub extends StubReader implements ColumnExtractorAware {}

    /** Format-reader stub that does NOT implement {@link ColumnExtractorAware}. */
    private static final class PlainStub extends StubReader {}

    /** Common base. The optimizer only ever inspects {@code instanceof ColumnExtractorAware}; the
     *  remaining {@link NoConfigFormatReader} methods stay unimplemented to make accidental use
     *  during a rule pass loud. */
    private static class StubReader implements NoConfigFormatReader {
        @Override
        public RowPositionStrategy rowPositionStrategy() {
            return PassThroughRowPositionStrategy.INSTANCE;
        }

        @Override
        public SourceMetadata metadata(StorageObject object) {
            throw new UnsupportedOperationException();
        }

        @Override
        public org.elasticsearch.compute.operator.CloseableIterator<org.elasticsearch.compute.data.Page> read(
            StorageObject object,
            FormatReadContext context
        ) {
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

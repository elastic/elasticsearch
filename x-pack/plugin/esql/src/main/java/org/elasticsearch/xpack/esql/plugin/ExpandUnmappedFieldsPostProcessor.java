/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Strings;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.plan.logical.UnmappedFieldsAttribute;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.session.Result;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Coordinator-side post-processor for {@code SET unmapped_fields="LOAD_ALL"}.
 *
 * <p>After all data-node pages are collected, scans the {@code _unmapped_fields} column
 * (a JSON-object keyword column) to discover the full set of unique field names across
 * all rows, then replaces that column with one dedicated {@code keyword} column per
 * unique field name. Rows that do not carry a given field get {@code null} in that column.
 * <p>
 * The data node only puts keys into the column that hold a value, so no expanded column comes out null in every row -
 * {@link #assertNoAllNullExpandedColumn} holds that end of the contract down.
 * <p>
 * TODO every row's {@code _source} ends up parsed three times: the data node parses it to filter the column, then the
 *  coordinator parses the column once to collect field names and once more to expand them. A columnar shape — one block of
 *  names and one of values — would let us build the union while reading and expand without re-parsing.
 */
class ExpandUnmappedFieldsPostProcessor {
    /**
     * Expands the {@code _unmapped_fields} column in {@code result} into per-field columns.
     * Returns {@code result} unchanged if no {@link UnmappedFieldsAttribute} is present in the schema.
     */
    static Result expand(Result result, BlockFactory blockFactory, PlannerSettings plannerSettings) {
        List<Attribute> schema = result.schema();

        int unmappedIdx = CollectionUtils.findIndex(schema, e -> e instanceof UnmappedFieldsAttribute);
        if (unmappedIdx == -1) {
            return result;
        }
        double reservationFactor = plannerSettings.sourceReservationFactor();

        // From here on we own the input pages: on success rewritePage drains them one by one, on any failure we release whatever
        // is left below. Page#releaseBlocks is idempotent, so re-releasing pages rewritePage already drained is a no-op.
        boolean success = false;
        try {
            // Converting the SortedSet to an ArrayList for faster iteration.
            var fieldNames = collectFieldNames(result, unmappedIdx, blockFactory.breaker(), reservationFactor);
            List<String> sortedFieldNames = new ArrayList<>(fieldNames);
            // TODO account for newSchema's field names against the circuit breaker. A wide _source turns into a wide schema, and
            // unlike the pages, the response schema has no breaker-tracked lifetime to release it against today.
            List<Attribute> newSchema = buildSchema(schema, unmappedIdx, sortedFieldNames);
            List<Page> newPages = rewritePages(result, unmappedIdx, sortedFieldNames, blockFactory, reservationFactor);

            Result expanded = new Result(
                newSchema,
                newPages,
                result.attributeMetadata(),
                result.configuration(),
                result.completionInfo(),
                result.executionInfo()
            );
            success = true;
            return expanded;
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(result.pages());
            }
        }
    }

    /**
     * Collect the unique field names (sorted) carried by {@code _unmapped_fields} across all pages.
     * <p>
     * Every key here earns an output column, which is why the data node drops the keys that carry no value - see
     * {@code UnmappedFieldsBlockLoader} and {@link #assertNoAllNullExpandedColumn}.
     * <p>
     * TODO cap this set. Every distinct key in any row's {@code _source} becomes an output column, so a wide or
     *  heterogeneous index can blow the response up into thousands of columns.
     * <p>
     * TODO walk the JSON with a parser instead of materialising a whole {@code Map} only to read its {@code keySet()}. That would
     *  also make the reservation below unnecessary.
     */
    private static SortedSet<String> collectFieldNames(Result result, int unmappedIdx, CircuitBreaker breaker, double reservationFactor) {
        TreeSet<String> fieldNames = new TreeSet<>();
        BytesRef scratch = new BytesRef();
        for (Page page : result.pages()) {
            BytesRefBlock unmappedBlock = page.getBlock(unmappedIdx);
            for (int row = 0; row < unmappedBlock.getPositionCount(); row++) {
                if (unmappedBlock.isNull(row)) {
                    continue;
                }
                BytesRef json = getBytesRef(unmappedBlock, row, scratch);
                long reservation = reserveForParse(json, breaker, reservationFactor);
                try {
                    fieldNames.addAll(parseJson(json).keySet());
                } finally {
                    breaker.addWithoutBreaking(-reservation);
                }
            }
        }
        return fieldNames;
    }

    /**
     * Reserves memory for one {@link #parseJson} call, which allocates a {@code Map} nothing else accounts for. The multiplier is
     * {@link PlannerSettings#SOURCE_RESERVATION_FACTOR}, whose javadoc records the measured ~8x blow-up of parsing {@code _source}
     * into a map - the very same parse this column goes through a second time here.
     *
     * @return the number of reserved bytes, to be handed back with {@link CircuitBreaker#addWithoutBreaking} once the map is gone
     */
    private static long reserveForParse(BytesRef json, CircuitBreaker breaker, double reservationFactor) {
        long reservation = (long) (json.length * reservationFactor);
        breaker.addEstimateBytesAndMaybeBreak(reservation, "unmapped fields expansion");
        return reservation;
    }

    /** Builds the expanded schema: every column except {@code _unmapped_fields}, then one keyword column per unmapped field name. */
    private static List<Attribute> buildSchema(List<Attribute> schema, int unmappedIdx, List<String> fieldNames) {
        List<Attribute> newSchema = new ArrayList<>(schema.size() - 1 + fieldNames.size());
        Set<String> existingNames = new HashSet<>();
        for (int i = 0; i < schema.size(); i++) {
            if (i != unmappedIdx) {
                Attribute attribute = schema.get(i);
                newSchema.add(attribute);
                existingNames.add(attribute.name());
            }
        }
        for (String name : fieldNames) {
            if (existingNames.contains(name)) {
                // Unreachable: the pattern excludes every name in the plan's output, so a key that made it into this column cannot
                // collide with a query column. This is an internal error rather than a user error, hence the 500 - it is an
                // AssertionError that does not take the node down in production.
                throw new IllegalStateException(
                    Strings.format(
                        "Conflict in unmapped field name: field '%s' appears both in the query schema and in the _unmapped_fields JSON",
                        name
                    )
                );
            }
            newSchema.add(new ReferenceAttribute(Source.EMPTY, null, name, DataType.KEYWORD));
        }
        return newSchema;
    }

    /** Rewrite each page, replacing the {@code _unmapped_fields} block with one block per expanded field name. */
    private static List<Page> rewritePages(
        Result result,
        int unmappedIdx,
        List<String> fieldNames,
        BlockFactory factory,
        double reservationFactor
    ) {
        int originalColumnCount = result.schema().size();
        var newPages = new ArrayList<Page>(result.pages().size());
        var success = false;
        try {
            for (Page p : result.pages()) {
                newPages.add(rewritePage(unmappedIdx, fieldNames, factory, p, originalColumnCount, reservationFactor));
            }
            assert assertNoAllNullExpandedColumn(newPages, originalColumnCount, fieldNames);
            success = true;
            return newPages;
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(newPages);
            }
        }
    }

    /**
     * Guard rail for what {@code UnmappedFieldsBlockLoader} promises this class: every key it writes into {@code _unmapped_fields}
     * holds a value, and that value is what {@link #appendRow} writes back, so no expanded column can come out {@code null} in every
     * row.
     * <p>
     * Only the expanded columns are checked: a retained column can legitimately be all null, e.g. {@code KEEP field_absent_everywhere}
     * resolves to a {@code null} literal.
     *
     * @return {@code true}, so this can be called from an {@code assert} and skipped entirely in production
     */
    private static boolean assertNoAllNullExpandedColumn(List<Page> pages, int originalColumnCount, List<String> fieldNames) {
        // Same layout as rewritePage builds: the retained columns are every original column but _unmapped_fields, then the expansion.
        int retainedBlockCount = originalColumnCount - 1;
        for (int i = 0; i < fieldNames.size(); i++) {
            int column = retainedBlockCount + i;
            boolean allNull = true;
            for (Page page : pages) {
                if (page.getBlock(column).areAllValuesNull() == false) {
                    allNull = false;
                    break;
                }
            }
            if (allNull) {
                throw new AssertionError(
                    Strings.format("Expanded unmapped field '%s' into a column that is null in every row", fieldNames.get(i))
                );
            }
        }
        return true;
    }

    private static Page rewritePage(
        int unmappedIdx,
        List<String> fieldNames,
        BlockFactory blockFactory,
        Page page,
        int originalColumnCount,
        double reservationFactor
    ) {
        // Output blocks are the retained columns (all but _unmapped_fields) followed by one expanded column per field name
        // collectFieldNames found, so fieldNames.size() is the single source for the expansion width.
        int retainedBlockCount = originalColumnCount - 1;
        int expandedBlockCount = fieldNames.size();
        Block[] allBlocks = new Block[retainedBlockCount + expandedBlockCount];

        int dest = 0;
        for (int i = 0; i < originalColumnCount; i++) {
            if (i != unmappedIdx) {
                var block = page.getBlock(i);
                block.incRef();
                allBlocks[dest++] = block;
            }
        }

        var success = false;
        BytesRefBlock.Builder[] builders = new BytesRefBlock.Builder[expandedBlockCount];
        try (var ignored = Releasables.wrap(builders)) {
            // Zero expanded columns means nothing to expand, so just drop the _unmapped_fields column, keep any retained blocks,
            // and skip the wasted per-row _source re-parse.
            if (expandedBlockCount > 0) {
                BytesRefBlock unmappedBlock = page.getBlock(unmappedIdx);
                Arrays.setAll(builders, i -> blockFactory.newBytesRefBlockBuilder(page.getPositionCount()));
                // Both grow to the largest value seen in this page, so they are per-page rather than per-result.
                var jsonScratch = new BytesRef();
                var scratch = new BytesRefBuilder();
                CircuitBreaker breaker = blockFactory.breaker();
                for (int row = 0; row < page.getPositionCount(); row++) {
                    if (unmappedBlock.isNull(row)) {
                        appendRow(Map.of(), fieldNames, builders, scratch);
                        continue;
                    }
                    BytesRef json = getBytesRef(unmappedBlock, row, jsonScratch);
                    long reservation = reserveForParse(json, breaker, reservationFactor);
                    try {
                        appendRow(parseJson(json), fieldNames, builders, scratch);
                    } finally {
                        breaker.addWithoutBreaking(-reservation);
                    }
                }
                for (int i = 0; i < builders.length; i++) {
                    allBlocks[retainedBlockCount + i] = builders[i].build();
                }
            }
            var result = new Page(page.getPositionCount(), allBlocks);
            // Release _unmapped_fields block from the circuit breaker; the surviving blocks were protected by incRef above.
            page.releaseBlocks();
            success = true;
            return result;
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(allBlocks);
            }
        }
    }

    /**
     * Appends one value per expanded field name, {@code null} where this row's JSON did not carry the field.
     * <p>
     * TODO every non-null value is copied three times: {@code String.valueOf} renders it, {@code copyChars} encodes that to UTF-8 and
     *  the builder copies the bytes again. Values that are already {@code String}s could go straight into the builder's byte array.
     */
    private static void appendRow(
        Map<String, Object> rowMap,
        List<String> fieldNames,
        BytesRefBlock.Builder[] builders,
        BytesRefBuilder scratch
    ) {
        for (int i = 0; i < builders.length; i++) {
            Object value = rowMap.get(fieldNames.get(i));
            if (value == null) {
                builders[i].appendNull();
            } else {
                scratch.copyChars(String.valueOf(value));
                builders[i].appendBytesRef(scratch.get());
            }
        }
    }

    private static BytesRef getBytesRef(BytesRefBlock unmappedBlock, int row, BytesRef scratch) {
        if (unmappedBlock.getValueCount(row) != 1) {
            throw new IllegalStateException(
                Strings.format(
                    "Expected exactly one value in _unmapped_fields block at row %d, but got %d",
                    row,
                    unmappedBlock.getValueCount(row)
                )
            );
        }
        return unmappedBlock.getBytesRef(unmappedBlock.getFirstValueIndex(row), scratch);
    }

    private static Map<String, Object> parseJson(BytesRef ref) {
        return XContentHelper.convertToMap(new BytesArray(ref.bytes, ref.offset, ref.length), false, XContentType.JSON).v2();
    }

    private ExpandUnmappedFieldsPostProcessor() {/* static class. */}
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.apache.lucene.util.BytesRef;
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
import org.elasticsearch.xpack.esql.plan.logical.UnmappedFieldsPattern;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.planner.UnmappedKeywordValues;
import org.elasticsearch.xpack.esql.session.Result;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.BiConsumer;

/**
 * Coordinator-side post-processor for {@code SET unmapped_fields="LOAD_ALL"}.
 *
 * <p>After all data-node pages are collected, flattens each row's {@code _unmapped_fields} JSON object to its leaves and replaces that
 * column with one {@code keyword} column per unique leaf path: dotted for nested objects, multivalue for arrays, no column of its own for
 * an object (matching the {@code null} an explicit reference to it reads), and {@code null} where a row lacks the leaf. Flattening lets a
 * synthetic-source index, which rebuilds a dotted source key as a nested object, expand to the same columns as a stored-source one.
 *
 * <p>The data node ships whole objects (it can only filter by top-level source key, pruning a subtree solely when a wildcard
 * {@code DROP} covers it), so this post-processor is where the {@link UnmappedFieldsPattern} is applied per flattened <em>leaf</em>
 * name.
 *
 * <p>A leaf is not a column when {@code KEEP} is resolved, so the plan cannot position it; the column order is re-derived here. When a
 * top {@code KEEP} governs the output (carried as {@link UnmappedFieldsAttribute#keepOrder()}), {@link UnmappedFieldsPattern#keepOrdered}
 * replays its left-to-right ordering over the real columns plus the discovered leaves so the response honors {@code KEEP}'s contract;
 * otherwise the leaves keep their natural real-then-alphabetical position.
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
        UnmappedFieldsAttribute unmappedAttribute = (UnmappedFieldsAttribute) schema.get(unmappedIdx);
        UnmappedFieldsPattern pattern = unmappedAttribute.pattern();

        // From here on we own the input pages: on success rewritePage drains them one by one, on any failure we release whatever
        // is left below. Page#releaseBlocks is idempotent, so re-releasing pages rewritePage already drained is a no-op.
        boolean success = false;
        try {
            var fieldNames = collectFieldNames(result, unmappedIdx, pattern, blockFactory.breaker(), reservationFactor);
            Set<String> existingNames = existingColumnNames(schema, unmappedIdx);
            List<String> leafNames = new ArrayList<>(fieldNames.size());
            // A leaf that collides with an existing column is dropped, not an error: with flattening a leaf mapped in one index can also
            // appear in another's _source, and the per-shard UnmappedKeywordBlockLoader already filled that column, so the value is kept.
            for (String name : fieldNames) {
                if (existingNames.contains(name) == false) {
                    leafNames.add(name);
                }
            }
            // TODO account for newSchema's field names against the circuit breaker. A wide _source turns into a wide schema, and
            // unlike the pages, the response schema has no breaker-tracked lifetime to release it against today.
            ExpandedLayout layout = computeLayout(schema, unmappedIdx, leafNames, unmappedAttribute.keepOrder());
            List<Page> newPages = rewritePages(result, unmappedIdx, leafNames, layout.blockOrder(), blockFactory, reservationFactor);

            Result expanded = new Result(
                layout.schema(),
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
     * Collect the unique leaf field names (sorted) carried by {@code _unmapped_fields} across all pages.
     * <p>
     * TODO cap this set. Every distinct leaf in any row's {@code _source} becomes an output column, so a wide, deep or
     *  heterogeneous index can blow the response up into thousands of columns.
     * <p>
     * TODO walk the JSON with a parser instead of materialising a whole {@code Map} only to flatten it to leaf names. That would
     *  also make the reservation below unnecessary.
     */
    private static SortedSet<String> collectFieldNames(
        Result result,
        int unmappedIdx,
        UnmappedFieldsPattern pattern,
        CircuitBreaker breaker,
        double reservationFactor
    ) {
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
                    collectLeaves("", parseJson(json), (name, value) -> fieldNames.add(name));
                } finally {
                    breaker.addWithoutBreaking(-reservation);
                }
            }
        }
        fieldNames.removeIf(name -> pattern.matches(name) == false);
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

    /** The names of every column except {@code _unmapped_fields}; used to keep an expanded leaf from shadowing a query column. */
    private static Set<String> existingColumnNames(List<Attribute> schema, int unmappedIdx) {
        Set<String> existingNames = new HashSet<>();
        for (int i = 0; i < schema.size(); i++) {
            if (i != unmappedIdx) {
                existingNames.add(schema.get(i).name());
            }
        }
        return existingNames;
    }

    /**
     * The expanded output layout: the reordered {@code schema} and, per output column, where its block comes from.
     */
    private record ExpandedLayout(List<Attribute> schema, int[] blockOrder) {}

    /**
     * Builds the expanded output layout: the final column order plus, per column, which block feeds it.
     */
    private static ExpandedLayout computeLayout(
        List<Attribute> schema,
        int unmappedIdx,
        List<String> leafNames,
        List<UnmappedFieldsPattern.KeepTerm> keepOrder
    ) {
        int originalColumnCount = schema.size();
        List<String> keptRealNames = new ArrayList<>();
        List<String> appendedRealNames = new ArrayList<>();
        Map<String, Integer> nameToSchemaIdx = new HashMap<>();
        // DetermineUnmappedFieldsToKeep pins the synthetic attribute right after the governing KEEP's projections, so a real column
        // before unmappedIdx was KEEP-selected (its order is replayable) and one after it was appended by a later EVAL (must trail).
        for (int i = 0; i < originalColumnCount; i++) {
            if (i != unmappedIdx) {
                String name = schema.get(i).name();
                nameToSchemaIdx.put(name, i);
                (i < unmappedIdx ? keptRealNames : appendedRealNames).add(name);
            }
        }
        Map<String, Integer> leafNameToIdx = new HashMap<>();
        for (int i = 0; i < leafNames.size(); i++) {
            leafNameToIdx.put(leafNames.get(i), i);
        }

        List<String> orderedNames;
        if (keepOrder.isEmpty()) {
            // No governing KEEP: natural order - every real column in schema order, then the expanded leaves.
            orderedNames = new ArrayList<>(originalColumnCount - 1 + leafNames.size());
            orderedNames.addAll(keptRealNames);
            orderedNames.addAll(appendedRealNames);
            orderedNames.addAll(leafNames);
        } else {
            List<String> keepScope = new ArrayList<>(keptRealNames.size() + leafNames.size());
            keepScope.addAll(keptRealNames);
            keepScope.addAll(leafNames);
            orderedNames = UnmappedFieldsPattern.keepOrdered(keepScope, keepOrder);
            orderedNames.addAll(appendedRealNames);
        }

        List<Attribute> newSchema = new ArrayList<>(orderedNames.size());
        int[] blockOrder = new int[orderedNames.size()];
        for (int pos = 0; pos < orderedNames.size(); pos++) {
            String name = orderedNames.get(pos);
            Integer schemaIdx = nameToSchemaIdx.get(name);
            if (schemaIdx != null) {
                newSchema.add(schema.get(schemaIdx));
                blockOrder[pos] = schemaIdx;
            } else {
                int leafIdx = leafNameToIdx.getOrDefault(name, -1);
                if (leafIdx < 0) {
                    throw new IllegalStateException("ordered name [" + name + "] is neither a retained column nor an expanded leaf");
                }
                newSchema.add(new ReferenceAttribute(Source.EMPTY, null, name, DataType.KEYWORD));
                blockOrder[pos] = originalColumnCount + leafIdx;
            }
        }
        return new ExpandedLayout(newSchema, blockOrder);
    }

    /** Rewrite each page, replacing the {@code _unmapped_fields} block with one block per expanded field name, in {@code blockOrder}. */
    private static List<Page> rewritePages(
        Result result,
        int unmappedIdx,
        List<String> leafNames,
        int[] blockOrder,
        BlockFactory factory,
        double reservationFactor
    ) {
        int originalColumnCount = result.schema().size();
        // The authoritative set of surviving leaf names: already pattern-filtered by collectFieldNames and collision-pruned by expand().
        // The per-row leaf sink keeps only these, so schema and values are driven by one source of truth and dropped leaves never build.
        Set<String> keep = Set.copyOf(leafNames);
        var newPages = new ArrayList<Page>(result.pages().size());
        var success = false;
        try {
            for (Page p : result.pages()) {
                newPages.add(rewritePage(unmappedIdx, keep, leafNames, blockOrder, originalColumnCount, factory, p, reservationFactor));
            }
            success = true;
            return newPages;
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(newPages);
            }
        }
    }

    private static Page rewritePage(
        int unmappedIdx,
        Set<String> keep,
        List<String> leafNames,
        int[] blockOrder,
        int originalColumnCount,
        BlockFactory blockFactory,
        Page page,
        double reservationFactor
    ) {
        // Output blocks follow blockOrder: a retained column (code < originalColumnCount) keeps its original block, an expanded leaf
        // gets a freshly built one. The retained/leaf split is disjoint (expand() drops colliding leaves) so each position has one source.
        int leafCount = leafNames.size();
        Block[] allBlocks = new Block[blockOrder.length];

        var success = false;
        BytesRefBlock.Builder[] builders = new BytesRefBlock.Builder[leafCount];
        try (var ignored = Releasables.wrap(builders)) {
            // Place the retained blocks now (incRef so releasing the input page below leaves them alive) and record where each leaf's
            // built block goes, so the expansion below builds straight into its final position.
            int[] leafOutputPos = new int[leafCount];
            for (int pos = 0; pos < blockOrder.length; pos++) {
                int code = blockOrder[pos];
                if (code < originalColumnCount) {
                    var block = page.getBlock(code);
                    block.incRef();
                    allBlocks[pos] = block;
                } else {
                    leafOutputPos[code - originalColumnCount] = pos;
                }
            }

            // Zero expanded columns means nothing to expand, so just drop the _unmapped_fields column, keep any retained blocks,
            // and skip the wasted per-row _source re-parse.
            if (leafCount > 0) {
                BytesRefBlock unmappedBlock = page.getBlock(unmappedIdx);
                Arrays.setAll(builders, i -> blockFactory.newBytesRefBlockBuilder(page.getPositionCount()));
                // valueScratch and leaves are reused across rows: valueScratch holds one leaf's keyword values before they are appended,
                // leaves holds the current row's flattened leaf-path to value map. jsonScratch grows to the largest value seen in this
                // page, so it is per-page rather than per-result.
                var jsonScratch = new BytesRef();
                List<BytesRef> valueScratch = new ArrayList<>();
                Map<String, Object> leaves = new HashMap<>();
                BiConsumer<String, Object> leafSink = (name, value) -> {
                    if (keep.contains(name)) {
                        collectLeaf(leaves, name, value);
                    }
                };
                CircuitBreaker breaker = blockFactory.breaker();
                for (int row = 0; row < page.getPositionCount(); row++) {
                    if (unmappedBlock.isNull(row)) {
                        appendRow(Map.of(), leafNames, builders, valueScratch);
                        continue;
                    }
                    BytesRef json = getBytesRef(unmappedBlock, row, jsonScratch);
                    long reservation = reserveForParse(json, breaker, reservationFactor);
                    try {
                        leaves.clear();
                        collectLeaves("", parseJson(json), leafSink);
                        appendRow(leaves, leafNames, builders, valueScratch);
                    } finally {
                        breaker.addWithoutBreaking(-reservation);
                    }
                }
                for (int i = 0; i < builders.length; i++) {
                    allBlocks[leafOutputPos[i]] = builders[i].build();
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
     * Appends this row's value for each expanded leaf: {@code null} where the row lacks the leaf or the leaf is an object, a single
     * keyword for a scalar, and a multivalue for an array (see {@link UnmappedKeywordValues}).
     * <p>
     * TODO each scalar is copied twice: {@link UnmappedKeywordValues} renders and UTF-8 encodes it into a fresh {@link BytesRef} and the
     *  builder copies those bytes again. Values that are already {@code String}s could go straight into the builder's byte array.
     */
    private static void appendRow(
        Map<String, Object> leaves,
        List<String> leafNames,
        BytesRefBlock.Builder[] builders,
        List<BytesRef> valueScratch
    ) {
        for (int i = 0; i < builders.length; i++) {
            valueScratch.clear();
            UnmappedKeywordValues.collect(leaves.get(leafNames.get(i)), valueScratch);
            if (valueScratch.isEmpty()) {
                builders[i].appendNull();
            } else if (valueScratch.size() == 1) {
                builders[i].appendBytesRef(valueScratch.get(0));
            } else {
                builders[i].beginPositionEntry();
                for (BytesRef value : valueScratch) {
                    builders[i].appendBytesRef(value);
                }
                builders[i].endPositionEntry();
            }
        }
    }

    /** Walks a parsed source object, invoking {@code sink} once per leaf with its dotted path and value (see {@link #collectValue}). */
    private static void collectLeaves(String prefix, Map<?, ?> map, BiConsumer<String, Object> sink) {
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            String name = prefix.isEmpty() ? String.valueOf(entry.getKey()) : prefix + "." + entry.getKey();
            collectValue(name, entry.getValue(), sink);
        }
    }

    /**
     * Emits the leaves of one {@code _source} value at {@code name}: an object recurses to dotted leaves, an array recurses element-wise
     * (so objects inside it flatten to the same leaves a sibling index mapping those subfields would surface, and scalar elements stay at
     * {@code name}), and any other value is a leaf as-is.
     */
    private static void collectValue(String name, Object value, BiConsumer<String, Object> sink) {
        if (value instanceof Map<?, ?> child) {
            collectLeaves(name, child, sink);
        } else if (value instanceof List<?> list) {
            for (Object element : list) {
                collectValue(name, element, sink);
            }
        } else {
            sink.accept(name, value);
        }
    }

    private static void collectLeaf(Map<String, Object> leaves, String name, Object value) {
        if (leaves.containsKey(name) == false) {
            leaves.put(name, value);
            return;
        }
        List<Object> combined = new ArrayList<>();
        flattenInto(combined, leaves.get(name));
        flattenInto(combined, value);
        leaves.put(name, combined);
    }

    private static void flattenInto(List<Object> combined, Object value) {
        if (value instanceof List<?> list) {
            combined.addAll(list);
        } else if (value != null) {
            combined.add(value);
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
        // Ordered so a row that produces the same leaf twice (a literal dotted key overlapping a nested path) merges its values in a
        // deterministic source order rather than an arbitrary HashMap iteration order.
        return XContentHelper.convertToMap(new BytesArray(ref.bytes, ref.offset, ref.length), true, XContentType.JSON).v2();
    }

    private ExpandUnmappedFieldsPostProcessor() {/* static class. */}
}

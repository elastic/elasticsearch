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
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Strings;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.analysis.UnmappedFieldsOrdering;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.plan.logical.UnmappedFieldsAttribute;
import org.elasticsearch.xpack.esql.plan.logical.UnmappedFieldsPattern;
import org.elasticsearch.xpack.esql.planner.UnmappedKeywordValues;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.BiConsumer;

import static org.elasticsearch.xpack.esql.approximation.ApproximationPlan.isApproximationColumn;

/**
 * Coordinator-side {@link Operator} for {@code SET unmapped_fields="LOAD_ALL"}.
 *
 * <p>Runs in a dedicated coordinator {@link org.elasticsearch.compute.operator.Driver} (see
 * {@code ComputeService#expandUnmappedFields}) after the query's compute has finished. Modelling the expansion as an operator lets the
 * compute framework own its yielding, cancellation, profiling and task description rather than the bespoke machinery the previous
 * post-processor needed.
 *
 * <p>Flattens each row's {@code _unmapped_fields} JSON object to its leaves and replaces that column with one {@code keyword} column per
 * unique leaf path: dotted for nested objects, multivalue for arrays, no column of its own for an object (matching the {@code null} an
 * explicit reference to it reads), and {@code null} where a row lacks the leaf. Flattening lets a synthetic-source index, which rebuilds
 * a dotted source key as a nested object, expand to the same columns as a stored-source one.
 *
 * <p>The data node ships whole objects (it can only filter by top-level source key, pruning a subtree solely when a wildcard
 * {@code DROP} covers it), so this operator is where the {@link UnmappedFieldsPattern} is applied per flattened <em>leaf</em> name.
 *
 * <p>A discovered field is not a column when {@code KEEP} is resolved, so the plan could not position it then.
 * {@link UnmappedFieldsOrdering} hands the discovered fields back to the plan as if they had been mapped all along and asks it for its
 * output: every {@code KEEP}/{@code DROP}/{@code RENAME} re-resolves itself, and {@code EVAL} columns trail the discovered fields because
 * the discovered fields take the relation slot the synthetic column occupied. Approximation extras ({@code _approximation_*}) are added
 * after analysis, so they are held out of that replay and pinned last.
 *
 * <p>The data node only puts keys into the column that hold a value, so no expanded column comes out null in every row -
 * {@link #assertNoAllNullExpandedColumn} holds that end of the contract down.
 *
 * <h2>Lifecycle</h2>
 * The operator is two-phase, mirroring {@code TopNOperator} / {@code HashAggregationOperator}. During the collect phase
 * {@link #addInput} buffers each page and merges that page's leaf names into a running union - one page per driver iteration, so the
 * driver yields between pages. {@link #finish} freezes the union, applies the pattern and computes the expanded output layout via
 * {@link UnmappedFieldsOrdering}. During the emit phase {@link #getOutput} rewrites and releases exactly one buffered page per call, so
 * the driver again yields between pages. Cancellation is driven by the framework: the driver polls
 * {@link DriverContext#checkForEarlyTermination()} between operator invocations, and both inner loops poll it every
 * {@link #ROWS_PER_CANCELLATION_CHECK} rows so a single wide page still aborts promptly.
 *
 * <p>Buffering every page holds the same footprint the {@code Result} held before the expansion ran (no regression); bounding the
 * per-row memory a wide {@code _source} can demand stays the circuit breaker's job.
 *
 * <p>TODO every row's {@code _source} ends up parsed three times: the data node parses it to filter the column, then the coordinator
 *  parses the column once to collect field names and once more to expand them. A columnar shape — one block of names and one of values —
 *  would let us build the union while reading and expand without re-parsing.
 */
public final class ExpandUnmappedFieldsOperator implements Operator {
    /**
     * How often the collect and emit loops poll {@link DriverContext#checkForEarlyTermination()}. Each phase is a scan over every row of
     * every page (parsing each row's {@code _source} JSON), so for a wide or high-row {@code LOAD_ALL} result a single page can run for a
     * while. Polling every {@value} rows keeps cancellation latency to a small fraction of a page while adding no measurable overhead per
     * row. Must stay a power of two for the bit-mask test below.
     */
    private static final int ROWS_PER_CANCELLATION_CHECK = 1024;

    static {
        assert Integer.bitCount(ROWS_PER_CANCELLATION_CHECK) == 1 : "ROWS_PER_CANCELLATION_CHECK must be a power of two for the bit-mask";
    }

    /**
     * Test-only seam invoked once at the start of {@link #finish} - after every page has been collected but before any page is emitted.
     * Production never installs a hook (the field stays {@code null}), so this adds a single volatile read per {@code LOAD_ALL} response
     * and nothing otherwise. {@code LoadAllCancellationIT} installs a hook that blocks until it has cancelled the task, which lets it
     * deterministically land a cancellation inside an in-progress expansion (rather than during the compute phase, where the drivers
     * would abort first) and assert that the driver's early-termination poll then aborts it and releases the buffered pages. Volatile so
     * the driver thread running the expansion observes the test's write.
     */
    static volatile Runnable expansionStartedForTest = null;

    private final DriverContext driverContext;
    private final BlockFactory blockFactory;
    private final double reservationFactor;
    @Nullable
    private final UnmappedFieldsOrdering ordering;
    private final List<Attribute> inputSchema;
    private final int unmappedIdx;
    private final UnmappedFieldsPattern pattern;

    /**
     * Buffered input pages, in arrival order. A slot is nulled as {@link #getOutput} drains it so a later {@link #close} cannot
     * re-release it.
     */
    private final List<Page> buffer = new ArrayList<>();
    /** Running union of the {@code _unmapped_fields} leaf names seen across the pages collected so far. */
    private final SortedSet<String> fieldNames = new TreeSet<>();
    private final BytesRef collectScratch = new BytesRef();

    private boolean finished = false;
    private int nextToEmit = 0;

    // Computed by finish(); read by getOutput() while draining and by expandedSchema() once the driver has completed.
    private List<Attribute> expandedSchema;
    private List<String> expandedFieldNames;
    private Set<String> keep;
    private int[] blockOrder;
    /** Per expanded column, whether any emitted page carried a non-null value for it - see {@link #assertNoAllNullExpandedColumn}. */
    private boolean[] expandedSawValue;

    public ExpandUnmappedFieldsOperator(
        DriverContext driverContext,
        List<Attribute> inputSchema,
        @Nullable UnmappedFieldsOrdering ordering,
        double reservationFactor
    ) {
        this.driverContext = driverContext;
        this.blockFactory = driverContext.blockFactory();
        this.reservationFactor = reservationFactor;
        this.ordering = ordering;
        this.inputSchema = inputSchema;
        this.unmappedIdx = CollectionUtils.findIndex(inputSchema, e -> e instanceof UnmappedFieldsAttribute);
        if (unmappedIdx == -1) {
            throw new IllegalStateException("schema has no _unmapped_fields column to expand: " + inputSchema);
        }
        this.pattern = ((UnmappedFieldsAttribute) inputSchema.get(unmappedIdx)).pattern();
    }

    /**
     * Whether a result with this schema still carries the synthetic {@code _unmapped_fields} column added by
     * {@code SET unmapped_fields="LOAD_ALL"}, i.e. whether it needs expanding. Lets callers skip building an expansion driver entirely
     * for the common non-{@code LOAD_ALL} query.
     */
    public static boolean hasUnmappedFields(List<Attribute> schema) {
        return CollectionUtils.findIndex(schema, e -> e instanceof UnmappedFieldsAttribute) != -1;
    }

    @Override
    public boolean needsInput() {
        return finished == false;
    }

    @Override
    public void addInput(Page page) {
        boolean success = false;
        try {
            buffer.add(page);
            collectFieldNames(page);
            success = true;
        } finally {
            if (success == false) {
                page.releaseBlocks();
                buffer.removeLast();
            }
        }
    }

    @Override
    public void finish() {
        if (finished) {
            return;
        }
        // Run the test seam before computing the layout so a blocking hook that cancels the task lands before the emit phase, exactly
        // where a real cancellation of an in-progress expansion would.
        Runnable expansionStarted = expansionStartedForTest;
        if (expansionStarted != null) {
            expansionStarted.run();
        }

        fieldNames.removeIf(name -> pattern.matches(name) == false);
        Set<String> existingNames = existingColumnNames(inputSchema, unmappedIdx);
        expandedFieldNames = new ArrayList<>(fieldNames.size());
        // A discovered field name that collides with an existing column name is dropped, not an error: with flattening a discovered
        // field mapped in one index can also appear in another's _source, and the per-shard UnmappedKeywordBlockLoader already filled
        // that column, so the value is kept.
        for (String name : fieldNames) {
            if (existingNames.contains(name) == false) {
                expandedFieldNames.add(name);
            }
        }
        // TODO account for expandedSchema's field names against the circuit breaker. A wide _source turns into a wide schema, and
        // unlike the pages, the response schema has no breaker-tracked lifetime to release it against today.
        ExpandedLayout layout = computeLayout(inputSchema, unmappedIdx, expandedFieldNames, ordering);
        expandedSchema = layout.schema();
        blockOrder = layout.blockOrder();
        keep = Set.copyOf(expandedFieldNames);
        expandedSawValue = new boolean[expandedFieldNames.size()];
        finished = true;
    }

    @Override
    public boolean isFinished() {
        return finished && nextToEmit >= buffer.size();
    }

    @Override
    public boolean canProduceMoreDataWithoutExtraInput() {
        return finished && nextToEmit < buffer.size();
    }

    @Override
    public Page getOutput() {
        if (finished == false || nextToEmit >= buffer.size()) {
            return null;
        }
        Page input = buffer.get(nextToEmit);
        // Null the slot before rewriting so a rewrite failure (which releases input) cannot leave a page close() would re-release.
        buffer.set(nextToEmit, null);
        nextToEmit++;
        Page output = rewritePage(input);
        try {
            // Assert-only guard; it reads the running expandedSawValue tally rather than the already-emitted pages, so it holds even
            // after the pages have been handed downstream.
            assert nextToEmit < buffer.size() || assertNoAllNullExpandedColumn();
        } catch (AssertionError e) {
            // The guard trips after this last page was built but before it was handed downstream, so release it here.
            output.releaseBlocks();
            throw e;
        }
        return output;
    }

    /** The expanded output schema. Only valid once {@link #finish} has run; safe to read after the driver has completed. */
    public List<Attribute> expandedSchema() {
        assert finished : "expandedSchema() read before finish()";
        return expandedSchema;
    }

    @Override
    public void close() {
        // Emitted slots have been nulled by getOutput(); Releasables skips nulls, so this releases only the pages still buffered.
        List<Page> remaining = new ArrayList<>(buffer);
        buffer.clear();
        Releasables.closeExpectNoException(remaining);
    }

    @Override
    public String toString() {
        return "ExpandUnmappedFieldsOperator[pattern=" + pattern + "]";
    }

    /**
     * Merge one page's {@code _unmapped_fields} leaf names into {@link #fieldNames}. Every key here earns an output column, which is why
     * the data node drops the keys that carry no value - see {@code UnmappedFieldsBlockLoader} and {@link #assertNoAllNullExpandedColumn}.
     * <p>
     * TODO cap this set. Every distinct key in any row's {@code _source} becomes an output column, so a wide or heterogeneous index can
     *  blow the response up into thousands of columns.
     * <p>
     * TODO walk the JSON with a parser instead of materialising a whole {@code Map} only to flatten it to field names. That would also
     *  make the reservation below unnecessary.
     */
    private void collectFieldNames(Page page) {
        CircuitBreaker breaker = blockFactory.breaker();
        BytesRefBlock unmappedBlock = page.getBlock(unmappedIdx);
        for (int row = 0; row < unmappedBlock.getPositionCount(); row++) {
            if ((row & (ROWS_PER_CANCELLATION_CHECK - 1)) == 0) {
                driverContext.checkForEarlyTermination();
            }
            if (unmappedBlock.isNull(row)) {
                continue;
            }
            BytesRef json = getBytesRef(unmappedBlock, row, collectScratch);
            long reservation = reserveForParse(json, breaker, reservationFactor);
            try {
                collectLeaves("", parseJson(json), (name, value) -> fieldNames.add(name));
            } finally {
                breaker.addWithoutBreaking(-reservation);
            }
        }
    }

    /**
     * Reserves memory for one {@link #parseJson} call, which allocates a {@code Map} nothing else accounts for. The multiplier records
     * the measured ~8x blow-up of parsing {@code _source} into a map - the very same parse this column goes through a second time here.
     *
     * @return the number of reserved bytes, to be handed back with {@link CircuitBreaker#addWithoutBreaking} once the map is gone
     */
    private static long reserveForParse(BytesRef json, CircuitBreaker breaker, double reservationFactor) {
        long reservation = (long) (json.length * reservationFactor);
        breaker.addEstimateBytesAndMaybeBreak(reservation, "unmapped fields expansion");
        return reservation;
    }

    /**
     * The names of every column except {@code _unmapped_fields}. A discovered field colliding with one of them is dropped rather than
     * expanded, so a discovered field can never shadow a query column.
     */
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
     * Builds the expanded output layout by asking {@code ordering} where the discovered fields belong: it hands them to the plan as if
     * they had been mapped all along, so {@code KEEP}/{@code DROP}/{@code RENAME} re-resolve themselves rather than being re-implemented
     * here. Discovered fields are recognised by {@link NameId} — the attributes handed out below are the very ones that come back —
     * while real columns match by name, which survives the optimizer minting new ids.
     */
    private static ExpandedLayout computeLayout(
        List<Attribute> schema,
        int unmappedIdx,
        List<String> expandedFieldsNames,
        @Nullable UnmappedFieldsOrdering ordering
    ) {
        int originalColumnCount = schema.size();
        List<Attribute> expandedFieldsAttributes = new ArrayList<>(expandedFieldsNames.size());
        Map<NameId, Integer> expandedAttributeIdToIdx = new HashMap<>(expandedFieldsNames.size());
        for (int i = 0; i < expandedFieldsNames.size(); i++) {
            Attribute attr = new ReferenceAttribute(Source.EMPTY, null, expandedFieldsNames.get(i), DataType.KEYWORD);
            expandedFieldsAttributes.add(attr);
            expandedAttributeIdToIdx.put(attr.id(), i);
        }
        // Approximation extras (_approximation_*) are appended after analysis, so the replayed plan knows nothing about them. They
        // are held out of the ordering and pinned last, which is where the response wants them anyway.
        // TODO: revisit approximation under LOAD_ALL. A top-level STATS makes the pattern NONE, so the synthetic column is never
        // planned and the two cannot co-occur; INLINE STATS does plan it, so confirm whether approximation can reach this path.
        Map<String, Integer> nameToSchemaIdx = new HashMap<>();
        List<Integer> approximationIdx = new ArrayList<>();
        for (int i = 0; i < originalColumnCount; i++) {
            if (i == unmappedIdx) {
                continue;
            }
            if (isApproximationColumn(schema.get(i).name())) {
                approximationIdx.add(i);
            } else {
                nameToSchemaIdx.put(schema.get(i).name(), i);
            }
        }
        int dataColumnCount = originalColumnCount - 1 - approximationIdx.size();

        List<Attribute> orderedExpandedAttributes = ordering == null ? null : ordering.order(expandedFieldsAttributes);
        if (orderedExpandedAttributes == null || orderedExpandedAttributes.size() != dataColumnCount + expandedFieldsAttributes.size()) {
            // Either nothing was captured, or the replay disagrees with the executed schema because something rewrote the shape
            // after analysis. Fall back to the natural real-then-discovered order rather than dropping or duplicating a column.
            assert ordering == null
                : Strings.format(
                    "unmapped fields ordering replay diverged from the executed schema: replay=%s executedWithoutUfa=%s discovered=%s",
                    orderedExpandedAttributes == null ? "null" : orderedExpandedAttributes.stream().map(Attribute::name).toList(),
                    nameToSchemaIdx.keySet(),
                    expandedFieldsNames
                );
            orderedExpandedAttributes = new ArrayList<>(dataColumnCount + expandedFieldsAttributes.size());
            for (int i = 0; i < originalColumnCount; i++) {
                if (i != unmappedIdx && isApproximationColumn(schema.get(i).name()) == false) {
                    orderedExpandedAttributes.add(schema.get(i));
                }
            }
            orderedExpandedAttributes.addAll(expandedFieldsAttributes);
        }

        List<Attribute> newSchema = new ArrayList<>(orderedExpandedAttributes.size() + approximationIdx.size());
        int[] blockOrder = new int[orderedExpandedAttributes.size() + approximationIdx.size()];
        int pos = 0;
        for (Attribute attribute : orderedExpandedAttributes) {
            Integer attributeIdx = expandedAttributeIdToIdx.get(attribute.id());
            if (attributeIdx != null) {
                newSchema.add(attribute);
                blockOrder[pos++] = originalColumnCount + attributeIdx;
                continue;
            }
            Integer schemaIdx = nameToSchemaIdx.get(attribute.name());
            if (schemaIdx == null) {
                throw new IllegalStateException(
                    "orderedExpandedAttributes column [" + attribute.name() + "] is neither a retained column nor a discovered field"
                );
            }
            newSchema.add(schema.get(schemaIdx));
            blockOrder[pos++] = schemaIdx;
        }
        for (int idx : approximationIdx) {
            newSchema.add(schema.get(idx));
            blockOrder[pos++] = idx;
        }
        return new ExpandedLayout(newSchema, blockOrder);
    }

    /**
     * Guard rail for what {@code UnmappedFieldsBlockLoader} promises this class: every key it writes into {@code _unmapped_fields}
     * holds a value, and that value is what {@link #appendRow} writes back, so no expanded column can come out {@code null} in every
     * row. Tracked incrementally as pages are emitted ({@link #expandedSawValue}) and checked once the last page has been rewritten.
     * <p>
     * Only the expanded columns are checked: a retained column can legitimately be all null, e.g. {@code KEEP field_absent_everywhere}
     * resolves to a {@code null} literal.
     *
     * @return {@code true}, so this can be called from an {@code assert} and skipped entirely in production
     */
    private boolean assertNoAllNullExpandedColumn() {
        for (int i = 0; i < expandedSawValue.length; i++) {
            if (expandedSawValue[i] == false) {
                throw new AssertionError(
                    Strings.format("Expanded unmapped field '%s' into a column that is null in every row", expandedFieldNames.get(i))
                );
            }
        }
        return true;
    }

    /** Rewrite one page, replacing the {@code _unmapped_fields} block with one block per expanded field name, in {@link #blockOrder}. */
    private Page rewritePage(Page page) {
        int originalColumnCount = inputSchema.size();
        int expandedFieldsCount = expandedFieldNames.size();
        Block[] allBlocks = new Block[blockOrder.length];

        boolean success = false;
        BytesRefBlock.Builder[] builders = new BytesRefBlock.Builder[expandedFieldsCount];
        try (var ignored = Releasables.wrap(builders)) {
            int[] fieldOutputPos = new int[expandedFieldsCount];
            for (int pos = 0; pos < blockOrder.length; pos++) {
                int code = blockOrder[pos];
                if (code < originalColumnCount) {
                    var block = page.getBlock(code);
                    block.incRef();
                    allBlocks[pos] = block;
                } else {
                    fieldOutputPos[code - originalColumnCount] = pos;
                }
            }

            // Zero expanded columns means nothing to expand, so just drop the _unmapped_fields column, keep any retained blocks, and
            // skip the wasted per-row _source re-parse.
            if (expandedFieldsCount > 0) {
                BytesRefBlock unmappedBlock = page.getBlock(unmappedIdx);
                Arrays.setAll(builders, i -> blockFactory.newBytesRefBlockBuilder(page.getPositionCount()));
                // ------ Naming convention ------
                // "leaf" = JSON string
                // "discovered field" = a "leaf" outside of the JSON parsing flow; it's already in the analysis/logical plan land

                // valueScratch and fieldNameAndValues are reused across rows: valueScratch holds one leaf's keyword values before they
                // are appended, fieldNameAndValues holds the current row's flattened leaf-path to value map. jsonScratch grows to the
                // largest value seen in this page, so it is per-page rather than per-result.
                var jsonScratch = new BytesRef();
                List<BytesRef> valueScratch = new ArrayList<>();
                Map<String, Object> fieldNameAndValues = new HashMap<>();
                BiConsumer<String, Object> leafSink = (name, value) -> {
                    if (keep.contains(name)) {
                        collectLeaf(fieldNameAndValues, name, value);
                    }
                };
                CircuitBreaker breaker = blockFactory.breaker();
                for (int row = 0; row < page.getPositionCount(); row++) {
                    if ((row & (ROWS_PER_CANCELLATION_CHECK - 1)) == 0) {
                        driverContext.checkForEarlyTermination();
                    }
                    if (unmappedBlock.isNull(row)) {
                        appendRow(Map.of(), expandedFieldNames, builders, valueScratch);
                        continue;
                    }
                    BytesRef json = getBytesRef(unmappedBlock, row, jsonScratch);
                    long reservation = reserveForParse(json, breaker, reservationFactor);
                    try {
                        fieldNameAndValues.clear();
                        collectLeaves("", parseJson(json), leafSink);
                        appendRow(fieldNameAndValues, expandedFieldNames, builders, valueScratch);
                    } finally {
                        breaker.addWithoutBreaking(-reservation);
                    }
                }
                for (int i = 0; i < builders.length; i++) {
                    Block built = builders[i].build();
                    assert (expandedSawValue[i] |= built.areAllValuesNull() == false) || true;
                    allBlocks[fieldOutputPos[i]] = built;
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
                page.releaseBlocks();
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
                for (BytesRef scratched : valueScratch) {
                    builders[i].appendBytesRef(scratched);
                }
                builders[i].endPositionEntry();
            }
        }
    }

    /**
     * Guard rail for the other half of what {@code UnmappedFieldsBlockLoader} promises: whatever it wrote under a key is pruned, so it
     * holds no {@code null} inside an array or object and no empty array or object at any depth. {@link #appendRow} renders the whole
     * value, so a {@code null} that survived would reach the user as a literal {@code "null"} inside a stringified array - where a
     * mapped field would have produced a multi-value, and multi-values never contain nulls.
     *
     * @return {@code true}, so this can be called from an {@code assert} and skipped entirely in production
     */
    private static boolean assertPruned(String fieldName, Object value) {
        if (isPruned(value) == false) {
            throw new AssertionError(
                Strings.format("Unmapped field '%s' carries a null or an empty array or object: [%s]", fieldName, value)
            );
        }
        return true;
    }

    /** Whether {@code value} is neither {@code null}, nor an empty container, nor a container hiding either of those at any depth. */
    private static boolean isPruned(Object value) {
        Collection<?> elements;
        if (value instanceof List<?> values) {
            elements = values;
        } else if (value instanceof Map<?, ?> map) {
            elements = map.values();
        } else {
            return value != null;
        }
        if (elements.isEmpty()) {
            return false;
        }
        for (Object element : elements) {
            if (isPruned(element) == false) {
                return false;
            }
        }
        return true;
    }

    /** Walks a parsed source object, invoking {@code sink} once per leaf with its dotted path and value (see {@link #collectValue}). */
    private static void collectLeaves(String prefix, Map<?, ?> map, BiConsumer<String, Object> sink) {
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            String name = prefix.isEmpty() ? String.valueOf(entry.getKey()) : prefix + "." + entry.getKey();
            // Top level only: the block loader prunes whole _source keys, so that is the granularity its promise is made at. A bare
            // null is left to assertNoAllNullExpandedColumn, which reports it as the all-null column it actually becomes.
            assert prefix.isEmpty() == false || entry.getValue() == null || assertPruned(name, entry.getValue());
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
}

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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Strings;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.support.AbstractXContentParser;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.plan.logical.UnmappedFieldsAttribute;
import org.elasticsearch.xpack.esql.plan.logical.UnmappedFieldsPattern;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.session.Result;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;

/**
 * Coordinator-side post-processor for {@code SET unmapped_fields="LOAD_ALL"}.
 *
 * <p>After all data-node pages are collected, flattens each row's {@code $$unmapped_fields} JSON object to its leaves and re-serialises
 * them as a single JSON object under a {@code _unmapped_fields} column of type {@link DataType#SOURCE} (like the {@code _source}
 * column of {@code METADATA _source}, but a flattened view of it rather than a structural subset). Field names are flattened:
 * {@code {"foo":{"bar":1}}} in {@code _source} becomes {@code {"foo.bar":1}} in {@code _unmapped_fields}. Arrays follow multivalue
 * semantics: a leaf with several values becomes a JSON array at the same (dotted) name, while a one-element array is
 * indistinguishable from a scalar and renders as the bare value ({@code {"a":[1]}} becomes {@code {"a":1}}). A row whose object
 * contributes no surviving leaf gets a {@code null}.
 *
 * <p>The column is not expanded into individual columns (see
 * <a href="https://github.com/elastic/elasticsearch/issues/161717">#161717</a>): a practical {@code _source} can hold a large number
 * of distinct unmapped fields, which does not compose well as one column per field. Returning a single {@code SOURCE} blob keeps the
 * response shape bounded and mirrors {@code METADATA _source}.
 *
 * <p>The data node ships whole objects (it can only filter by top-level source key, pruning a subtree solely when a wildcard
 * {@code DROP} covers it), so this post-processor is where the {@link UnmappedFieldsPattern} is applied per flattened <em>leaf</em>
 * name, and where a leaf colliding with a real output column is dropped so a discovered field can never shadow a query column.
 *
 * <p>The {@code _unmapped_fields} column takes the slot the synthetic column occupied in the executed schema; it is produced after
 * analysis and is not referable from the query language (the synthetic {@code $$}-prefixed name kept it unreachable during planning).
 * If the query itself already outputs a column named {@code _unmapped_fields}, that column wins, as with any other colliding leaf:
 * the discovered fields are dropped and no second column of that name is produced.
 *
 * <p>Each row's JSON is streamed with a parser and never materialised as a {@code Map}, so a structure-heavy row (an array of many
 * tiny objects) costs only the leaves it flattens to, not an object graph many times its byte size.
 *
 * <p>The flattening scans every row of every page once, so it polls for cancellation at the start of every page and then every
 * {@link #ROWS_PER_CANCELLATION_CHECK} rows within one, and throws {@link TaskCancelledException} to abort promptly. Cancellation is
 * row-granular: a single pathological row (deeply nested {@code _source} with very many leaves) still flattens to completion before
 * the next poll. That is acceptable because the cost scales with row count rather than any one row, and the circuit breaker - not
 * cancellation - is what bounds the memory a wide row can demand.
 *
 * <p>TODO every row's {@code _source} ends up parsed twice: the data node parses it to filter the column, then the coordinator parses
 *  the column once more here to flatten it. #161644 moves the per-leaf filtering and the streaming {@code _source} read to the shard
 *  (the first half of #158479); what remains is flattening on the shard too, so the column ships as-is with no reparse here.
 */
public final class ExpandUnmappedFieldsPostProcessor {
    /** The user-visible name of the single flattened unmapped-fields column this post-processor emits. */
    public static final String UNMAPPED_FIELDS_COLUMN_NAME = "_unmapped_fields";

    /**
     * How often the flattening loop polls {@code isCancelled}. The flattening is a coordinator-side, single-threaded scan over every
     * row of every page (parsing each row's {@code _source} JSON), so for a high-row {@code LOAD_ALL} result it can run for seconds.
     * Polling every {@value} rows keeps cancellation latency to a small fraction of a page while adding no measurable overhead per row.
     * Must stay a power of two for the bit-mask test below.
     */
    private static final int ROWS_PER_CANCELLATION_CHECK = 1024;

    static {
        assert Integer.bitCount(ROWS_PER_CANCELLATION_CHECK) == 1 : "ROWS_PER_CANCELLATION_CHECK must be a power of two for the bit-mask";
    }

    /**
     * Test-only seam invoked once at the start of the flattening phase — after a {@code $$unmapped_fields} column has been confirmed
     * present but before any page is scanned. Production never installs a hook (the field stays {@code null}), so this adds a single
     * volatile read per {@code LOAD_ALL} response and nothing otherwise. {@code LoadAllCancellationIT} installs a hook that blocks
     * until it has cancelled the task, which lets it deterministically land a cancellation inside an in-progress flattening (rather than
     * during the compute phase, where the drivers would abort first) and assert that the per-row {@link #ROWS_PER_CANCELLATION_CHECK}
     * poll then aborts it. Volatile so the coordinator thread running the flattening observes the test's write.
     */
    static volatile Runnable flatteningStartedForTest = null;

    /**
     * Flattens the {@code $$unmapped_fields} column in {@code result} into a single {@code _unmapped_fields} {@link DataType#SOURCE}
     * column. Returns {@code result} unchanged if no {@link UnmappedFieldsAttribute} is present in the schema.
     *
     * @param isCancelled polled every {@link #ROWS_PER_CANCELLATION_CHECK} rows during the (potentially long) flattening; when it reports
     *                    the task cancelled the flattening throws {@link TaskCancelledException} and releases the input and any partially
     *                    built pages, so an expensive flattening can be cancelled promptly instead of only after it finishes.
     */
    public static Result flatten(Result result, BlockFactory blockFactory, PlannerSettings plannerSettings, BooleanSupplier isCancelled) {
        List<Attribute> schema = result.schema();

        int unmappedIdx = unmappedFieldsIndex(schema);
        if (unmappedIdx == -1) {
            return result;
        }
        // From #160286: the flattening below is a CPU-heavy, coordinator-side scan over every row of every page, so it must run on the
        // esql_worker pool (EsqlSession dispatches it there) rather than a transport/search thread. The guard sits after the no-op early
        // return so the "no unmapped fields" path, which completes inline on the calling thread, is unaffected. assertCurrentThreadPool is
        // a no-op on test threads, so unit tests that call flatten() directly are not impacted.
        assert ThreadPool.assertCurrentThreadPool(EsqlPlugin.ESQL_WORKER_THREAD_POOL_NAME);
        double reservationFactor = plannerSettings.sourceReservationFactor();
        UnmappedFieldsAttribute unmappedAttribute = (UnmappedFieldsAttribute) schema.get(unmappedIdx);
        UnmappedFieldsPattern pattern = unmappedAttribute.pattern();
        Set<String> existingNames = existingColumnNames(schema, unmappedIdx);
        if (existingNames.contains(UNMAPPED_FIELDS_COLUMN_NAME)) {
            return withoutUnmappedColumn(result, schema, unmappedIdx);
        }

        // Replace the synthetic column in place with the user-visible SOURCE column; everything else keeps its slot.
        List<Attribute> newSchema = new ArrayList<>(schema);
        newSchema.set(unmappedIdx, new ReferenceAttribute(Source.EMPTY, UNMAPPED_FIELDS_COLUMN_NAME, DataType.SOURCE));

        // From here on we own the input pages: on success rewritePage drains them one by one, on any failure we release whatever
        // is left below. Page#releaseBlocks is idempotent, so re-releasing pages rewritePage already drained is a no-op.
        boolean success = false;
        try {
            // Run the test seam inside the try so a throwing hook releases the input pages through the finally below, exactly like a
            // cancellation or parsing failure would.
            Runnable flatteningStarted = flatteningStartedForTest;
            if (flatteningStarted != null) {
                flatteningStarted.run();
            }
            List<Page> newPages = rewritePages(result, unmappedIdx, pattern, existingNames, blockFactory, reservationFactor, isCancelled);

            Result flattened = new Result(
                newSchema,
                newPages,
                result.attributeMetadata(),
                result.configuration(),
                result.completionInfo(),
                result.executionInfo(),
                result.approximationApplied()
            );
            success = true;
            return flattened;
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(result.pages());
            }
        }
    }

    /**
     * Drops the synthetic column when the query already outputs a column named {@link #UNMAPPED_FIELDS_COLUMN_NAME}: the query's column
     * wins, and a second column of the same name would make the response ambiguous.
     */
    private static Result withoutUnmappedColumn(Result result, List<Attribute> schema, int unmappedIdx) {
        int[] keep = new int[schema.size() - 1];
        List<Attribute> newSchema = new ArrayList<>(keep.length);
        for (int c = 0, k = 0; c < schema.size(); c++) {
            if (c != unmappedIdx) {
                keep[k++] = c;
                newSchema.add(schema.get(c));
            }
        }
        List<Page> newPages = new ArrayList<>(result.pages().size());
        boolean success = false;
        try {
            for (Page page : result.pages()) {
                newPages.add(page.projectBlocks(keep));
                page.releaseBlocks();
            }
            success = true;
            return new Result(
                newSchema,
                newPages,
                result.attributeMetadata(),
                result.configuration(),
                result.completionInfo(),
                result.executionInfo(),
                result.approximationApplied()
            );
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(newPages);
                Releasables.closeExpectNoException(result.pages());
            }
        }
    }

    /** Whether {@link #flatten} would rewrite a result with this schema, i.e. the synthetic {@code $$unmapped_fields} column is present. */
    public static boolean hasUnmappedFields(List<Attribute> schema) {
        return unmappedFieldsIndex(schema) != -1;
    }

    /** Index of the synthetic {@code $$unmapped_fields} column in {@code schema}, or {@code -1} if none is present. */
    private static int unmappedFieldsIndex(List<Attribute> schema) {
        return CollectionUtils.findIndex(schema, e -> e instanceof UnmappedFieldsAttribute);
    }

    /** Throws {@link TaskCancelledException} if {@code isCancelled} reports the query cancelled, so a long flattening aborts promptly. */
    private static void throwIfCancelled(BooleanSupplier isCancelled) {
        if (isCancelled.getAsBoolean()) {
            throw new TaskCancelledException("task cancelled during unmapped fields flattening");
        }
    }

    /**
     * Reserves memory for flattening one row. The walk holds no parsed {@code Map}, but the row's surviving leaves are buffered and then
     * re-serialised, which can cost several times the row's own bytes (boxed scalars, a list for a leaf with many values). The multiplier
     * is {@link PlannerSettings#SOURCE_RESERVATION_FACTOR}, a deliberate overestimate of that buffering that matches what the data node
     * reserved to produce the row in the first place.
     *
     * @return the number of reserved bytes, to be handed back with {@link CircuitBreaker#addWithoutBreaking} once the leaves are gone
     */
    private static long reserveForParse(BytesRef json, CircuitBreaker breaker, double reservationFactor) {
        long reservation = (long) (json.length * reservationFactor);
        breaker.addEstimateBytesAndMaybeBreak(reservation, "unmapped fields flattening");
        return reservation;
    }

    /**
     * The names of every column except {@code $$unmapped_fields}. A discovered leaf colliding with one of them is dropped rather
     * than kept, so a discovered field can never shadow a query column.
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

    /** Rewrite each page, replacing the {@code $$unmapped_fields} block with a flattened {@code _unmapped_fields} SOURCE block. */
    private static List<Page> rewritePages(
        Result result,
        int unmappedIdx,
        UnmappedFieldsPattern pattern,
        Set<String> existingNames,
        BlockFactory factory,
        double reservationFactor,
        BooleanSupplier isCancelled
    ) {
        var newPages = new ArrayList<Page>(result.pages().size());
        var success = false;
        try {
            for (Page p : result.pages()) {
                newPages.add(rewritePage(unmappedIdx, pattern, existingNames, factory, p, reservationFactor, isCancelled));
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
        UnmappedFieldsPattern pattern,
        Set<String> existingNames,
        BlockFactory blockFactory,
        Page page,
        double reservationFactor,
        BooleanSupplier isCancelled
    ) {
        int columnCount = page.getBlockCount();
        Block[] allBlocks = new Block[columnCount];

        var success = false;
        BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(page.getPositionCount());
        try {
            BytesRefBlock unmappedBlock = page.getBlock(unmappedIdx);
            // jsonScratch grows to the largest value seen in this page, so it is per-page rather than per-result. leaves is reused
            // across rows and is ordered so a leaf that appears twice (a literal dotted key overlapping a nested path) merges in a
            // deterministic source order rather than an arbitrary HashMap iteration order.
            var jsonScratch = new BytesRef();
            Map<String, Object> leaves = new LinkedHashMap<>();
            BiConsumer<String, Object> leafSink = (name, value) -> {
                // The data node ships whole objects (filtering only by top-level source key), so the pattern is applied here per
                // flattened leaf, and a leaf colliding with a real output column is dropped so it can never shadow a query column.
                if (value != null && existingNames.contains(name) == false && pattern.matches(name)) {
                    collectLeaf(leaves, name, value);
                }
            };
            CircuitBreaker breaker = blockFactory.breaker();
            for (int row = 0; row < page.getPositionCount(); row++) {
                if ((row & (ROWS_PER_CANCELLATION_CHECK - 1)) == 0) {
                    throwIfCancelled(isCancelled);
                }
                if (unmappedBlock.isNull(row)) {
                    builder.appendNull();
                    continue;
                }
                BytesRef json = getBytesRef(unmappedBlock, row, jsonScratch);
                long reservation = reserveForParse(json, breaker, reservationFactor);
                try {
                    leaves.clear();
                    walkJson(json, leafSink);
                    appendFlattened(leaves, builder);
                } finally {
                    breaker.addWithoutBreaking(-reservation);
                }
            }

            for (int c = 0; c < columnCount; c++) {
                if (c == unmappedIdx) {
                    allBlocks[c] = builder.build();
                    builder = null;
                } else {
                    Block block = page.getBlock(c);
                    block.incRef();
                    allBlocks[c] = block;
                }
            }
            var result = new Page(page.getPositionCount(), allBlocks);
            // Release the $$unmapped_fields block from the circuit breaker; the surviving blocks were protected by incRef above.
            page.releaseBlocks();
            success = true;
            return result;
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(allBlocks);
                if (builder != null) {
                    Releasables.closeExpectNoException(builder);
                }
            }
        }
    }

    /**
     * Re-serialises this row's surviving flattened leaves as a single JSON object. A scalar renders as its value, an array (a leaf seen
     * more than once, or a JSON array value) renders as a JSON array. A row with no surviving leaf appends {@code null} rather than an
     * empty object, which would carry no more information than a null.
     */
    private static void appendFlattened(Map<String, Object> leaves, BytesRefBlock.Builder builder) {
        if (leaves.isEmpty()) {
            builder.appendNull();
            return;
        }
        try (XContentBuilder json = XContentFactory.jsonBuilder()) {
            json.startObject();
            for (Map.Entry<String, Object> leaf : leaves.entrySet()) {
                json.field(leaf.getKey(), leaf.getValue() instanceof MultiValue multi ? multi.values : leaf.getValue());
            }
            json.endObject();
            builder.appendBytesRef(BytesReference.bytes(json).toBytesRef());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Streams one row's {@code $$unmapped_fields} JSON object, invoking {@code sink} once per leaf with its dotted path and value - the
     * same leaves a parsed {@code Map} would yield, but without ever materialising that map. Streaming is what bounds the heap a
     * structure-heavy row can demand: a row whose {@code _source} is, say, an array of a million one-field objects flattens one leaf at
     * a time instead of building a {@code Map}/{@code List} graph many times its own byte size.
     */
    private static void walkJson(BytesRef json, BiConsumer<String, Object> sink) {
        try (
            XContentParser parser = XContentHelper.createParserNotCompressed(
                XContentParserConfiguration.EMPTY,
                new BytesArray(json.bytes, json.offset, json.length),
                XContentType.JSON
            )
        ) {
            if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                throw new IllegalStateException("expected $$unmapped_fields to be a JSON object but was [" + parser.currentToken() + "]");
            }
            for (String field = parser.nextFieldName(); field != null; field = parser.nextFieldName()) {
                // The block loader prunes whole _source keys, so a bare top-level null is the one null that can reach here; it is
                // forwarded so the sink decides, and every deeper value goes through walkValue.
                if (parser.nextToken() == XContentParser.Token.VALUE_NULL) {
                    sink.accept(field, null);
                } else {
                    walkValue(field, parser, sink);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to parse $$unmapped_fields source", e);
        }
    }

    /**
     * Emits the leaves of one {@code _source} value at {@code name}: an object recurses to dotted leaves, an array recurses element-wise
     * (so objects inside it flatten to the same leaves a sibling index mapping those subfields would surface, and scalar elements stay at
     * {@code name}), and any scalar is a leaf as-is - read with {@link AbstractXContentParser#readValue} so it is the same
     * {@code String}/{@code Number}/{@code Boolean} a parsed map would have held.
     * <p>
     * Guard rail for what {@code UnmappedFieldsBlockLoader} promises: whatever it wrote under a key is pruned, so below the top level it
     * holds no {@code null} and no empty array or object at any depth. The asserts fire only with assertions enabled; in production the
     * walk still emits whatever it is given.
     */
    private static void walkValue(String name, XContentParser parser, BiConsumer<String, Object> sink) throws IOException {
        switch (parser.currentToken()) {
            case START_OBJECT -> {
                String field = parser.nextFieldName();
                assert field != null : prunedViolation(name);
                for (; field != null; field = parser.nextFieldName()) {
                    parser.nextToken();
                    walkValue(name + "." + field, parser, sink);
                }
            }
            case START_ARRAY -> {
                XContentParser.Token token = parser.nextToken();
                assert token != XContentParser.Token.END_ARRAY : prunedViolation(name);
                for (; token != XContentParser.Token.END_ARRAY; token = parser.nextToken()) {
                    walkValue(name, parser, sink);
                }
            }
            case VALUE_NULL -> {
                assert false : prunedViolation(name);
                sink.accept(name, null);
            }
            // Only scalar tokens reach here - objects and arrays recurse through the cases above - so readValue never builds a map; it
            // just returns the scalar exactly as the map parse did. The map factory is required by the signature but goes unused.
            case VALUE_STRING, VALUE_NUMBER, VALUE_BOOLEAN, VALUE_EMBEDDED_OBJECT -> sink.accept(
                name,
                AbstractXContentParser.readValue(parser, LinkedHashMap::new)
            );
            default -> throw new IllegalStateException("unexpected token [" + parser.currentToken() + "] while walking $$unmapped_fields");
        }
    }

    /** Message for the pruning guard rail in {@link #walkValue}: the data node must have pruned nulls and empty containers from _source. */
    private static String prunedViolation(String name) {
        return Strings.format("Unmapped field '%s' carries a null or an empty array or object", name);
    }

    /**
     * Records one leaf value. The first value under a name is stored as is; a second turns it into a {@link MultiValue}, which later
     * values are appended to in place. The walk only emits scalars, so a stored {@link MultiValue} is always one built here. Appending
     * in place matters: a leaf with a very large number of values (an array of many objects flattening to one dotted name) would
     * otherwise be copied in full on every further value, which is quadratic in the number of values.
     */
    private static void collectLeaf(Map<String, Object> leaves, String name, Object value) {
        Object existing = leaves.putIfAbsent(name, value);
        if (existing == null) {
            return;
        }
        if (existing instanceof MultiValue multi) {
            multi.values.add(value);
        } else {
            MultiValue multi = new MultiValue();
            multi.values.add(existing);
            multi.values.add(value);
            leaves.put(name, multi);
        }
    }

    /** The values of a leaf that appeared more than once in a row, in source order. */
    private static final class MultiValue {
        final List<Object> values = new ArrayList<>();
    }

    private static BytesRef getBytesRef(BytesRefBlock unmappedBlock, int row, BytesRef scratch) {
        if (unmappedBlock.getValueCount(row) != 1) {
            throw new IllegalStateException(
                Strings.format(
                    "Expected exactly one value in $$unmapped_fields block at row %d, but got %d",
                    row,
                    unmappedBlock.getValueCount(row)
                )
            );
        }
        return unmappedBlock.getBytesRef(unmappedBlock.getFirstValueIndex(row), scratch);
    }

    private ExpandUnmappedFieldsPostProcessor() {/* static class. */}
}

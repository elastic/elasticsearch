/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.approximation.ApproximationPlan;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.UnmappedFieldsAttribute;
import org.elasticsearch.xpack.esql.plan.logical.UnmappedFieldsPattern;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.session.Result;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Tests the coordinator-side flattening of {@code SET unmapped_fields="LOAD_ALL"} into a single {@code _unmapped_fields}
 * {@link DataType#SOURCE} column (see <a href="https://github.com/elastic/elasticsearch/issues/161717">#161717</a>). The synthetic
 * {@code $$unmapped_fields} column is replaced in place by a {@code _unmapped_fields} column holding one flattened JSON object per row,
 * dotted leaf names and all; a row that contributes no surviving leaf gets a {@code null}. JSON scalar types are preserved (a number
 * stays a number), unlike the stringified-to-keyword expansion this replaced.
 */
public class ExpandUnmappedFieldsPostProcessorTests extends ComputeTestCase {
    public void testFlattensAcrossPagesIntoSourceColumn() {
        BlockFactory bf = blockFactory();
        Result result = result(
            List.of(intAttr(), unmappedAttr()),
            List.of(
                page(bf, List.of(row(1, jsonObject("{'pet':'Rex','city':'Berlin'}")))),
                page(bf, List.of(row(2, jsonObject("{'pet':'Max','zip':'10115'}"))))
            )
        );

        Result flattened = flatten(result, bf);
        try {
            assertThat(flattened, not(sameInstance(result)));
            assertThat(names(flattened), equalTo(List.of(INT_ATTR, UNMAPPED)));
            assertThat(dataTypes(flattened), equalTo(List.of(DataType.INTEGER, DataType.SOURCE)));
            assertThat(
                rows(flattened),
                contains(
                    matchesMap().entry(INT_ATTR, 1).entry(UNMAPPED, matchesMap().entry("pet", "Rex").entry("city", "Berlin")),
                    matchesMap().entry(INT_ATTR, 2).entry(UNMAPPED, matchesMap().entry("pet", "Max").entry("zip", "10115"))
                )
            );
            assertThat(flattened.configuration(), sameInstance(result.configuration()));
            assertThat(flattened.completionInfo(), sameInstance(result.completionInfo()));
        } finally {
            Releasables.close(flattened.pages());
        }
    }

    public void testReturnsSameResultWhenNoUnmappedFieldsAttribute() {
        BlockFactory bf = blockFactory();
        Result result = singlePage(bf, List.of(intAttr()), row(1));

        Result flattened = flatten(result, bf);
        try {
            assertThat(flattened, sameInstance(result));
        } finally {
            Releasables.close(flattened.pages());
        }
    }

    public void testNullAndEmptyUnmappedValuesBecomeNullSourceCells() {
        BlockFactory bf = blockFactory();
        Result result = singlePage(
            bf,
            List.of(intAttr(), unmappedAttr()),
            row(1, null),
            row(2, jsonObject("{}")),
            row(3, jsonObject("{'a':'x'}"))
        );

        Result flattened = flatten(result, bf);
        try {
            // The SOURCE column is always present under LOAD_ALL; a row with no surviving leaf is simply null in it.
            assertThat(names(flattened), equalTo(List.of(INT_ATTR, UNMAPPED)));
            assertThat(
                rows(flattened),
                contains(
                    matchesMap().entry(INT_ATTR, 1),
                    matchesMap().entry(INT_ATTR, 2),
                    matchesMap().entry(INT_ATTR, 3).entry(UNMAPPED, matchesMap().entry("a", "x"))
                )
            );
        } finally {
            Releasables.close(flattened.pages());
        }
    }

    public void testAllNullUnmappedKeepsSourceColumnAllNull() {
        BlockFactory bf = blockFactory();
        Result result = singlePage(bf, List.of(intAttr(), unmappedAttr()), row(1, null), row(2, null));

        Result flattened = flatten(result, bf);
        try {
            assertThat(names(flattened), equalTo(List.of(INT_ATTR, UNMAPPED)));
            assertThat(rows(flattened), contains(matchesMap().entry(INT_ATTR, 1), matchesMap().entry(INT_ATTR, 2)));
        } finally {
            Releasables.close(flattened.pages());
        }
    }

    public void testSourceColumnStaysInPlaceAmongApproximationColumns() {
        BlockFactory bf = blockFactory();
        String ci = ApproximationPlan.CONFIDENCE_INTERVAL_COLUMN_PREFIX + "extra)";
        String certified = ApproximationPlan.CERTIFIED_COLUMN_PREFIX + "extra)";
        Result result = singlePage(
            bf,
            List.of(intAttr(), unmappedAttr(), keywordAttr("extra"), keywordAttr(ci), keywordAttr(certified)),
            row(1, jsonObject("{'pet':'Rex','city':'Berlin'}"), "after", "ci", "yes")
        );

        Result flattened = flatten(result, bf);
        try {
            // The column takes the synthetic column's slot; the surrounding columns (including approximation ones) keep their place.
            assertThat(names(flattened), equalTo(List.of(INT_ATTR, UNMAPPED, "extra", ci, certified)));
            assertThat(
                rows(flattened),
                contains(
                    matchesMap().entry(INT_ATTR, 1)
                        .entry(UNMAPPED, matchesMap().entry("pet", "Rex").entry("city", "Berlin"))
                        .entry("extra", "after")
                        .entry(ci, "ci")
                        .entry(certified, "yes")
                )
            );
        } finally {
            Releasables.close(flattened.pages());
        }
    }

    public void testNestedObjectFlattensToDottedLeafAndScalarTypesArePreserved() {
        BlockFactory bf = blockFactory();
        Result result = singlePage(
            bf,
            List.of(intAttr(), unmappedAttr()),
            row(1, jsonObject("{'count':5,'active':true,'nested':{'x':1}}"))
        );

        Result flattened = flatten(result, bf);
        try {
            // A nested object contributes no key of its own; its leaf becomes a dotted key (nested.x). Non-string scalars keep their
            // JSON type inside the SOURCE blob (number stays a number, boolean stays a boolean).
            assertThat(
                rows(flattened),
                contains(
                    matchesMap().entry(INT_ATTR, 1)
                        .entry(UNMAPPED, matchesMap().entry("count", 5).entry("active", true).entry("nested.x", 1))
                )
            );
        } finally {
            Releasables.close(flattened.pages());
        }
    }

    public void testFlattenedLeafCollidingWithQueryColumnIsDropped() {
        BlockFactory bf = blockFactory();
        Result result = singlePage(
            bf,
            List.of(keywordAttr("network.eth0.rx"), unmappedAttr()),
            row("7", jsonObject("{'network':{'bytes_in':10,'eth0':{'tx':5,'rx':7}}}"))
        );

        Result flattened = flatten(result, bf);
        try {
            // network.eth0.rx collides with the query column, so it is dropped from the blob; the other leaves survive.
            assertThat(
                rows(flattened),
                contains(
                    matchesMap().entry("network.eth0.rx", "7")
                        .entry(UNMAPPED, matchesMap().entry("network.bytes_in", 10).entry("network.eth0.tx", 5))
                )
            );
        } finally {
            Releasables.close(flattened.pages());
        }
    }

    public void testArrayOfObjectsFlattensElementWiseToDottedLeafMultivalue() {
        BlockFactory bf = blockFactory();
        Result result = singlePage(
            bf,
            List.of(intAttr(), unmappedAttr()),
            row(1, jsonObject("{'tags':['a','b'],'samples':[{'nested':'x'},{'nested':'y'}]}"))
        );

        Result flattened = flatten(result, bf);
        try {
            assertThat(
                rows(flattened),
                contains(
                    matchesMap().entry(INT_ATTR, 1)
                        .entry(UNMAPPED, matchesMap().entry("tags", List.of("a", "b")).entry("samples.nested", List.of("x", "y")))
                )
            );
        } finally {
            Releasables.close(flattened.pages());
        }
    }

    public void testDuplicateLeafFromLiteralAndNestedKeyMergesToMultivalue() {
        BlockFactory bf = blockFactory();
        Result result = singlePage(bf, List.of(intAttr(), unmappedAttr()), row(1, jsonObject("{'a.b':'literal','a':{'b':'nested'}}")));

        Result flattened = flatten(result, bf);
        try {
            assertThat(
                rows(flattened),
                contains(matchesMap().entry(INT_ATTR, 1).entry(UNMAPPED, matchesMap().entry("a.b", List.of("literal", "nested"))))
            );
        } finally {
            Releasables.close(flattened.pages());
        }
    }

    public void testSingleElementArrayRendersAsScalar() {
        BlockFactory bf = blockFactory();
        Result result = singlePage(bf, List.of(intAttr(), unmappedAttr()), row(1, jsonObject("{'one':['a'],'two':['a','b']}")));

        Result flattened = flatten(result, bf);
        try {
            // A leaf with one value is indistinguishable from a scalar, as in any multivalue field; only several values make an array.
            assertThat(
                rows(flattened),
                contains(matchesMap().entry(INT_ATTR, 1).entry(UNMAPPED, matchesMap().entry("one", "a").entry("two", List.of("a", "b"))))
            );
        } finally {
            Releasables.close(flattened.pages());
        }
    }

    public void testQueryColumnNamedLikeTheUnmappedColumnWinsAndNoSecondColumnIsProduced() {
        BlockFactory bf = blockFactory();
        Result result = singlePage(
            bf,
            List.of(keywordAttr(UNMAPPED), intAttr(), unmappedAttr()),
            row("mine", 1, jsonObject("{'a':'x'}")),
            row("yours", 2, null)
        );

        Result flattened = flatten(result, bf);
        try {
            assertThat(names(flattened), equalTo(List.of(UNMAPPED, INT_ATTR)));
            assertThat(dataTypes(flattened), equalTo(List.of(DataType.KEYWORD, DataType.INTEGER)));
            assertThat(
                rows(flattened),
                contains(matchesMap().entry(UNMAPPED, "mine").entry(INT_ATTR, 1), matchesMap().entry(UNMAPPED, "yours").entry(INT_ATTR, 2))
            );
        } finally {
            Releasables.close(flattened.pages());
        }
    }

    public void testLargeLongsAndDoublesSurviveTheRoundTrip() {
        BlockFactory bf = blockFactory();
        Result result = singlePage(
            bf,
            List.of(intAttr(), unmappedAttr()),
            row(1, jsonObject("{'big':9223372036854775807,'neg':-9223372036854775808,'half':1.5,'tenth':0.1}"))
        );

        Result flattened = flatten(result, bf);
        try {
            assertThat(
                rows(flattened),
                contains(
                    matchesMap().entry(INT_ATTR, 1)
                        .entry(
                            UNMAPPED,
                            matchesMap().entry("big", Long.MAX_VALUE).entry("neg", Long.MIN_VALUE).entry("half", 1.5).entry("tenth", 0.1)
                        )
                )
            );
        } finally {
            Releasables.close(flattened.pages());
        }
    }

    public void testEscapesAndNonAsciiRoundTripAsValidJson() {
        BlockFactory bf = blockFactory();
        String json =
            "{\"qu\\\"ote\":\"back\\\\slash\",\"ctl\":\"line\\nbreak\\ttab\",\"caf\u00e9\":\"na\u00efve\",\"emoji\":\"\uD83D\uDE00\"}";
        Result result = singlePage(bf, List.of(intAttr(), unmappedAttr()), row(1, json));

        Result flattened = flatten(result, bf);
        try {
            // Keys and values with quotes, backslashes, control characters, accents and a surrogate pair must come back out of the
            // re-serialised blob as the same strings, i.e. the blob is still valid JSON.
            assertThat(
                rows(flattened),
                contains(
                    matchesMap().entry(INT_ATTR, 1)
                        .entry(
                            UNMAPPED,
                            matchesMap().entry("qu\"ote", "back\\slash")
                                .entry("ctl", "line\nbreak\ttab")
                                .entry("caf\u00e9", "na\u00efve")
                                .entry("emoji", "\uD83D\uDE00")
                        )
                )
            );
        } finally {
            Releasables.close(flattened.pages());
        }
    }

    public void testDottedKeyHoldingAnObjectFlattensToTheFullDottedPath() {
        BlockFactory bf = blockFactory();
        Result result = singlePage(bf, List.of(intAttr(), unmappedAttr()), row(1, jsonObject("{'a.b':{'c':1,'d':{'e':2}}}")));

        Result flattened = flatten(result, bf);
        try {
            assertThat(
                rows(flattened),
                contains(matchesMap().entry(INT_ATTR, 1).entry(UNMAPPED, matchesMap().entry("a.b.c", 1).entry("a.b.d.e", 2)))
            );
        } finally {
            Releasables.close(flattened.pages());
        }
    }

    public void testVeryWideRowKeepsEveryLeafAndReleasesTheBreaker() {
        BlockFactory bf = blockFactory();
        int leaves = 20_000;
        StringBuilder json = new StringBuilder("{");
        for (int i = 0; i < leaves; i++) {
            json.append(i == 0 ? "" : ",").append("\"f").append(i).append("\":").append(i);
        }
        Result result = singlePage(bf, List.of(intAttr(), unmappedAttr()), row(1, json.append('}').toString()));

        Result flattened = flatten(result, bf);
        try {
            Map<?, ?> blob = (Map<?, ?>) rows(flattened).get(0).get(UNMAPPED);
            assertThat(blob.size(), equalTo(leaves));
            assertThat(blob.get("f0"), equalTo(0));
            assertThat(blob.get("f19999"), equalTo(19_999));
        } finally {
            Releasables.close(flattened.pages());
        }
        assertThat("the parse reservation was not handed back", bf.breaker().getUsed(), equalTo(0L));
    }

    public void testDeepNestingFlattensToOneLongDottedPath() {
        BlockFactory bf = blockFactory();
        int depth = 500;
        Result result = singlePage(bf, List.of(intAttr(), unmappedAttr()), row(1, nested(depth)));

        Result flattened = flatten(result, bf);
        try {
            Map<?, ?> blob = (Map<?, ?>) rows(flattened).get(0).get(UNMAPPED);
            assertThat(blob.size(), equalTo(1));
            assertThat(blob.keySet().iterator().next(), equalTo("k" + ".k".repeat(depth - 1)));
        } finally {
            Releasables.close(flattened.pages());
        }
    }

    public void testNestingBeyondTheParserLimitFailsCleanlyWithoutLeaking() {
        BlockFactory bf = blockFactory();
        // _source nested deeper than the JSON parser allows cannot be indexed either; this guards the coordinator against it anyway.
        Result result = singlePage(bf, List.of(intAttr(), unmappedAttr()), row(1, nested(5_000)));

        expectThrows(Exception.class, () -> flatten(result, bf));
        assertThat("a failed flattening leaked pages or reservation", bf.breaker().getUsed(), equalTo(0L));
    }

    public void testArrayOfManyTinyObjectsFlattensWithinABreakerSizedToTheReservation() {
        String json = arrayOfTinyObjects(200_000);
        // The breaker has room for the input page, the reservation (sourceReservationFactor times the JSON) and an output the size of
        // the JSON, and no more. That must be enough: nothing unaccounted-for is needed to flatten a structure-heavy row.
        long limit = inputPageBytes(json) + reservation(json) + json.length();
        BlockFactory bf = blockFactory(ByteSizeValue.ofBytes(limit));
        Result result = singlePage(bf, List.of(intAttr(), unmappedAttr()), row(1, json));

        Result flattened = flatten(result, bf);
        try {
            Map<?, ?> blob = (Map<?, ?>) rows(flattened).get(0).get(UNMAPPED);
            assertThat(blob.keySet(), contains("a.x"));
            assertThat(((List<?>) blob.get("a.x")).size(), equalTo(200_000));
        } finally {
            Releasables.close(flattened.pages());
        }
        assertThat("the reservation was not handed back", bf.breaker().getUsed(), equalTo(0L));
    }

    public void testArrayOfManyTinyObjectsBreaksCleanlyWhenTheReservationDoesNotFit() {
        String json = arrayOfTinyObjects(200_000);
        // Room for the input page but only half the reservation on top of it, as on a node whose breaker is nearly full.
        long limit = inputPageBytes(json) + reservation(json) / 2;
        BlockFactory bf = blockFactory(ByteSizeValue.ofBytes(limit));
        Result result = singlePage(bf, List.of(intAttr(), unmappedAttr()), row(1, json));

        expectThrows(CircuitBreakingException.class, () -> flatten(result, bf));
        assertThat("a refused reservation leaked pages or bytes", bf.breaker().getUsed(), equalTo(0L));
    }

    /** What the breaker is charged for holding {@code json} as a one-row input page, measured rather than assumed. */
    private long inputPageBytes(String json) {
        BlockFactory probe = blockFactory();
        Result result = singlePage(probe, List.of(intAttr(), unmappedAttr()), row(1, json));
        try {
            return probe.breaker().getUsed();
        } finally {
            Releasables.close(result.pages());
        }
    }

    private static long reservation(String json) {
        return (long) (new BytesRef(json).length * PlannerSettings.DEFAULTS.sourceReservationFactor());
    }

    public void testChildWildcardIncludeKeepsObjectLeavesAndDropsSiblings() {
        BlockFactory bf = blockFactory();
        Result result = singlePage(
            bf,
            List.of(intAttr(), unmappedAttr(UnmappedFieldsPattern.includes(List.of("unmapped.*")))),
            row(1, jsonObject("{'unmapped':{'deep':{'leaf':'v'},'foo':'f'},'other':'o'}"))
        );

        Result flattened = flatten(result, bf);
        try {
            assertThat(
                rows(flattened),
                contains(
                    matchesMap().entry(INT_ATTR, 1)
                        .entry(UNMAPPED, matchesMap().entry("unmapped.deep.leaf", "v").entry("unmapped.foo", "f"))
                )
            );
        } finally {
            Releasables.close(flattened.pages());
        }
    }

    public void testExactObjectFieldNameProducesNoLeaf() {
        BlockFactory bf = blockFactory();
        Result result = singlePage(
            bf,
            List.of(intAttr(), unmappedAttr(UnmappedFieldsPattern.includes(List.of("samples")))),
            row(1, jsonObject("{'samples':{'nested':'x'}}"))
        );

        Result flattened = flatten(result, bf);
        try {
            // "samples" matches the object, but only its dotted leaf "samples.nested" could be a leaf, and that does not match.
            assertThat(rows(flattened), contains(matchesMap().entry(INT_ATTR, 1)));
        } finally {
            Releasables.close(flattened.pages());
        }
    }

    /**
     * The coordinator trusts the data node to only send pruned values - {@code UnmappedFieldsBlockLoaderTests} pins that end of the
     * contract. These guard rails hold the other end: a value-less part that arrived anyway (a null inside an array or object, an empty
     * container) must not pass silently, since it would otherwise render as a literal {@code "null"} or {@code "{}"}.
     */
    public void testNullInsideArrayTripsGuardRail() {
        BlockFactory bf = blockFactory();
        Result result = result(List.of(intAttr(), unmappedAttr()), List.of(page(bf, List.of(row(1, jsonObject("{'a':[null,'x']}"))))));

        AssertionError e = expectThrows(AssertionError.class, () -> flatten(result, bf));
        assertThat(e.getMessage(), containsString("Unmapped field 'a' carries a null or an empty array or object"));
        assertThat("the guard rail leaked pages", bf.breaker().getUsed(), equalTo(0L));
    }

    public void testNullInsideObjectTripsGuardRail() {
        BlockFactory bf = blockFactory();
        Result result = result(
            List.of(intAttr(), unmappedAttr()),
            List.of(page(bf, List.of(row(1, jsonObject("{'a':{'keep':'me','drop':[]}}")))))
        );

        AssertionError e = expectThrows(AssertionError.class, () -> flatten(result, bf));
        assertThat(e.getMessage(), containsString("Unmapped field 'a.drop' carries a null or an empty array or object"));
        assertThat("the guard rail leaked pages", bf.breaker().getUsed(), equalTo(0L));
    }

    public void testEmptyObjectTripsGuardRail() {
        BlockFactory bf = blockFactory();
        Result result = result(List.of(intAttr(), unmappedAttr()), List.of(page(bf, List.of(row(1, jsonObject("{'a':{}}"))))));

        AssertionError e = expectThrows(AssertionError.class, () -> flatten(result, bf));
        assertThat(e.getMessage(), containsString("Unmapped field 'a' carries a null or an empty array or object"));
        assertThat("the guard rail leaked pages", bf.breaker().getUsed(), equalTo(0L));
    }

    public void testFlattenUnderCrankyBreakerDoesNotLeak() {
        testWithCrankyBlockFactory(bf -> {
            StringBuilder json = new StringBuilder("{");
            for (int i = 0; i < 50; i++) {
                if (i > 0) {
                    json.append(',');
                }
                json.append("\"field_").append(i).append("\":\"v").append(i).append('"');
            }
            json.append('}');

            List<List<Object>> rows = new ArrayList<>();
            for (int row = 0; row < 20; row++) {
                rows.add(row(row, json.toString()));
            }
            Result result = result(List.of(intAttr(), unmappedAttr()), List.of(page(bf, rows)));
            // testWithCrankyBlockFactory catches the CircuitBreakingException and asserts its message; ComputeTestCase then
            // asserts every breaker is back to zero, which is what this test is about.
            Releasables.close(flatten(result, bf).pages());
        });
    }

    public void testFlattenReleasesInputPagesWhenItFails() {
        BlockFactory bf = blockFactory();
        Block intBlock;
        try (var builder = bf.newIntBlockBuilder(1)) {
            builder.appendInt(1);
            intBlock = builder.build();
        }
        BytesRefBlock unmappedBlock;
        try (BytesRefBlock.Builder builder = bf.newBytesRefBlockBuilder(1)) {
            builder.beginPositionEntry();
            builder.appendBytesRef(new BytesRef(jsonObject("{'a':'x'}")));
            builder.appendBytesRef(new BytesRef(jsonObject("{'b':'y'}")));
            builder.endPositionEntry();
            unmappedBlock = builder.build();
        }
        Result result = result(List.of(intAttr(), unmappedAttr()), List.of(new Page(intBlock, unmappedBlock)));
        assertThat("input pages should reserve breaker memory before flatten runs", bf.breaker().getUsed(), greaterThan(0L));

        var e = expectThrows(IllegalStateException.class, () -> flatten(result, bf));
        assertThat(e.getMessage(), containsString("Expected exactly one value"));

        // No manual release here: the point is that flatten must have released the input pages on its failure path.
        assertThat("flatten leaked the input pages on failure", bf.breaker().getUsed(), equalTo(0L));
    }

    /**
     * Pins {@code EsqlSession}'s LOAD_ALL dispatch contract: the flattening is handed to the {@code esql_worker} pool wrapped in
     * {@link ActionRunnable#wrapReleasing}, so the buffered result pages are released if that pool rejects the task (e.g. the node is
     * shutting down or the worker queue is saturated). Here a one-thread, zero-queue worker is saturated so the dispatch is rejected;
     * the test asserts the pages are freed (breaker back to zero) and the listener observes the rejection rather than a leak.
     */
    public void testDispatchReleasesPagesWhenWorkerRejects() throws Exception {
        BlockFactory bf = blockFactory();
        List<Page> pages = List.of(
            page(bf, List.of(row(1, jsonObject("{'pet':'Rex'}")))),
            page(bf, List.of(row(2, jsonObject("{'city':'Berlin'}"))))
        );
        assertThat("pages should reserve breaker memory before dispatch", bf.breaker().getUsed(), greaterThan(0L));

        var executor = EsExecutors.newFixed(
            EsqlPlugin.ESQL_WORKER_THREAD_POOL_NAME,
            1,
            0,
            Thread::new,
            new ThreadContext(Settings.EMPTY),
            EsExecutors.TaskTrackingConfig.DO_NOT_TRACK
        );
        try {
            // Occupy the single worker thread so the next submission has nowhere to queue and is rejected.
            var barrier = new CyclicBarrier(2);
            executor.execute(() -> safeAwait(barrier));

            var rejection = new PlainActionFuture<Void>();
            executor.execute(ActionRunnable.wrapReleasing(new ActionListener<Void>() {
                @Override
                public void onResponse(Void unused) {
                    fail("flattening must not run once the worker has rejected the task");
                }

                @Override
                public void onFailure(Exception e) {
                    assertThat(e, instanceOf(EsRejectedExecutionException.class));
                    rejection.onResponse(null);
                }
            },
                () -> Releasables.closeExpectNoException(pages),
                ll -> fail("flattening body must not run once the worker has rejected the task")
            ));

            safeGet(rejection);
            assertThat("rejected dispatch must release the result pages", bf.breaker().getUsed(), equalTo(0L));
            safeAwait(barrier);
        } finally {
            ThreadPool.terminate(executor, 10, TimeUnit.SECONDS);
        }
    }

    public void testCancellationThrowsAndReleasesPages() {
        BlockFactory bf = blockFactory();
        Result result = result(
            List.of(intAttr(), unmappedAttr()),
            List.of(page(bf, List.of(row(1, jsonObject("{'pet':'Rex'}")))), page(bf, List.of(row(2, jsonObject("{'pet':'Max'}")))))
        );
        assertThat("input pages should reserve breaker memory before flatten runs", bf.breaker().getUsed(), greaterThan(0L));

        // Stands in for a task cancelled mid-flattening: the checker reports cancelled as soon as the flattening polls it.
        expectThrows(
            TaskCancelledException.class,
            () -> ExpandUnmappedFieldsPostProcessor.flatten(result, bf, PlannerSettings.DEFAULTS, () -> true)
        );

        assertThat("flatten leaked pages when cancelled", bf.breaker().getUsed(), equalTo(0L));
    }

    /**
     * The flattening is a single scan over every row of every page, polling {@code isCancelled} at rows {@code 0, 1024, 2048, ...},
     * i.e. {@code ceil(rows / 1024)} times. This scans a page wide enough to cross the 1024-row threshold several times and pins the
     * poll count, catching a regression that polled every row (far too often) or only once (defeating a mid-scan checkpoint).
     */
    public void testCancellationPollCadenceMatchesRowThreshold() {
        BlockFactory bf = blockFactory();
        int rows = 3000;
        List<List<Object>> pageRows = new ArrayList<>(rows);
        for (int i = 0; i < rows; i++) {
            pageRows.add(row(i, jsonObject("{'pet':'Rex'}")));
        }
        Result result = result(List.of(intAttr(), unmappedAttr()), List.of(page(bf, pageRows)));

        AtomicInteger polls = new AtomicInteger();
        Result flattened = ExpandUnmappedFieldsPostProcessor.flatten(result, bf, PlannerSettings.DEFAULTS, () -> {
            polls.incrementAndGet();
            return false;
        });
        try {
            // 1024 mirrors the production ROWS_PER_CANCELLATION_CHECK, which is private to the post-processor.
            int perScan = (rows + 1023) / 1024;
            assertThat(polls.get(), equalTo(perScan));
        } finally {
            Releasables.close(flattened.pages());
        }
    }

    private static Result flatten(Result result, BlockFactory blockFactory) {
        return flatten(result, blockFactory, PlannerSettings.DEFAULTS);
    }

    private static Result flatten(Result result, BlockFactory blockFactory, PlannerSettings plannerSettings) {
        return ExpandUnmappedFieldsPostProcessor.flatten(result, blockFactory, plannerSettings, () -> false);
    }

    private static Result result(List<Attribute> schema, List<Page> pages) {
        return new Result(schema, pages, Map.of(), EsqlTestUtils.TEST_CFG, DriverCompletionInfo.EMPTY, null, null);
    }

    /** A {@link Result} of one {@link #page}: what most of these tests need, without nesting {@link #row}s two lists deep. */
    @SafeVarargs
    @SuppressWarnings("varargs") // rows is only read, never stored or published, so passing it on cannot pollute the heap
    private static Result singlePage(BlockFactory bf, List<Attribute> schema, List<Object>... rows) {
        return result(schema, List.of(page(bf, List.of(rows))));
    }

    private static Attribute intAttr() {
        return new ReferenceAttribute(Source.EMPTY, null, INT_ATTR, DataType.INTEGER);
    }

    private static Attribute keywordAttr(String name) {
        return new ReferenceAttribute(Source.EMPTY, null, name, DataType.KEYWORD);
    }

    private static UnmappedFieldsAttribute unmappedAttr() {
        return new UnmappedFieldsAttribute(Source.EMPTY, UnmappedFieldsPattern.ALL);
    }

    private static UnmappedFieldsAttribute unmappedAttr(UnmappedFieldsPattern pattern) {
        return new UnmappedFieldsAttribute(Source.EMPTY, pattern);
    }

    /** Builds a single page whose blocks are inferred from {@code rows} (one {@link #row} per position). */
    private static Page page(BlockFactory bf, List<List<Object>> rows) {
        return new Page(BlockUtils.fromList(bf, rows));
    }

    /** A single row of column values; accepts {@code null} cells (unlike {@link List#of}). */
    private static List<Object> row(Object... values) {
        return Arrays.asList(values);
    }

    /** Turns single quotes into double quotes so JSON literals read without escaping. */
    /** {@code {"a":[{"x":1},{"x":1},...]}} with {@code elements} elements: a few bytes each, but many leaves for one dotted name. */
    private static String arrayOfTinyObjects(int elements) {
        StringBuilder json = new StringBuilder(elements * 8 + 16).append("{\"a\":[");
        for (int i = 0; i < elements; i++) {
            json.append(i == 0 ? "" : ",").append("{\"x\":1}");
        }
        return json.append("]}").toString();
    }

    /** {@code {"k":{"k":...1...}}}, {@code depth} objects deep. */
    private static String nested(int depth) {
        return "{\"k\":".repeat(depth) + "1" + "}".repeat(depth);
    }

    private static String jsonObject(String singleQuoted) {
        return singleQuoted.replace('\'', '"');
    }

    private static List<String> names(Result r) {
        return r.schema().stream().map(Attribute::name).toList();
    }

    private static List<DataType> dataTypes(Result r) {
        return r.schema().stream().map(Attribute::dataType).toList();
    }

    /**
     * One map per row holding only the non-null cells. The {@code _unmapped_fields} SOURCE cell is parsed back from its JSON bytes into
     * a nested map so tests can assert its flattened leaves (a null cell is omitted, like the row never having that column).
     */
    private static List<Map<String, Object>> rows(Result r) {
        List<Attribute> schema = r.schema();
        List<Map<String, Object>> rows = new ArrayList<>();
        for (Page page : r.pages()) {
            for (int row = 0; row < page.getPositionCount(); row++) {
                Map<String, Object> cells = new LinkedHashMap<>();
                for (int col = 0; col < schema.size(); col++) {
                    Attribute attribute = schema.get(col);
                    Object value = attribute.dataType() == DataType.SOURCE
                        ? parseSource(page.getBlock(col), row)
                        : valueAt(page.getBlock(col), row);
                    if (value != null) {
                        cells.put(attribute.name(), value);
                    }
                }
                rows.add(cells);
            }
        }
        return rows;
    }

    private static @Nullable Object valueAt(Block block, int row) {
        Object value = BlockUtils.toJavaObject(block, row);
        return value instanceof BytesRef bytesRef ? bytesRef.utf8ToString() : value;
    }

    /** Parses the JSON object held in a SOURCE cell back into a map, or {@code null} if the cell is null. */
    private static @Nullable Map<String, Object> parseSource(Block block, int row) {
        if (block.isNull(row)) {
            return null;
        }
        BytesRef json = ((BytesRefBlock) block).getBytesRef(((BytesRefBlock) block).getFirstValueIndex(row), new BytesRef());
        return XContentHelper.convertToMap(new BytesArray(json.bytes, json.offset, json.length), true, XContentType.JSON).v2();
    }

    private static final String INT_ATTR = "emp_no";
    private static final String UNMAPPED = ExpandUnmappedFieldsPostProcessor.UNMAPPED_FIELDS_COLUMN_NAME;
}

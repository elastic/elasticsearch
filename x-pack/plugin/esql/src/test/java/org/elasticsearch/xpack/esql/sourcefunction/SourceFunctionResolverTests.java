/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.sourcefunction;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.cluster.metadata.ViewMetadata;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LeafPlan;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedSourceFunctionRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.ImmediateLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

/**
 * The facility driven by a function that exists only here.
 * <p>
 * That is the point of these tests rather than an accident of them: a function written in a test exercises every
 * part of the shared path, so nothing in that path can have been shaped by a particular shipped function's needs.
 * A test that reached for a real function would prove the opposite.
 */
public class SourceFunctionResolverTests extends ESTestCase {
    /**
     * A function whose rows are a literal. Four methods, and none of them about a wire format, a data node or a
     * privilege — a function of this shape has none of those, and the facility gives it no seam to acquire one.
     */
    private static class Numbers implements SourceFunction {

        private final AtomicInteger bindings = new AtomicInteger();

        @Override
        public String name() {
            return "numbers";
        }

        @Override
        public SourceFunctionSignature signature() {
            return SourceFunctionSignature.of(SourceFunctionSignature.string("upto"));
        }

        @Override
        public void validate(SourceFunctionInvocation call) {
            if (call.string(0).matches("\\d+") == false) {
                throw new ParsingException(call.arguments().get(0).source(), "[numbers] takes a number, found [{}]", call.string(0));
            }
        }

        @Override
        public void toRelation(
            SourceFunctionInvocation call,
            List<NamedExpression> metadataFields,
            ProjectMetadata projectMetadata,
            ActionListener<LeafPlan> listener
        ) {
            bindings.incrementAndGet();
            listener.onResponse(counting(call.source(), Long.parseLong(call.string(0))));
        }
    }

    public void testPlanWithNoSourceFunctionIsHandedBackUntouched() {
        LogicalPlan plan = new UnresolvedRelation(
            Source.EMPTY,
            new IndexPattern(Source.EMPTY, "logs"),
            false,
            List.of(),
            IndexMode.STANDARD,
            null
        );
        assertSame(plan, resolve(new Numbers(), plan));
    }

    public void testTheLeafBecomesTheRelationTheFunctionReturned() {
        LogicalPlan resolved = resolve(new Numbers(), call("3"));

        assertThat(resolved, instanceOf(LocalRelation.class));
        LocalRelation rows = (LocalRelation) resolved;
        assertThat(rows.output().size(), equalTo(1));
        assertThat(rows.output().get(0).name(), equalTo("n"));
        assertThat(rows.supplier().get().getPositionCount(), equalTo(3));
    }

    /** A function is one leaf of a source list and never assumes it is alone in it. */
    public void testFunctionBesideAnIndexPattern() {
        LogicalPlan plan = new UnionAll(
            Source.EMPTY,
            List.of(
                new UnresolvedRelation(Source.EMPTY, new IndexPattern(Source.EMPTY, "logs"), false, List.of(), IndexMode.STANDARD, null),
                call("2")
            ),
            List.of()
        );

        LogicalPlan resolved = resolve(new Numbers(), plan);

        List<LogicalPlan> children = resolved.children();
        assertThat(children.get(0), instanceOf(UnresolvedRelation.class));
        assertThat(children.get(1), instanceOf(LocalRelation.class));
    }

    /** Every call in one list binds, and each gets its own leaf rather than one being reused for both. */
    public void testSeveralCallsEachBind() {
        Numbers function = new Numbers();
        LogicalPlan plan = new UnionAll(Source.EMPTY, List.of(call("1"), call("2")), List.of());

        LogicalPlan resolved = resolve(function, plan);

        assertThat(function.bindings.get(), equalTo(2));
        assertThat(((LocalRelation) resolved.children().get(0)).supplier().get().getPositionCount(), equalTo(1));
        assertThat(((LocalRelation) resolved.children().get(1)).supplier().get().getPositionCount(), equalTo(2));
    }

    /** A leaf nested under other commands is still found and still replaced. */
    public void testLeafBelowAnotherCommandIsReplaced() {
        LogicalPlan plan = new Filter(Source.EMPTY, call("1"), Literal.TRUE);
        assertThat(resolve(new Numbers(), plan).children().get(0), instanceOf(LocalRelation.class));
    }

    public void testUnknownNameIsAParseErrorAtTheCall() {
        Source source = new Source(1, 5, "nope(1)");
        LogicalPlan plan = new UnresolvedSourceFunctionRelation(
            source,
            new SourceFunctionInvocation(source, "nope", List.of(new SourceFunctionArgument.Value(source, DataType.KEYWORD, "1"))),
            List.of()
        );

        ParsingException e = expectThrows(ParsingException.class, () -> resolve(new Numbers(), plan));
        assertThat(e.getMessage(), containsString("unknown source function [nope], expected one of [numbers]"));
    }

    /** The shape is checked by the shared signature, so a function writes no arity code of its own. */
    public void testWrongArityIsRefusedBeforeTheFunctionIsAsked() {
        Numbers function = new Numbers();

        expectThrows(ParsingException.class, () -> resolve(function, callWithNoArguments()));
        assertThat("the function must not have been asked to bind", function.bindings.get(), equalTo(0));
    }

    /** A function's own validation runs, and runs before it binds. */
    public void testValidationRefusesBeforeBinding() {
        Numbers function = new Numbers();

        ParsingException e = expectThrows(ParsingException.class, () -> resolve(function, call("many")));
        assertThat(e.getMessage(), containsString("[numbers] takes a number, found [many]"));
        assertThat(function.bindings.get(), equalTo(0));
    }

    /**
     * A map argument reaches the function as the last element of one flat list, as a scalar function's does - and
     * this goes through the resolver so that it is the function, not the record, that is shown to receive it.
     */
    public void testMapArrivesAsAnArgument() {
        MapReading function = new MapReading();
        Source source = Source.EMPTY;
        UnresolvedSourceFunctionRelation leaf = new UnresolvedSourceFunctionRelation(
            source,
            new SourceFunctionInvocation(
                source,
                "mapreading",
                List.of(
                    new SourceFunctionArgument.Value(source, DataType.KEYWORD, "1"),
                    new SourceFunctionArgument.NamedValues(source, Map.of("k", "v"))
                )
            ),
            List.of()
        );

        resolve(function, leaf);

        assertThat(function.seen, equalTo(Map.of("k", "v")));
    }

    /**
     * Two functions that answer only once released bind concurrently: neither completes while the other is still
     * outstanding, so the resolver cannot be answering them one at a time. This is what the facility's refcounting
     * is for, and every other function here completes inline.
     */
    public void testFunctionsBindConcurrently() throws Exception {
        Deferred left = new Deferred("left");
        Deferred right = new Deferred("right");
        SourceFunctionResolver resolver = new SourceFunctionResolver(
            new SourceFunctionRegistry(List.of(left, right)),
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        LogicalPlan plan = new UnionAll(Source.EMPTY, List.of(named("left"), named("right")), List.of());
        PlainActionFuture<LogicalPlan> future = new PlainActionFuture<>();

        resolver.resolveSourceFunctions(plan, null, future);

        // Both were asked before either answered, which an implementation binding in sequence could not manage.
        assertThat(left.asked.await(10, TimeUnit.SECONDS), equalTo(true));
        assertThat(right.asked.await(10, TimeUnit.SECONDS), equalTo(true));
        assertThat(future.isDone(), equalTo(false));

        left.release();
        right.release();
        LogicalPlan resolved = future.actionGet(10, TimeUnit.SECONDS);
        assertThat(resolved.children().get(0), instanceOf(LocalRelation.class));
        assertThat(resolved.children().get(1), instanceOf(LocalRelation.class));
    }

    /**
     * Every call binds when the answers arrive together from as many threads as there are calls.
     * <p>
     * {@link #testFunctionsBindConcurrently} proves the binds are <i>outstanding</i> at the same time, but it
     * releases them one after another on the test thread, so nothing else here has two answers landing at once.
     * This covers that: the releases are held at a barrier and go through together, across the whole width.
     * <p>
     * What it is not is a gate on how the binds are collected, though it looks like one: a shared unsynchronized
     * map would pass it, because such a map is pre-sized from the leaf count and the window left is a probe
     * collision of a few instructions. A race that narrow is not made deterministic by repeating it, which is why
     * the resolver gives each bind a slot of its own instead.
     */
    public void testEveryCallBindsWhenTheAnswersArriveTogether() throws Exception {
        int calls = 16;
        for (int attempt = 0; attempt < 20; attempt++) {
            List<Deferred> functions = new ArrayList<>(calls);
            List<LogicalPlan> children = new ArrayList<>(calls);
            for (int i = 0; i < calls; i++) {
                functions.add(new Deferred("f" + i));
                children.add(named("f" + i));
            }
            SourceFunctionResolver resolver = new SourceFunctionResolver(
                new SourceFunctionRegistry(new ArrayList<SourceFunction>(functions)),
                EsExecutors.DIRECT_EXECUTOR_SERVICE
            );
            PlainActionFuture<LogicalPlan> future = new PlainActionFuture<>();

            resolver.resolveSourceFunctions(new UnionAll(Source.EMPTY, children, List.of()), null, future);

            for (Deferred function : functions) {
                assertThat(function.asked.await(10, TimeUnit.SECONDS), equalTo(true));
            }

            // One thread per call, all released from the barrier, so the answers overlap rather than queue.
            CyclicBarrier start = new CyclicBarrier(calls);
            List<Thread> threads = new ArrayList<>(calls);
            for (Deferred function : functions) {
                Thread thread = new Thread(() -> {
                    try {
                        start.await(10, TimeUnit.SECONDS);
                    } catch (Exception e) {
                        throw new AssertionError(e);
                    }
                    function.release();
                });
                threads.add(thread);
                thread.start();
            }
            for (Thread thread : threads) {
                thread.join();
            }

            LogicalPlan resolved = future.actionGet(10, TimeUnit.SECONDS);
            // Every child bound, and bound to a relation rather than to whatever another call's bind left behind.
            assertThat(resolved.children(), hasSize(calls));
            for (LogicalPlan child : resolved.children()) {
                assertThat(child, instanceOf(LocalRelation.class));
            }
        }
    }

    /** A function that fails its listener fails the query, rather than the resolver answering with a partial plan. */
    public void testBindFailureFailsTheQuery() {
        Deferred failing = new Deferred("left");
        SourceFunctionResolver resolver = new SourceFunctionResolver(
            new SourceFunctionRegistry(List.of(failing)),
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        PlainActionFuture<LogicalPlan> future = new PlainActionFuture<>();

        resolver.resolveSourceFunctions(named("left"), null, future);
        failing.fail(new IllegalStateException("listing refused"));

        IllegalStateException e = expectThrows(IllegalStateException.class, future::actionGet);
        assertThat(e.getMessage(), containsString("listing refused"));
    }

    /**
     * The plan comes back on the executor the resolver was given, whatever thread the function answered on.
     * <p>
     * This is not decoration: the caller resumes planning on a particular pool and asserts it, while a function
     * that does I/O completes wherever its response landed - a transport worker, or the pool a field-caps handler
     * runs on. Without the fork, every function doing I/O has to remember to do this itself, and forgetting it
     * trips an assertion on a snapshot build and silently runs planning on the wrong pool in production.
     */
    public void testPlanIsHandedBackOnTheGivenExecutor() throws Exception {
        AtomicInteger forks = new AtomicInteger();
        Executor recording = command -> {
            forks.incrementAndGet();
            command.run();
        };
        Deferred function = new Deferred("left");
        SourceFunctionResolver resolver = new SourceFunctionResolver(new SourceFunctionRegistry(List.of(function)), recording);
        PlainActionFuture<LogicalPlan> future = new PlainActionFuture<>();

        resolver.resolveSourceFunctions(named("left"), null, future);
        // Answered from another thread, as a function doing I/O would be.
        Thread elsewhere = new Thread(function::release);
        elsewhere.start();
        elsewhere.join();

        assertThat(future.actionGet(10, TimeUnit.SECONDS), instanceOf(LocalRelation.class));
        assertThat(forks.get(), equalTo(1));
    }

    /** And a query naming no function never leaves the caller's thread, so it pays nothing for that fork. */
    public void testNoFunctionMeansNoFork() {
        AtomicInteger forks = new AtomicInteger();
        Executor recording = command -> {
            forks.incrementAndGet();
            command.run();
        };
        SourceFunctionResolver resolver = new SourceFunctionResolver(new SourceFunctionRegistry(List.of()), recording);
        PlainActionFuture<LogicalPlan> future = new PlainActionFuture<>();

        LogicalPlan plan = new UnresolvedRelation(
            Source.EMPTY,
            new IndexPattern(Source.EMPTY, "logs"),
            false,
            List.of(),
            IndexMode.STANDARD,
            null
        );
        resolver.resolveSourceFunctions(plan, null, future);

        assertThat(future.actionGet(), equalTo(plan));
        assertThat(forks.get(), equalTo(0));
    }

    /** Answers its listener only when told to, so a test can hold two binds open at once. */
    private static class Deferred implements SourceFunction {

        private final String name;
        private final SetOnce<Source> source = new SetOnce<>();
        private final CountDownLatch asked = new CountDownLatch(1);
        private final SetOnce<ActionListener<LeafPlan>> listener = new SetOnce<>();

        Deferred(String name) {
            this.name = name;
        }

        void release() {
            listener.get().onResponse(counting(source.get(), 0));
        }

        void fail(Exception e) {
            listener.get().onFailure(e);
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public SourceFunctionSignature signature() {
            return SourceFunctionSignature.of(SourceFunctionSignature.string("x"));
        }

        @Override
        public void toRelation(
            SourceFunctionInvocation call,
            List<NamedExpression> metadataFields,
            ProjectMetadata projectMetadata,
            ActionListener<LeafPlan> listener
        ) {
            this.source.set(call.source());
            this.listener.set(listener);
            asked.countDown();
        }
    }

    /** Completes its listener with nothing, which is the one way a leaf can be left unbound. */
    private static class Nulling implements SourceFunction {

        @Override
        public String name() {
            return "nulling";
        }

        @Override
        public SourceFunctionSignature signature() {
            return SourceFunctionSignature.of(SourceFunctionSignature.string("x"));
        }

        @Override
        public void toRelation(
            SourceFunctionInvocation call,
            List<NamedExpression> metadataFields,
            ProjectMetadata projectMetadata,
            ActionListener<LeafPlan> listener
        ) {
            listener.onResponse(null);
        }
    }

    /** Hands back another call rather than a relation, which the rewrite would otherwise leave in the plan. */
    private static class Recursing implements SourceFunction {

        @Override
        public String name() {
            return "recursing";
        }

        @Override
        public SourceFunctionSignature signature() {
            return SourceFunctionSignature.of(SourceFunctionSignature.string("x"));
        }

        @Override
        public void toRelation(
            SourceFunctionInvocation call,
            List<NamedExpression> metadataFields,
            ProjectMetadata projectMetadata,
            ActionListener<LeafPlan> listener
        ) {
            listener.onResponse(new UnresolvedSourceFunctionRelation(call.source(), call, metadataFields));
        }
    }

    /**
     * A function completing with nothing is refused by name. Unguarded the rewrite leaves the leaf in place and the
     * query fails further on as an NPE naming no function, which is a bug report nobody can act on.
     */
    public void testAFunctionBindingNoRelationIsRefusedByName() {
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> resolve(new Nulling(), named("nulling")));
        assertThat(e.getMessage(), containsString("[nulling] bound no relation for a call in this query"));
    }

    /**
     * And a function handing back another call is refused too, rather than the leaf surviving the rewrite and being
     * reported to the user as their own syntax being unresolved.
     */
    public void testAFunctionReturningAnotherCallIsRefusedByName() {
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> resolve(new Recursing(), named("recursing")));
        assertThat(e.getMessage(), containsString("[recursing] returned another source function call"));
    }

    private static UnresolvedSourceFunctionRelation named(String function) {
        Source source = Source.EMPTY;
        return new UnresolvedSourceFunctionRelation(
            source,
            new SourceFunctionInvocation(source, function, List.of(new SourceFunctionArgument.Value(source, DataType.KEYWORD, "1"))),
            List.of()
        );
    }

    /** Records the named values it was handed, so a test can assert the function saw them rather than the record. */
    private static class MapReading implements SourceFunction {

        private Map<String, Object> seen;

        @Override
        public String name() {
            return "mapreading";
        }

        @Override
        public SourceFunctionSignature signature() {
            return SourceFunctionSignature.of(
                SourceFunctionSignature.string("upto"),
                SourceFunctionSignature.optional(SourceFunctionSignature.namedValues("options"))
            );
        }

        @Override
        public void toRelation(
            SourceFunctionInvocation call,
            List<NamedExpression> metadataFields,
            ProjectMetadata projectMetadata,
            ActionListener<LeafPlan> listener
        ) {
            seen = call.namedValues(1);
            listener.onResponse(counting(call.source(), 0));
        }
    }

    /** Throws where a function may well throw: working out its own columns, before the listener goes anywhere. */
    private static class Throwing implements SourceFunction {

        @Override
        public String name() {
            return "boom";
        }

        @Override
        public SourceFunctionSignature signature() {
            return SourceFunctionSignature.of(SourceFunctionSignature.string("x"));
        }

        @Override
        public void toRelation(
            SourceFunctionInvocation call,
            List<NamedExpression> metadataFields,
            ProjectMetadata projectMetadata,
            ActionListener<LeafPlan> listener
        ) {
            throw new IllegalStateException("cannot say what my columns are");
        }
    }

    /**
     * A function may throw instead of completing its listener, and the failure has to arrive the way every other
     * failure does: through the listener. Without the guard the exception escapes this method synchronously - so a
     * caller that catches it and then waits on the listener waits for good, because the ref acquired before the
     * function ran is never released and the count never reaches zero.
     */
    public void testFunctionThrowingInsteadOfCompletingFailsTheQuery() {
        SourceFunctionResolver resolver = new SourceFunctionResolver(
            new SourceFunctionRegistry(List.of(new Throwing())),
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        Source source = Source.EMPTY;
        UnresolvedSourceFunctionRelation leaf = new UnresolvedSourceFunctionRelation(
            source,
            new SourceFunctionInvocation(source, "boom", List.of(new SourceFunctionArgument.Value(source, DataType.KEYWORD, "1"))),
            List.of()
        );
        PlainActionFuture<LogicalPlan> future = new PlainActionFuture<>();
        resolver.resolveSourceFunctions(leaf, null, future);

        IllegalStateException e = expectThrows(IllegalStateException.class, future::actionGet);
        assertThat(e.getMessage(), containsString("cannot say what my columns are"));
    }

    /** Completes its listener and then throws, which is the shape the resolver's notifyOnce wrapper is there for. */
    private static class CompletesThenThrows implements SourceFunction {

        @Override
        public String name() {
            return "completes_then_throws";
        }

        @Override
        public SourceFunctionSignature signature() {
            return SourceFunctionSignature.of(SourceFunctionSignature.string("x"));
        }

        @Override
        public void toRelation(
            SourceFunctionInvocation call,
            List<NamedExpression> metadataFields,
            ProjectMetadata projectMetadata,
            ActionListener<LeafPlan> listener
        ) {
            listener.onResponse(counting(call.source(), 2));
            throw new IllegalStateException("answered, and then fell over");
        }
    }

    /**
     * A function that answers and then throws is answered, not failed. {@code ActionListener.run} catches what
     * {@code toRelation} threw and routes it to the listener the function has already completed, so the second
     * notification is the one the wrapper swallows. Without it the reference count is released twice: an
     * {@code AssertionError} here, and in production, where assertions are off, a count reaching zero early and
     * the plan substituted while a bind is still outstanding. Delete the {@code notifyOnce} call in
     * {@code resolveSourceFunctions} and this is what reds.
     */
    public void testAFunctionCompletingAndThenThrowingIsAnswered() {
        assertThat(resolve(new CompletesThenThrows(), named("completes_then_throws")), instanceOf(LocalRelation.class));
    }

    /**
     * Two leaves can compare equal - {@link UnresolvedSourceFunctionRelation}'s equality excludes its own source -
     * so each must still bind and receive its own relation. Keyed by equality instead, both would collapse onto
     * one relation instance and the same attribute ids would be installed twice in one plan.
     */
    public void testTwoCallsSharingASourceEachGetTheirOwnRelation() {
        UnresolvedSourceFunctionRelation left = call("2");
        UnresolvedSourceFunctionRelation right = call("2");
        assertThat(left, equalTo(right));

        Numbers function = new Numbers();
        LogicalPlan resolved = resolve(function, new UnionAll(Source.EMPTY, List.of(left, right), List.of()));

        assertThat(function.bindings.get(), equalTo(2));
        List<LogicalPlan> children = resolved.children();
        assertThat(children.get(0), instanceOf(LocalRelation.class));
        assertThat(children.get(1), instanceOf(LocalRelation.class));
        assertThat(children.get(0) == children.get(1), equalTo(false));
    }

    /**
     * And the mirror of it: one leaf the plan reaches from several parents is asked once per parent. {@code FORK}
     * builds that shape — {@code LogicalPlanBuilder#visitForkCommand} applies every branch's factory to the one
     * plan above it — so {@code FROM f(x) | FORK (..) (..)} arrives here with the same leaf object under each
     * branch, and each place gets its own relation. A function doing I/O therefore pays a round trip per place,
     * which is what the rest of the pipeline already costs for a fork.
     */
    public void testALeafReachedFromSeveralParentsIsAskedPerParent() {
        Numbers function = new Numbers();
        UnresolvedSourceFunctionRelation shared = call("2");
        LogicalPlan plan = new UnionAll(
            Source.EMPTY,
            List.of(new Filter(Source.EMPTY, shared, Literal.TRUE), new Filter(Source.EMPTY, shared, Literal.TRUE)),
            List.of()
        );

        LogicalPlan resolved = resolve(function, plan);

        assertThat(function.bindings.get(), equalTo(2));
        LogicalPlan left = resolved.children().get(0).children().get(0);
        LogicalPlan right = resolved.children().get(1).children().get(0);
        assertThat(left, instanceOf(LocalRelation.class));
        assertThat(right, instanceOf(LocalRelation.class));
        // Two places, two relations, which is what an index pattern already does: an UnresolvedRelation under two
        // branches is visited once per branch by Analyzer.ResolveTable and yields a new EsRelation each time.
        // Nothing is shared, so nothing has to be made safe to share.
        assertNotSame(left, right);
        // And each carries its own page, so one reader releasing its block cannot reach the other's.
        Page first = ((LocalRelation) left).supplier().get();
        Page second = ((LocalRelation) right).supplier().get();
        assertNotSame(first, second);
        assertNotSame(first.getBlock(0), second.getBlock(0));
    }

    /** What a function hands back is installed as it came: the facility adds no wrapper of its own. */
    public void testARelationIsInstalledExactlyAsItCameBack() {
        LogicalPlan resolved = resolve(new Numbers(), call("2"));

        assertThat(((LocalRelation) resolved).supplier(), instanceOf(ImmediateLocalSupplier.class));
    }

    /**
     * Answers every call from one relation built once, which is the obvious thing to write for a function whose
     * rows follow from its arguments, and what a per-argument cache produces for free.
     */
    private static class Caching implements SourceFunction {
        private final LocalRelation cached = counting(Source.EMPTY, 2);

        @Override
        public String name() {
            return "caching";
        }

        @Override
        public SourceFunctionSignature signature() {
            return SourceFunctionSignature.of(SourceFunctionSignature.string("x"));
        }

        @Override
        public void toRelation(
            SourceFunctionInvocation call,
            List<NamedExpression> metadataFields,
            ProjectMetadata projectMetadata,
            ActionListener<LeafPlan> listener
        ) {
            listener.onResponse(cached);
        }
    }

    /**
     * One relation may not stand in two places, and the facility will not repair it: the first consumer releases
     * the page and the second reads a released block, while the attributes are shared too, so one plan would
     * carry the same ids under two parents. Refused with the function named, rather than crashing in execution
     * with a message that names nothing the author can act on.
     */
    public void testAFunctionAnsweringTwoCallsWithOneRelationIsRefused() {
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> resolve(new Caching(), new UnionAll(Source.EMPTY, List.of(named("caching"), named("caching")), List.of()))
        );
        assertThat(e.getMessage(), containsString("source function [caching] answered more than one place with one relation"));
    }

    /**
     * And the arm asking per place created: ONE call, written once, standing under two parents. A function that
     * caches per argument is correct for every query without a FORK or a repeated LET reference, and wrong the
     * moment there is one -- the two places would hold one page and one set of attribute ids. Refused the same
     * way, and the message has to name that shape, because the author of such a function is looking for a second
     * call that was never written.
     */
    public void testOneCallUnderSeveralParentsAnsweredWithOneRelationIsRefused() {
        UnresolvedSourceFunctionRelation shared = named("caching");
        LogicalPlan plan = new UnionAll(
            Source.EMPTY,
            List.of(new Filter(Source.EMPTY, shared, Literal.TRUE), new Filter(Source.EMPTY, shared, Literal.TRUE)),
            List.of()
        );

        IllegalStateException e = expectThrows(IllegalStateException.class, () -> resolve(new Caching(), plan));

        assertThat(e.getMessage(), containsString("source function [caching] answered more than one place"));
        assertThat(e.getMessage(), containsString("FORK branches or LET references"));
    }

    /** The discriminating half: that same cached relation under a single call stands, because it is in one place. */
    public void testACachedRelationUnderOneCallStands() {
        assertThat(resolve(new Caching(), named("caching")), instanceOf(LocalRelation.class));
    }

    /**
     * And the same refusal for a relation the engine completes, which is the case that reads as harmless. Each
     * occurrence of an unresolved leaf does become its own relation downstream, so sharing one looks free — but
     * what runs downstream keys a leaf by identity and pairs it with a scope that varies per place.
     * {@code DatasetResolver} collects a scoped relation per occurrence, each carrying its own
     * {@code wildcardsMatchDatasets}, then skips any relation already in an identity map, so one object at two
     * places is resolved once under whichever scope reached it first and the other place inherits that answer.
     * One rule for both kinds, because a carve-out is what a function author reads as permission.
     */
    public void testAFunctionAnsweringTwoPlacesWithOneUnresolvedRelationIsRefused() {
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> resolve(
                new CachingUnresolved(),
                new UnionAll(Source.EMPTY, List.of(named("caching_unresolved"), named("caching_unresolved")), List.of())
            )
        );
        assertThat(e.getMessage(), containsString("source function [caching_unresolved] answered more than one place"));
    }

    /**
     * Distinct relation objects are not enough, and this is the shape that proves it: a function holding one
     * page, wrapping it in a new {@link LocalRelation} with freshly minted columns per call. Object identity
     * passes it, and both places would still read the one page — whichever consumer gets there first releases
     * the blocks the other is still holding.
     */
    public void testTwoPlacesAnsweredWithOnePageIsRefused() {
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> resolve(
                new SharingOnePage(),
                new UnionAll(Source.EMPTY, List.of(named("sharing_one_page"), named("sharing_one_page")), List.of())
            )
        );
        assertThat(e.getMessage(), containsString("source function [sharing_one_page] answered more than one place"));
        assertThat(e.getMessage(), containsString("same page"));
    }

    /**
     * And the other half of the obligation, which no page check would catch: its own page each time, and one
     * static column list, so the two places carry attributes with the same {@code NameId}s. Ids are what the
     * rest of the engine resolves references by, so one plan would carry the same ids under two parents.
     */
    public void testTwoPlacesAnsweredWithOneColumnIdIsRefused() {
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> resolve(
                new SharingColumnIds(),
                new UnionAll(Source.EMPTY, List.of(named("sharing_column_ids"), named("sharing_column_ids")), List.of())
            )
        );
        assertThat(e.getMessage(), containsString("source function [sharing_column_ids] answered more than one place"));
        assertThat(e.getMessage(), containsString("same column, by its id,"));
    }

    /** One page held across calls, with columns minted fresh each time, so only the page is shared. */
    private static class SharingOnePage implements SourceFunction {
        private final LocalSupplier shared = counting(Source.EMPTY, 2).supplier();

        @Override
        public String name() {
            return "sharing_one_page";
        }

        @Override
        public SourceFunctionSignature signature() {
            return SourceFunctionSignature.of(SourceFunctionSignature.string("x"));
        }

        @Override
        public void toRelation(
            SourceFunctionInvocation call,
            List<NamedExpression> metadataFields,
            ProjectMetadata projectMetadata,
            ActionListener<LeafPlan> listener
        ) {
            Attribute column = new ReferenceAttribute(Source.EMPTY, "n", DataType.LONG);
            listener.onResponse(new LocalRelation(call.source(), List.of(column), shared));
        }
    }

    /** The mirror: a page of its own each call, around one column list, so only the ids are shared. */
    private static class SharingColumnIds implements SourceFunction {
        private final List<Attribute> columns = List.of(new ReferenceAttribute(Source.EMPTY, "n", DataType.LONG));

        @Override
        public String name() {
            return "sharing_column_ids";
        }

        @Override
        public SourceFunctionSignature signature() {
            return SourceFunctionSignature.of(SourceFunctionSignature.string("x"));
        }

        @Override
        public void toRelation(
            SourceFunctionInvocation call,
            List<NamedExpression> metadataFields,
            ProjectMetadata projectMetadata,
            ActionListener<LeafPlan> listener
        ) {
            listener.onResponse(new LocalRelation(call.source(), columns, counting(Source.EMPTY, 2).supplier()));
        }
    }

    /** As {@link Caching}, but handing back a leaf the engine completes rather than one it does not. */
    private static class CachingUnresolved implements SourceFunction {
        private final UnresolvedRelation cached = new UnresolvedRelation(
            Source.EMPTY,
            new IndexPattern(Source.EMPTY, "logs"),
            false,
            List.of(),
            IndexMode.STANDARD,
            null
        );

        @Override
        public String name() {
            return "caching_unresolved";
        }

        @Override
        public SourceFunctionSignature signature() {
            return SourceFunctionSignature.of(SourceFunctionSignature.string("x"));
        }

        @Override
        public void toRelation(
            SourceFunctionInvocation call,
            List<NamedExpression> metadataFields,
            ProjectMetadata projectMetadata,
            ActionListener<LeafPlan> listener
        ) {
            listener.onResponse(cached);
        }
    }

    /**
     * A query may name many calls, and they go out together, so the ceiling is checked before any of them is
     * asked rather than by the branch-count check on the optimized plan, which would arrive after every one of
     * them had been made.
     */
    public void testAQueryWiderThanTheCeilingIsRefusedBeforeAnythingIsAsked() {
        Numbers function = new Numbers();
        List<LogicalPlan> calls = new ArrayList<>();
        for (int i = 0; i <= SourceFunctionResolver.MAX_CALLS_PER_QUERY; i++) {
            calls.add(call("1"));
        }

        ParsingException e = expectThrows(ParsingException.class, () -> resolve(function, new UnionAll(Source.EMPTY, calls, List.of())));

        assertThat(
            e.getMessage(),
            containsString("a query may call up to [" + SourceFunctionResolver.MAX_CALLS_PER_QUERY + "] source functions, found [")
        );
        assertThat("nothing may have been asked", function.bindings.get(), equalTo(0));
    }

    /** And a query exactly at the ceiling is not refused, so the check discriminates rather than always firing. */
    public void testAQueryAtTheCeilingIsAllowed() {
        Numbers function = new Numbers();
        List<LogicalPlan> calls = new ArrayList<>();
        for (int i = 0; i < SourceFunctionResolver.MAX_CALLS_PER_QUERY; i++) {
            calls.add(call("1"));
        }

        resolve(function, new UnionAll(Source.EMPTY, calls, List.of()));

        assertThat(function.bindings.get(), equalTo(SourceFunctionResolver.MAX_CALLS_PER_QUERY));
    }

    /** A call of the same function with nothing in the parentheses, which its signature refuses. */
    private static UnresolvedSourceFunctionRelation callWithNoArguments() {
        Source source = Source.EMPTY;
        return new UnresolvedSourceFunctionRelation(source, new SourceFunctionInvocation(source, "numbers", List.of()), List.of());
    }

    private static UnresolvedSourceFunctionRelation call(String argument) {
        Source source = Source.EMPTY;
        return new UnresolvedSourceFunctionRelation(
            source,
            new SourceFunctionInvocation(source, "numbers", List.of(new SourceFunctionArgument.Value(source, DataType.KEYWORD, argument))),
            List.of()
        );
    }

    /**
     * A leaf built by hand: one {@code long} column holding {@code 0..count}. The facility takes any
     * {@link LeafPlan}, so nothing here needs a base class to produce one — which is also what keeps these tests
     * about resolution rather than about how a particular kind of function fills a page.
     */
    private static LocalRelation counting(Source source, long count) {
        Attribute column = new ReferenceAttribute(Source.EMPTY, "n", DataType.LONG);
        LongBlock block;
        try (LongBlock.Builder builder = PlannerUtils.NON_BREAKING_BLOCK_FACTORY.newLongBlockBuilder((int) count)) {
            for (long i = 0; i < count; i++) {
                builder.appendLong(i);
            }
            block = builder.build();
        }
        return new LocalRelation(source, List.of(column), LocalSupplier.of(new Page((int) count, block)));
    }

    private static LogicalPlan resolve(SourceFunction function, LogicalPlan plan) {
        SourceFunctionResolver resolver = new SourceFunctionResolver(
            new SourceFunctionRegistry(List.of(function)),
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        PlainActionFuture<LogicalPlan> future = new PlainActionFuture<>();
        resolver.resolveSourceFunctions(plan, null, future);
        return future.actionGet();
    }

    /** The same, against cluster state, which is what the view check reads. */
    private static LogicalPlan resolve(SourceFunction function, LogicalPlan plan, ProjectMetadata projectMetadata) {
        SourceFunctionResolver resolver = new SourceFunctionResolver(
            new SourceFunctionRegistry(List.of(function)),
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        PlainActionFuture<LogicalPlan> future = new PlainActionFuture<>();
        resolver.resolveSourceFunctions(plan, projectMetadata, future);
        return future.actionGet();
    }

    /** A project holding one view called {@code v}, and no index. */
    private static ProjectMetadata projectWithAView() {
        return ProjectMetadata.builder(ProjectId.DEFAULT)
            .putCustom(ViewMetadata.TYPE, new ViewMetadata(Map.of("v", new View("v", "FROM logs"))))
            .build();
    }

    /** Hands back an index pattern of the test's choosing, which is the shape a view name would arrive in. */
    private static class Patterned implements SourceFunction {
        private final String pattern;

        Patterned(String pattern) {
            this.pattern = pattern;
        }

        @Override
        public String name() {
            return "patterned";
        }

        @Override
        public SourceFunctionSignature signature() {
            return SourceFunctionSignature.of(SourceFunctionSignature.string("x"));
        }

        @Override
        public void toRelation(
            SourceFunctionInvocation call,
            List<NamedExpression> metadataFields,
            ProjectMetadata projectMetadata,
            ActionListener<LeafPlan> listener
        ) {
            listener.onResponse(
                new UnresolvedRelation(
                    call.source(),
                    new IndexPattern(call.source(), pattern),
                    false,
                    metadataFields,
                    IndexMode.STANDARD,
                    null
                )
            );
        }
    }

    /**
     * A view a function names would never be expanded -- expansion has already run by the time a function is asked
     * -- so the name would reach the user as an unknown index for a view they can list. Refused instead, naming
     * both the function and the view.
     */
    public void testAFunctionReturningAViewNameIsRefused() {
        ParsingException e = expectThrows(
            ParsingException.class,
            () -> resolve(new Patterned("v"), named("patterned"), projectWithAView())
        );
        assertThat(e.getMessage(), containsString("source function [patterned] cannot read view [v]"));
    }

    /** And a view inside a comma-separated list is found too, which is how a function returning several names returns them. */
    public void testAFunctionReturningAListWithAViewInItIsRefused() {
        ParsingException e = expectThrows(
            ParsingException.class,
            () -> resolve(new Patterned("logs, v ,other"), named("patterned"), projectWithAView())
        );
        assertThat(e.getMessage(), containsString("cannot read view [v]"));
    }

    /** An ordinary index name is not refused for a view existing elsewhere in the project, so the check discriminates. */
    public void testAFunctionReturningAnIndexNameIsAllowedWhereAViewExists() {
        LogicalPlan resolved = resolve(new Patterned("logs"), named("patterned"), projectWithAView());
        assertThat(resolved, instanceOf(UnresolvedRelation.class));
        assertThat(((UnresolvedRelation) resolved).indexPattern().indexPattern(), equalTo("logs"));
    }

    /**
     * And a wildcard that would match the view is allowed. Whether a wildcard matches views at all is the query's
     * own setting, and a function's pattern is not where that was asked, so refusing it would refuse a pattern on
     * a setting its author never saw.
     */
    public void testAFunctionReturningAWildcardThatCouldMatchAViewIsAllowed() {
        LogicalPlan resolved = resolve(new Patterned("v*"), named("patterned"), projectWithAView());
        assertThat(resolved, instanceOf(UnresolvedRelation.class));
    }

    /** With no view in the project the same return stands, so the refusal is about the view and not about the shape. */
    public void testTheSameReturnStandsWhereNoViewExists() {
        LogicalPlan resolved = resolve(new Patterned("v"), named("patterned"), ProjectMetadata.builder(ProjectId.DEFAULT).build());
        assertThat(resolved, instanceOf(UnresolvedRelation.class));
    }

}

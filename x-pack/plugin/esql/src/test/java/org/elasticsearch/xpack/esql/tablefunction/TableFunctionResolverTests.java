/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.tablefunction;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedTableFunctionRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * The facility driven by a function that exists only here.
 * <p>
 * That is the point of these tests rather than an accident of them: a function written in a test exercises every
 * part of the shared path, so nothing in that path can have been shaped by a particular shipped function's needs.
 * A test that reached for a real function would prove the opposite.
 */
public class TableFunctionResolverTests extends ESTestCase {

    /**
     * A function whose rows are a literal. Four methods, and none of them about a wire format, a data node or a
     * privilege — a function of this shape has none of those, and {@link CoordinatorRowsFunction} gives it no seam
     * to acquire one.
     */
    private static class Numbers extends CoordinatorRowsFunction {

        private final AtomicInteger bindings = new AtomicInteger();

        @Override
        public String name() {
            return "numbers";
        }

        @Override
        public TableFunctionSignature signature() {
            return TableFunctionSignature.of(TableFunctionSignature.name("upto"));
        }

        @Override
        public void validate(TableFunctionInvocation call) {
            if (call.name(0).matches("\\d+") == false) {
                throw new ParsingException(call.arguments().get(0).source(), "[numbers] takes a number, found [{}]", call.name(0));
            }
        }

        @Override
        protected void write(TableFunctionInvocation call, ProjectMetadata projectMetadata, TableRows rows, ActionListener<Void> listener) {
            bindings.incrementAndGet();
            long upto = Long.parseLong(call.name(0));
            computed(listener, out -> {
                TableRows.Longs n = out.longs("n");
                for (long i = 0; i < upto; i++) {
                    n.append(i);
                }
            }, rows);
        }
    }

    public void testPlanWithNoTableFunctionIsHandedBackUntouched() {
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
        LogicalPlan plan = new UnresolvedTableFunctionRelation(
            source,
            new TableFunctionInvocation(source, "nope", List.of(new TableFunctionArgument.Name(source, "1"))),
            List.of()
        );

        ParsingException e = expectThrows(ParsingException.class, () -> resolve(new Numbers(), plan));
        assertThat(e.getMessage(), containsString("unknown table function [nope], expected one of [numbers]"));
    }

    /** The shape is checked by the shared signature, so a function writes no arity code of its own. */
    public void testWrongArityIsRefusedBeforeTheFunctionIsAsked() {
        Numbers function = new Numbers();
        LogicalPlan plan = new UnresolvedTableFunctionRelation(
            Source.EMPTY,
            new TableFunctionInvocation(Source.EMPTY, "numbers", List.of()),
            List.of()
        );

        expectThrows(ParsingException.class, () -> resolve(function, plan));
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
        UnresolvedTableFunctionRelation leaf = new UnresolvedTableFunctionRelation(
            source,
            new TableFunctionInvocation(
                source,
                "mapreading",
                List.of(new TableFunctionArgument.Name(source, "1"), new TableFunctionArgument.NamedValues(source, Map.of("k", "v")))
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
        TableFunctionResolver resolver = new TableFunctionResolver(
            new TableFunctionRegistry(List.of(left, right)),
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        LogicalPlan plan = new UnionAll(Source.EMPTY, List.of(named("left"), named("right")), List.of());
        PlainActionFuture<LogicalPlan> future = new PlainActionFuture<>();

        resolver.resolveTableFunctions(plan, null, future);

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

    /** A function that fails its listener fails the query, rather than the resolver answering with a partial plan. */
    public void testBindFailureFailsTheQuery() {
        Deferred failing = new Deferred("left");
        TableFunctionResolver resolver = new TableFunctionResolver(
            new TableFunctionRegistry(List.of(failing)),
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        PlainActionFuture<LogicalPlan> future = new PlainActionFuture<>();

        resolver.resolveTableFunctions(named("left"), null, future);
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
        TableFunctionResolver resolver = new TableFunctionResolver(new TableFunctionRegistry(List.of(function)), recording);
        PlainActionFuture<LogicalPlan> future = new PlainActionFuture<>();

        resolver.resolveTableFunctions(named("left"), null, future);
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
        TableFunctionResolver resolver = new TableFunctionResolver(new TableFunctionRegistry(List.of()), recording);
        PlainActionFuture<LogicalPlan> future = new PlainActionFuture<>();

        LogicalPlan plan = new UnresolvedRelation(
            Source.EMPTY,
            new IndexPattern(Source.EMPTY, "logs"),
            false,
            List.of(),
            IndexMode.STANDARD,
            null
        );
        resolver.resolveTableFunctions(plan, null, future);

        assertThat(future.actionGet(), equalTo(plan));
        assertThat(forks.get(), equalTo(0));
    }

    /** Answers its listener only when told to, so a test can hold two binds open at once. */
    private static class Deferred extends CoordinatorRowsFunction {

        private final String name;
        private final SetOnce<TableRows> rows = new SetOnce<>();
        private final CountDownLatch asked = new CountDownLatch(1);
        private final SetOnce<ActionListener<Void>> listener = new SetOnce<>();

        Deferred(String name) {
            this.name = name;
        }

        void release() {
            rows.get().longs("n");
            listener.get().onResponse(null);
        }

        void fail(Exception e) {
            listener.get().onFailure(e);
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public TableFunctionSignature signature() {
            return TableFunctionSignature.of(TableFunctionSignature.name("x"));
        }

        @Override
        protected void write(TableFunctionInvocation call, ProjectMetadata projectMetadata, TableRows rows, ActionListener<Void> listener) {
            this.rows.set(rows);
            this.listener.set(listener);
            asked.countDown();
        }
    }

    private static UnresolvedTableFunctionRelation named(String function) {
        Source source = Source.EMPTY;
        return new UnresolvedTableFunctionRelation(
            source,
            new TableFunctionInvocation(source, function, List.of(new TableFunctionArgument.Name(source, "1"))),
            List.of()
        );
    }

    /** Records the named values it was handed, so a test can assert the function saw them rather than the record. */
    private static class MapReading extends CoordinatorRowsFunction {

        private Map<String, Object> seen;

        @Override
        public String name() {
            return "mapreading";
        }

        @Override
        public TableFunctionSignature signature() {
            return TableFunctionSignature.of(
                TableFunctionSignature.name("upto"),
                TableFunctionSignature.optional(TableFunctionSignature.namedValues("options"))
            );
        }

        @Override
        protected void write(TableFunctionInvocation call, ProjectMetadata projectMetadata, TableRows rows, ActionListener<Void> listener) {
            seen = call.namedValues(1);
            computed(listener, out -> out.longs("n"), rows);
        }
    }

    /** Throws where a function may well throw: working out its own columns, before the listener goes anywhere. */
    private static class Throwing extends CoordinatorRowsFunction {

        @Override
        public String name() {
            return "boom";
        }

        @Override
        public TableFunctionSignature signature() {
            return TableFunctionSignature.of(TableFunctionSignature.name("x"));
        }

        @Override
        protected void write(TableFunctionInvocation call, ProjectMetadata projectMetadata, TableRows rows, ActionListener<Void> listener) {
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
        TableFunctionResolver resolver = new TableFunctionResolver(
            new TableFunctionRegistry(List.of(new Throwing())),
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        Source source = Source.EMPTY;
        UnresolvedTableFunctionRelation leaf = new UnresolvedTableFunctionRelation(
            source,
            new TableFunctionInvocation(source, "boom", List.of(new TableFunctionArgument.Name(source, "1"))),
            List.of()
        );
        PlainActionFuture<LogicalPlan> future = new PlainActionFuture<>();
        resolver.resolveTableFunctions(leaf, null, future);

        IllegalStateException e = expectThrows(IllegalStateException.class, future::actionGet);
        assertThat(e.getMessage(), containsString("cannot say what my columns are"));
    }

    /**
     * Two leaves can compare equal - {@link UnresolvedTableFunctionRelation}'s equality excludes its own source -
     * so each must still bind and receive its own relation. Keyed by equality instead, both would collapse onto
     * one relation instance and the same attribute ids would be installed twice in one plan.
     */
    public void testTwoCallsSharingASourceEachGetTheirOwnRelation() {
        UnresolvedTableFunctionRelation left = call("2");
        UnresolvedTableFunctionRelation right = call("2");
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
     * One row holding one cell, so a test names the cell it hands over instead of counting brackets. The three
     * levels - rows, row, cell - are easy to be one out on, and being one out turns a nested list into an ordinary
     * multivalue, which is exactly the thing under test.
     */
    private static List<List<Object>> oneCell(Object cell) {
        List<Object> row = new ArrayList<>(1);
        row.add(cell);
        return List.of(row);
    }

    /** A call of the same function with nothing in the parentheses, which its signature refuses. */
    private static UnresolvedTableFunctionRelation callWithNoArguments() {
        Source source = Source.EMPTY;
        return new UnresolvedTableFunctionRelation(source, new TableFunctionInvocation(source, "numbers", List.of()), List.of());
    }

    private static UnresolvedTableFunctionRelation call(String argument) {
        Source source = Source.EMPTY;
        return new UnresolvedTableFunctionRelation(
            source,
            new TableFunctionInvocation(source, "numbers", List.of(new TableFunctionArgument.Name(source, argument))),
            List.of()
        );
    }

    private static LogicalPlan resolve(TableFunction function, LogicalPlan plan) {
        TableFunctionResolver resolver = new TableFunctionResolver(
            new TableFunctionRegistry(List.of(function)),
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        PlainActionFuture<LogicalPlan> future = new PlainActionFuture<>();
        resolver.resolveTableFunctions(plan, null, future);
        return future.actionGet();
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.tablefunction;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedTableFunctionRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.versionfield.Version;

import java.net.InetAddress;
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
        protected List<Attribute> columns(TableFunctionInvocation call, TableFunctionContext context) {
            return List.of(column("n", DataType.LONG));
        }

        @Override
        protected void rows(TableFunctionInvocation call, TableFunctionContext context, ActionListener<List<List<Object>>> listener) {
            bindings.incrementAndGet();
            long upto = Long.parseLong(call.name(0));
            computed(listener, () -> {
                List<List<Object>> rows = new ArrayList<>();
                for (long i = 0; i < upto; i++) {
                    rows.add(List.of(i));
                }
                return rows;
            });
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

    /** A zero-row answer still owes one block per declared column, or the page disagrees with the output. */
    public void testZeroRowsStillCarriesOneBlockPerColumn() {
        LocalRelation rows = (LocalRelation) resolve(new Numbers(), call("0"));
        assertThat(rows.supplier().get().getBlockCount(), equalTo(1));
        assertThat(rows.supplier().get().getPositionCount(), equalTo(0));
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

    /**
     * A value of the wrong Java type is refused for the string-backed types too, where the block builder would
     * have accepted anything: {@code BlockUtils.appendValue}'s {@code BYTES_REF} arm stringifies rather than
     * casting, so typing the builder from the declaration does not catch this on its own.
     */
    public void testAnIntegerIsRefusedForAKeywordColumn() {
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> resolve(new Typed(DataType.KEYWORD, List.of(List.of(42))), call("1"))
        );
        assertThat(e.getMessage(), containsString("produced [Integer] for column [c] declared [keyword]"));
    }

    /**
     * A wrong-typed cell inside a list says what the list held. Reporting the value's own class names the JDK's
     * list implementation — {@code List12} for a two-element {@code List.of} — which tells a function's author
     * neither that it was a list nor what was in it.
     */
    public void testAListOfTheWrongTypeSaysWhatItHeld() {
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> resolve(new Typed(DataType.LONG, List.of(List.of(List.of("a", "b")))), call("1"))
        );
        assertThat(e.getMessage(), containsString("produced a list of [String] for column [c] declared [long]"));
    }

    /**
     * A declared type the engine has no builder for is refused against the column that declared it. Both calls
     * behind this throw about the element type alone, which is not the thing a function's author chose.
     */
    public void testADeclaredTypeWithNoBuilderNamesItsColumn() {
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> resolve(new Typed(DataType.FLOAT, List.of(List.of(1.0f))), call("1"))
        );
        assertThat(e.getMessage(), containsString("declared column [c] as [float], which it cannot produce values for"));
    }

    /**
     * {@code NULL} is the other way a declaration fails, and it fails later: it has an element type and a block
     * builder, and no arm that appends a value to one, so every value is refused rather than the column being
     * refused up front. Reported against the column all the same, which is the whole point.
     */
    public void testANullDeclaredColumnNamesItsColumn() {
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> resolve(new Typed(DataType.NULL, List.of(List.of("anything"))), call("1"))
        );
        assertThat(e.getMessage(), containsString("produced [String] for column [c] declared [null]"));
        assertThat(e.getCause(), instanceOf(UnsupportedOperationException.class));
    }

    /**
     * For a type whose bytes are an encoding rather than the text, the seam encodes: a function hands back an
     * {@code InetAddress} and the block carries the sixteen bytes {@code InetAddressPoint} produces.
     */
    public void testAnIpColumnIsEncodedBySeam() throws Exception {
        LocalRelation rows = (LocalRelation) resolve(new Typed(DataType.IP, List.of(List.of(InetAddress.getByName("1.2.3.4")))), call("1"));

        BytesRef held = ((BytesRefBlock) rows.supplier().get().getBlock(0)).getBytesRef(0, new BytesRef());
        assertThat(held.length, equalTo(InetAddressPoint.BYTES));
        assertThat(held, equalTo(new BytesRef(InetAddressPoint.encode(InetAddress.getByName("1.2.3.4")))));
    }

    /**
     * And the shape that cannot be checked is refused. A {@code BytesRef} of the text "1.2.3.4" is seven bytes
     * where sixteen belong — right element type, wrong bytes, and nothing downstream able to tell. Checking the
     * Java type alone let this through, which is what the encoding above exists to close.
     */
    public void testATextBytesRefIsRefusedForAnIpColumn() {
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> resolve(new Typed(DataType.IP, List.of(List.of(new BytesRef("1.2.3.4")))), call("1"))
        );
        assertThat(e.getMessage(), containsString("declared [ip], which needs an InetAddress"));
    }

    /** An already-encoded ip is still accepted, because sixteen bytes is an invariant the seam can check. */
    public void testAnEncodedIpBytesRefIsAccepted() throws Exception {
        BytesRef encoded = new BytesRef(InetAddressPoint.encode(InetAddress.getByName("1.2.3.4")));
        LocalRelation rows = (LocalRelation) resolve(new Typed(DataType.IP, List.of(List.of(encoded))), call("1"));

        assertThat(rows.supplier().get().getPositionCount(), equalTo(1));
    }

    /** A version is encoded too, and its encoding is not its text: twelve bytes for "1.2.3", not five. */
    public void testAVersionColumnIsEncoded() {
        LocalRelation rows = (LocalRelation) resolve(new Typed(DataType.VERSION, List.of(List.of("1.2.3"))), call("1"));

        BytesRef held = ((BytesRefBlock) rows.supplier().get().getBlock(0)).getBytesRef(0, new BytesRef());
        assertThat(held, equalTo(new Version("1.2.3").toBytesRef()));
        assertThat(held.length, equalTo(12));
    }

    /**
     * A multivalued cell is one position holding several values, which the block builders support — so the seam
     * takes a List for every type rather than for the numeric family only, and encodes each element.
     */
    public void testAMultivaluedKeywordCellIsOnePosition() {
        LocalRelation rows = (LocalRelation) resolve(new Typed(DataType.KEYWORD, List.of(List.of(List.of("a", "b")))), call("1"));

        assertThat(rows.supplier().get().getPositionCount(), equalTo(1));
        assertThat(rows.supplier().get().getBlock(0).getValueCount(0), equalTo(2));
    }

    /** The same for a numeric column, which went through a different arm and had no test either. */
    public void testAMultivaluedLongCellIsOnePosition() {
        LocalRelation rows = (LocalRelation) resolve(new Mismatched(List.of(List.of(List.of(1L, 2L)))), call("1"));

        assertThat(rows.supplier().get().getPositionCount(), equalTo(1));
        assertThat(rows.supplier().get().getBlock(0).getValueCount(0), equalTo(2));
    }

    /** A String is what a keyword column is for, so that one passes. */
    public void testAStringIsAcceptedForAKeywordColumn() {
        LocalRelation rows = (LocalRelation) resolve(new Typed(DataType.KEYWORD, List.of(List.of("ok"))), call("1"));

        assertThat(rows.supplier().get().getPositionCount(), equalTo(1));
        assertThat(rows.supplier().get().getBlock(0).elementType(), equalTo(ElementType.BYTES_REF));
    }

    /** Declares one column of a given type and hands back whatever it was built with. */
    private static class Typed extends CoordinatorRowsFunction {

        private final DataType type;
        private final List<List<Object>> rows;

        Typed(DataType type, List<List<Object>> rows) {
            this.type = type;
            this.rows = rows;
        }

        @Override
        public String name() {
            return "numbers";
        }

        @Override
        public TableFunctionSignature signature() {
            return TableFunctionSignature.of(TableFunctionSignature.name("upto"));
        }

        @Override
        protected List<Attribute> columns(TableFunctionInvocation call, TableFunctionContext context) {
            return List.of(column("c", type));
        }

        @Override
        protected void rows(TableFunctionInvocation call, TableFunctionContext context, ActionListener<List<List<Object>>> listener) {
            computed(listener, () -> rows);
        }
    }

    /** Answers its listener only when told to, so a test can hold two binds open at once. */
    private static class Deferred extends CoordinatorRowsFunction {

        private final String name;
        private final CountDownLatch asked = new CountDownLatch(1);
        private final SetOnce<ActionListener<List<List<Object>>>> listener = new SetOnce<>();

        Deferred(String name) {
            this.name = name;
        }

        void release() {
            listener.get().onResponse(List.of());
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
        protected List<Attribute> columns(TableFunctionInvocation call, TableFunctionContext context) {
            return List.of(column("n", DataType.LONG));
        }

        @Override
        protected void rows(TableFunctionInvocation call, TableFunctionContext context, ActionListener<List<List<Object>>> listener) {
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
        protected List<Attribute> columns(TableFunctionInvocation call, TableFunctionContext context) {
            seen = call.namedValues(1);
            return List.of(column("n", DataType.LONG));
        }

        @Override
        protected void rows(TableFunctionInvocation call, TableFunctionContext context, ActionListener<List<List<Object>>> listener) {
            computed(listener, List::of);
        }
    }

    /**
     * Declares one {@code LONG} column and hands back whatever it was built with, so a test can put a value of a
     * different Java type, or a row of a different width, through the base class.
     */
    private static class Mismatched extends CoordinatorRowsFunction {

        private final List<List<Object>> rows;

        Mismatched(List<List<Object>> rows) {
            this.rows = rows;
        }

        @Override
        public String name() {
            return "numbers";
        }

        @Override
        public TableFunctionSignature signature() {
            return TableFunctionSignature.of(TableFunctionSignature.name("upto"));
        }

        @Override
        protected List<Attribute> columns(TableFunctionInvocation call, TableFunctionContext context) {
            return List.of(column("n", DataType.LONG));
        }

        @Override
        protected void rows(TableFunctionInvocation call, TableFunctionContext context, ActionListener<List<List<Object>>> listener) {
            computed(listener, () -> rows);
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
        protected List<Attribute> columns(TableFunctionInvocation call, TableFunctionContext context) {
            throw new IllegalStateException("cannot say what my columns are");
        }

        @Override
        protected void rows(TableFunctionInvocation call, TableFunctionContext context, ActionListener<List<List<Object>>> listener) {
            throw new AssertionError("not reached");
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
     * The declared column type is the block's type, so a value of another Java type is refused against the column
     * that declared it. Typed from the data instead, an {@code Integer} would have quietly produced an {@code INT}
     * block under a {@code LONG} attribute, and only a consumer casting the block would have noticed.
     */
    public void testValueDisagreeingWithItsDeclaredTypeIsRefused() {
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> resolve(new Mismatched(List.of(List.of(1), List.of(2))), call("2"))
        );
        assertThat(e.getMessage(), containsString("produced [Integer] for column [n] declared [long]"));
    }

    /** The declared type is what the block carries when the function does agree with it. */
    public void testBlockIsTypedFromTheDeclaration() {
        LocalRelation rows = (LocalRelation) resolve(new Mismatched(List.of(List.of(1L), List.of(2L))), call("2"));

        assertThat(rows.output().get(0).dataType(), equalTo(DataType.LONG));
        assertThat(rows.supplier().get().getBlock(0).elementType(), equalTo(ElementType.LONG));
        assertThat(rows.supplier().get().getPositionCount(), equalTo(2));
    }

    /** A function disagreeing with its own column list is refused rather than building a page of another width. */
    public void testRowOfTheWrongWidthIsRefused() {
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> resolve(new Mismatched(List.of(List.of(1L, 2L))), call("1"))
        );
        assertThat(e.getMessage(), containsString("row of [2] values for [1] columns"));
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

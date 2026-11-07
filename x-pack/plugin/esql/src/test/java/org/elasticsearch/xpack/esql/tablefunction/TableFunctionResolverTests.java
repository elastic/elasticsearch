/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.tablefunction;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * The facility driven by a function that exists only here.
 * <p>
 * That is the point of these tests rather than an accident of them: this change adds the facility and registers
 * no function at all, so if a function written in a test exercises every part of the shared path, nothing in that
 * path can have been shaped by one real function's needs. A test that reached for the one shipped function would
 * prove the opposite.
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

    /** A map argument reaches the function as the last element of one flat list, as a scalar function's does. */
    public void testMapArrivesAsAnArgument() {
        Source source = Source.EMPTY;
        TableFunctionInvocation call = new TableFunctionInvocation(
            source,
            "numbers",
            List.of(new TableFunctionArgument.Name(source, "1"), new TableFunctionArgument.NamedValues(source, Map.of("k", "v")))
        );
        assertThat(call.arguments().size(), equalTo(2));
        assertThat(call.namedValues(1), equalTo(Map.of("k", "v")));
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
        TableFunctionResolver resolver = new TableFunctionResolver(new TableFunctionRegistry(List.of(function)));
        PlainActionFuture<LogicalPlan> future = new PlainActionFuture<>();
        resolver.resolveTableFunctions(plan, null, future);
        return future.actionGet();
    }
}

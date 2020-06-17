/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.eql.tree;

import org.elasticsearch.xpack.eql.expression.function.scalar.string.CIDRMatch;
import org.elasticsearch.xpack.eql.expression.function.scalar.string.CIDRMatchFunctionPipe;
import org.elasticsearch.xpack.eql.expression.function.scalar.string.Concat;
import org.elasticsearch.xpack.eql.expression.function.scalar.string.ConcatFunctionPipe;
import org.elasticsearch.xpack.eql.expression.function.scalar.string.Match;
import org.elasticsearch.xpack.eql.expression.function.scalar.string.Wildcard;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.InPipe;
import org.elasticsearch.xpack.ql.tree.Node;
import org.elasticsearch.xpack.ql.tree.NodeSubclassTests;

import java.util.List;

import static java.util.Arrays.asList;

/**
 * Looks for all subclasses of {@link Node} and verifies that they
 * implement {@link Node#info()} and
 * {@link Node#replaceChildren(List)} sanely. It'd be better if
 * each subclass had its own test case that verified those methods
 * and any other interesting things that that they do but we're a
 * long way from that and this gets the job done for now.
 * <p>
 * This test attempts to use reflection to create believeable nodes
 * and manipulate them in believeable ways with as little knowledge
 * of the actual subclasses as possible. This is problematic because
 * it is possible, for example, for nodes to stackoverflow because
 * they <strong>can</strong> contain themselves. So this class
 * <strong>does</strong> have some {@link Node}-subclass-specific
 * knowledge. As little as I could get away with though.
 * <p>
 * When there are actual tests for a subclass of {@linkplain Node}
 * then this class will do two things:
 * <ul>
 * <li>Skip running any tests for that subclass entirely.
 * <li>Delegate to that test to build nodes of that type when a
 * node of that type is called for.
 * </ul>
 */
public class EqlNodeSubclassTests<T extends B, B extends Node<B>> extends NodeSubclassTests<T, B> {

    private static final List<Class<?>> CLASSES_WITH_MIN_TWO_CHILDREN = asList(In.class, InPipe.class,
        CIDRMatch.class, CIDRMatchFunctionPipe.class, Concat.class, ConcatFunctionPipe.class, Match.class, Wildcard.class);

    public EqlNodeSubclassTests(Class<T> subclass) {
        super(subclass);
    }

    @Override
    protected boolean hasAtLeastTwoChildren(Class<? extends Node<?>> toBuildClass) {
        return CLASSES_WITH_MIN_TWO_CHILDREN.stream().anyMatch(toBuildClass::equals);
    }

}
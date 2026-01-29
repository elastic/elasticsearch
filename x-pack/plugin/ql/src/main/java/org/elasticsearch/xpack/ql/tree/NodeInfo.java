/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.tree;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;

/**
 * Information about a {@link Node}.
 * <p>
 * All the uses of this are fairly non-OO and we're looking
 * for ways to use this less and less.
 * <p>
 * The implementations of this class are super copy-and-paste-ish
 * but they are better then the sneaky reflection tricks we had
 * earlier. Still terrifying.
 *
 * @param <T> actual subclass of node that produced this {@linkplain NodeInfo}
 */
public abstract class NodeInfo<T extends Node<?>> {
    protected final T node;

    private NodeInfo(T node) {
        this.node = node;
    }

    /**
     * Values for all properties on the instance that created
     * this {@linkplain NodeInfo}.
     */
    public final List<Object> properties() {
        return unmodifiableList(innerProperties());
    }

    protected abstract List<Object> innerProperties();

    /**
     * Transform the properties on {@code node}, returning a new instance
     * of {@code N} if any properties change.
     */
    final <E> T transform(Function<? super E, ? extends E> rule, Class<E> typeToken) {
        List<?> children = node.children();

        Function<Object, Object> realRule = p -> {
            if (p != children && (p == null || typeToken.isInstance(p)) && false == children.contains(p)) {
                return rule.apply(typeToken.cast(p));
            }
            return p;
        };
        return innerTransform(realRule);
    }

    protected abstract T innerTransform(Function<Object, Object> rule);

    /**
     * Builds a {@link NodeInfo} for Nodes without any properties.
     */
    public static <T extends Node<?>> NodeInfo<T> create(T n) {
        return new NodeInfo<T>(n) {
            @Override
            protected List<Object> innerProperties() {
                return emptyList();
            }

            protected T innerTransform(Function<Object, Object> rule) {
                return node;
            }
        };
    }

    public static <T extends Node<?>, P1> NodeInfo<T> create(T n, BiFunction<Source, P1, T> ctor, P1 p1) {
        return new NodeInfo<T>(n) {
            @Override
            protected List<Object> innerProperties() {
                return Arrays.asList(p1);
            }

            protected T innerTransform(Function<Object, Object> rule) {
                boolean same = true;

                @SuppressWarnings("unchecked")
                P1 newP1 = (P1) rule.apply(p1);
                same &= Objects.equals(p1, newP1);

                return same ? node : ctor.apply(node.source(), newP1);
            }
        };
    }

    public static <T extends Node<?>, P1, P2> NodeInfo<T> create(T n, NodeCtor2<P1, P2, T> ctor, P1 p1, P2 p2) {
        return new NodeInfo<T>(n) {
            @Override
            protected List<Object> innerProperties() {
                return Arrays.asList(p1, p2);
            }

            protected T innerTransform(Function<Object, Object> rule) {
                boolean same = true;

                @SuppressWarnings("unchecked")
                P1 newP1 = (P1) rule.apply(p1);
                same &= Objects.equals(p1, newP1);
                @SuppressWarnings("unchecked")
                P2 newP2 = (P2) rule.apply(p2);
                same &= Objects.equals(p2, newP2);

                return same ? node : ctor.apply(node.source(), newP1, newP2);
            }
        };
    }

    public interface NodeCtor2<P1, P2, T> {
        T apply(Source l, P1 p1, P2 p2);
    }

    public static <T extends Node<?>, P1, P2, P3> NodeInfo<T> create(T n, NodeCtor3<P1, P2, P3, T> ctor, P1 p1, P2 p2, P3 p3) {
        return new NodeInfo<T>(n) {
            @Override
            protected List<Object> innerProperties() {
                return Arrays.asList(p1, p2, p3);
            }

            protected T innerTransform(Function<Object, Object> rule) {
                boolean same = true;

                @SuppressWarnings("unchecked")
                P1 newP1 = (P1) rule.apply(p1);
                same &= Objects.equals(p1, newP1);
                @SuppressWarnings("unchecked")
                P2 newP2 = (P2) rule.apply(p2);
                same &= Objects.equals(p2, newP2);
                @SuppressWarnings("unchecked")
                P3 newP3 = (P3) rule.apply(p3);
                same &= Objects.equals(p3, newP3);

                return same ? node : ctor.apply(node.source(), newP1, newP2, newP3);
            }
        };
    }

    public interface NodeCtor3<P1, P2, P3, T> {
        T apply(Source l, P1 p1, P2 p2, P3 p3);
    }

    public static <T extends Node<?>, P1, P2, P3, P4> NodeInfo<T> create(
        T n,
        NodeCtor4<P1, P2, P3, P4, T> ctor,
        P1 p1,
        P2 p2,
        P3 p3,
        P4 p4
    ) {
        return new NodeInfo<T>(n) {
            @Override
            protected List<Object> innerProperties() {
                return Arrays.asList(p1, p2, p3, p4);
            }

            protected T innerTransform(Function<Object, Object> rule) {
                boolean same = true;

                @SuppressWarnings("unchecked")
                P1 newP1 = (P1) rule.apply(p1);
                same &= Objects.equals(p1, newP1);
                @SuppressWarnings("unchecked")
                P2 newP2 = (P2) rule.apply(p2);
                same &= Objects.equals(p2, newP2);
                @SuppressWarnings("unchecked")
                P3 newP3 = (P3) rule.apply(p3);
                same &= Objects.equals(p3, newP3);
                @SuppressWarnings("unchecked")
                P4 newP4 = (P4) rule.apply(p4);
                same &= Objects.equals(p4, newP4);

                return same ? node : ctor.apply(node.source(), newP1, newP2, newP3, newP4);
            }
        };
    }

    public interface NodeCtor4<P1, P2, P3, P4, T> {
        T apply(Source l, P1 p1, P2 p2, P3 p3, P4 p4);
    }

    public static <T extends Node<?>, P1, P2, P3, P4, P5> NodeInfo<T> create(
        T n,
        NodeCtor5<P1, P2, P3, P4, P5, T> ctor,
        P1 p1,
        P2 p2,
        P3 p3,
        P4 p4,
        P5 p5
    ) {
        return new NodeInfo<T>(n) {
            @Override
            protected List<Object> innerProperties() {
                return Arrays.asList(p1, p2, p3, p4, p5);
            }

            protected T innerTransform(Function<Object, Object> rule) {
                boolean same = true;

                @SuppressWarnings("unchecked")
                P1 newP1 = (P1) rule.apply(p1);
                same &= Objects.equals(p1, newP1);
                @SuppressWarnings("unchecked")
                P2 newP2 = (P2) rule.apply(p2);
                same &= Objects.equals(p2, newP2);
                @SuppressWarnings("unchecked")
                P3 newP3 = (P3) rule.apply(p3);
                same &= Objects.equals(p3, newP3);
                @SuppressWarnings("unchecked")
                P4 newP4 = (P4) rule.apply(p4);
                same &= Objects.equals(p4, newP4);
                @SuppressWarnings("unchecked")
                P5 newP5 = (P5) rule.apply(p5);
                same &= Objects.equals(p5, newP5);

                return same ? node : ctor.apply(node.source(), newP1, newP2, newP3, newP4, newP5);
            }
        };
    }

    public interface NodeCtor5<P1, P2, P3, P4, P5, T> {
        T apply(Source l, P1 p1, P2 p2, P3 p3, P4 p4, P5 p5);
    }

    public static <T extends Node<?>, P1, P2, P3, P4, P5, P6> NodeInfo<T> create(
        T n,
        NodeCtor6<P1, P2, P3, P4, P5, P6, T> ctor,
        P1 p1,
        P2 p2,
        P3 p3,
        P4 p4,
        P5 p5,
        P6 p6
    ) {
        return new NodeInfo<T>(n) {
            @Override
            protected List<Object> innerProperties() {
                return Arrays.asList(p1, p2, p3, p4, p5, p6);
            }

            protected T innerTransform(Function<Object, Object> rule) {
                boolean same = true;

                @SuppressWarnings("unchecked")
                P1 newP1 = (P1) rule.apply(p1);
                same &= Objects.equals(p1, newP1);
                @SuppressWarnings("unchecked")
                P2 newP2 = (P2) rule.apply(p2);
                same &= Objects.equals(p2, newP2);
                @SuppressWarnings("unchecked")
                P3 newP3 = (P3) rule.apply(p3);
                same &= Objects.equals(p3, newP3);
                @SuppressWarnings("unchecked")
                P4 newP4 = (P4) rule.apply(p4);
                same &= Objects.equals(p4, newP4);
                @SuppressWarnings("unchecked")
                P5 newP5 = (P5) rule.apply(p5);
                same &= Objects.equals(p5, newP5);
                @SuppressWarnings("unchecked")
                P6 newP6 = (P6) rule.apply(p6);
                same &= Objects.equals(p6, newP6);

                return same ? node : ctor.apply(node.source(), newP1, newP2, newP3, newP4, newP5, newP6);
            }
        };
    }

    public interface NodeCtor6<P1, P2, P3, P4, P5, P6, T> {
        T apply(Source l, P1 p1, P2 p2, P3 p3, P4 p4, P5 p5, P6 p6);
    }

    public static <T extends Node<?>, P1, P2, P3, P4, P5, P6, P7> NodeInfo<T> create(
        T n,
        NodeCtor7<P1, P2, P3, P4, P5, P6, P7, T> ctor,
        P1 p1,
        P2 p2,
        P3 p3,
        P4 p4,
        P5 p5,
        P6 p6,
        P7 p7
    ) {
        return new NodeInfo<T>(n) {
            @Override
            protected List<Object> innerProperties() {
                return Arrays.asList(p1, p2, p3, p4, p5, p6, p7);
            }

            protected T innerTransform(Function<Object, Object> rule) {
                boolean same = true;

                @SuppressWarnings("unchecked")
                P1 newP1 = (P1) rule.apply(p1);
                same &= Objects.equals(p1, newP1);
                @SuppressWarnings("unchecked")
                P2 newP2 = (P2) rule.apply(p2);
                same &= Objects.equals(p2, newP2);
                @SuppressWarnings("unchecked")
                P3 newP3 = (P3) rule.apply(p3);
                same &= Objects.equals(p3, newP3);
                @SuppressWarnings("unchecked")
                P4 newP4 = (P4) rule.apply(p4);
                same &= Objects.equals(p4, newP4);
                @SuppressWarnings("unchecked")
                P5 newP5 = (P5) rule.apply(p5);
                same &= Objects.equals(p5, newP5);
                @SuppressWarnings("unchecked")
                P6 newP6 = (P6) rule.apply(p6);
                same &= Objects.equals(p6, newP6);
                @SuppressWarnings("unchecked")
                P7 newP7 = (P7) rule.apply(p7);
                same &= Objects.equals(p7, newP7);

                return same ? node : ctor.apply(node.source(), newP1, newP2, newP3, newP4, newP5, newP6, newP7);
            }
        };
    }

    public interface NodeCtor7<P1, P2, P3, P4, P5, P6, P7, T> {
        T apply(Source l, P1 p1, P2 p2, P3 p3, P4 p4, P5 p5, P6 p6, P7 p7);
    }

    public static <T extends Node<?>, P1, P2, P3, P4, P5, P6, P7, P8> NodeInfo<T> create(
        T n,
        NodeCtor8<P1, P2, P3, P4, P5, P6, P7, P8, T> ctor,
        P1 p1,
        P2 p2,
        P3 p3,
        P4 p4,
        P5 p5,
        P6 p6,
        P7 p7,
        P8 p8
    ) {
        return new NodeInfo<T>(n) {
            @Override
            protected List<Object> innerProperties() {
                return Arrays.asList(p1, p2, p3, p4, p5, p6, p7, p8);
            }

            protected T innerTransform(Function<Object, Object> rule) {
                boolean same = true;

                @SuppressWarnings("unchecked")
                P1 newP1 = (P1) rule.apply(p1);
                same &= Objects.equals(p1, newP1);
                @SuppressWarnings("unchecked")
                P2 newP2 = (P2) rule.apply(p2);
                same &= Objects.equals(p2, newP2);
                @SuppressWarnings("unchecked")
                P3 newP3 = (P3) rule.apply(p3);
                same &= Objects.equals(p3, newP3);
                @SuppressWarnings("unchecked")
                P4 newP4 = (P4) rule.apply(p4);
                same &= Objects.equals(p4, newP4);
                @SuppressWarnings("unchecked")
                P5 newP5 = (P5) rule.apply(p5);
                same &= Objects.equals(p5, newP5);
                @SuppressWarnings("unchecked")
                P6 newP6 = (P6) rule.apply(p6);
                same &= Objects.equals(p6, newP6);
                @SuppressWarnings("unchecked")
                P7 newP7 = (P7) rule.apply(p7);
                same &= Objects.equals(p7, newP7);
                @SuppressWarnings("unchecked")
                P8 newP8 = (P8) rule.apply(p8);
                same &= Objects.equals(p8, newP8);

                return same ? node : ctor.apply(node.source(), newP1, newP2, newP3, newP4, newP5, newP6, newP7, newP8);
            }
        };
    }

    public interface NodeCtor8<P1, P2, P3, P4, P5, P6, P7, P8, T> {
        T apply(Source l, P1 p1, P2 p2, P3 p3, P4 p4, P5 p5, P6 p6, P7 p7, P8 p8);
    }

    public static <T extends Node<?>, P1, P2, P3, P4, P5, P6, P7, P8, P9> NodeInfo<T> create(
        T n,
        NodeCtor9<P1, P2, P3, P4, P5, P6, P7, P8, P9, T> ctor,
        P1 p1,
        P2 p2,
        P3 p3,
        P4 p4,
        P5 p5,
        P6 p6,
        P7 p7,
        P8 p8,
        P9 p9
    ) {
        return new NodeInfo<T>(n) {
            @Override
            protected List<Object> innerProperties() {
                return Arrays.asList(p1, p2, p3, p4, p5, p6, p7, p8, p9);
            }

            protected T innerTransform(Function<Object, Object> rule) {
                boolean same = true;

                @SuppressWarnings("unchecked")
                P1 newP1 = (P1) rule.apply(p1);
                same &= Objects.equals(p1, newP1);
                @SuppressWarnings("unchecked")
                P2 newP2 = (P2) rule.apply(p2);
                same &= Objects.equals(p2, newP2);
                @SuppressWarnings("unchecked")
                P3 newP3 = (P3) rule.apply(p3);
                same &= Objects.equals(p3, newP3);
                @SuppressWarnings("unchecked")
                P4 newP4 = (P4) rule.apply(p4);
                same &= Objects.equals(p4, newP4);
                @SuppressWarnings("unchecked")
                P5 newP5 = (P5) rule.apply(p5);
                same &= Objects.equals(p5, newP5);
                @SuppressWarnings("unchecked")
                P6 newP6 = (P6) rule.apply(p6);
                same &= Objects.equals(p6, newP6);
                @SuppressWarnings("unchecked")
                P7 newP7 = (P7) rule.apply(p7);
                same &= Objects.equals(p7, newP7);
                @SuppressWarnings("unchecked")
                P8 newP8 = (P8) rule.apply(p8);
                same &= Objects.equals(p8, newP8);
                @SuppressWarnings("unchecked")
                P9 newP9 = (P9) rule.apply(p9);
                same &= Objects.equals(p9, newP9);

                return same ? node : ctor.apply(node.source(), newP1, newP2, newP3, newP4, newP5, newP6, newP7, newP8, newP9);
            }
        };
    }

    public interface NodeCtor9<P1, P2, P3, P4, P5, P6, P7, P8, P9, T> {
        T apply(Source l, P1 p1, P2 p2, P3 p3, P4 p4, P5 p5, P6 p6, P7 p7, P8 p8, P9 p9);
    }

    public static <T extends Node<?>, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10> NodeInfo<T> create(
        T n,
        NodeCtor10<P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, T> ctor,
        P1 p1,
        P2 p2,
        P3 p3,
        P4 p4,
        P5 p5,
        P6 p6,
        P7 p7,
        P8 p8,
        P9 p9,
        P10 p10
    ) {
        return new NodeInfo<T>(n) {
            @Override
            protected List<Object> innerProperties() {
                return Arrays.asList(p1, p2, p3, p4, p5, p6, p7, p8, p9, p10);
            }

            protected T innerTransform(Function<Object, Object> rule) {
                boolean same = true;

                @SuppressWarnings("unchecked")
                P1 newP1 = (P1) rule.apply(p1);
                same &= Objects.equals(p1, newP1);
                @SuppressWarnings("unchecked")
                P2 newP2 = (P2) rule.apply(p2);
                same &= Objects.equals(p2, newP2);
                @SuppressWarnings("unchecked")
                P3 newP3 = (P3) rule.apply(p3);
                same &= Objects.equals(p3, newP3);
                @SuppressWarnings("unchecked")
                P4 newP4 = (P4) rule.apply(p4);
                same &= Objects.equals(p4, newP4);
                @SuppressWarnings("unchecked")
                P5 newP5 = (P5) rule.apply(p5);
                same &= Objects.equals(p5, newP5);
                @SuppressWarnings("unchecked")
                P6 newP6 = (P6) rule.apply(p6);
                same &= Objects.equals(p6, newP6);
                @SuppressWarnings("unchecked")
                P7 newP7 = (P7) rule.apply(p7);
                same &= Objects.equals(p7, newP7);
                @SuppressWarnings("unchecked")
                P8 newP8 = (P8) rule.apply(p8);
                same &= Objects.equals(p8, newP8);
                @SuppressWarnings("unchecked")
                P9 newP9 = (P9) rule.apply(p9);
                same &= Objects.equals(p9, newP9);
                @SuppressWarnings("unchecked")
                P10 newP10 = (P10) rule.apply(p10);
                same &= Objects.equals(p10, newP10);

                return same ? node : ctor.apply(node.source(), newP1, newP2, newP3, newP4, newP5, newP6, newP7, newP8, newP9, newP10);
            }
        };
    }

    public interface NodeCtor10<P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, T> {
        T apply(Source l, P1 p1, P2 p2, P3 p3, P4 p4, P5 p5, P6 p6, P7 p7, P8 p8, P9 p9, P10 p10);
    }
}

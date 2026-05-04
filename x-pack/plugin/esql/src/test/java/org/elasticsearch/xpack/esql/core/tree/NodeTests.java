/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.tree;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.esql.core.tree.SourceTests.randomSource;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class NodeTests extends ESTestCase {
    public void testToStringNoChildren() {
        assertToString(new NoChildren(randomSource(), "thing"), equalTo("NoChildren[thing]"));
    }

    public void testToStringDepth1() {
        assertToString(dummy(), equalTo("ChildrenAreAProperty[thing]"));
    }

    public void testToStringDepth2() {
        assertToString(dummy(dummy()), equalTo("""
            ChildrenAreAProperty[thing]
            \\_ChildrenAreAProperty[thing]"""));

        int length = between(2, 100);
        StringBuilder expected = new StringBuilder("ChildrenAreAProperty[thing]");
        expected.append("\n|_ChildrenAreAProperty[thing]".repeat(Math.max(0, length - 1)));
        expected.append("\n\\_ChildrenAreAProperty[thing]");
        assertToString(dummy(IntStream.range(0, length).mapToObj(i -> dummy()).toArray(Dummy[]::new)), equalTo(expected.toString()));
    }

    public void testToStringDepth3() {
        assertToString(dummy(dummy(dummy())), equalTo("""
            ChildrenAreAProperty[thing]
            \\_ChildrenAreAProperty[thing]
              \\_ChildrenAreAProperty[thing]"""));

        int length = between(2, 10);
        StringBuilder expected = new StringBuilder("ChildrenAreAProperty[thing]");
        for (int i = 0; i < length - 1; i++) {
            expected.append("\n|_ChildrenAreAProperty[thing]");
            expected.append("\n| \\_ChildrenAreAProperty[thing]");
        }
        expected.append("\n\\_ChildrenAreAProperty[thing]");
        expected.append("\n  \\_ChildrenAreAProperty[thing]");
        assertToString(dummy(IntStream.range(0, length).mapToObj(i -> dummy(dummy())).toArray(Dummy[]::new)), equalTo(expected.toString()));
    }

    public void testToStringDeepChain() {
        int depth = between(3, 100);
        Dummy node = dummy();
        for (int i = 1; i < depth; i++) {
            node = dummy(node);
        }
        StringBuilder expected = new StringBuilder("ChildrenAreAProperty[thing]");
        for (int i = 1; i < depth; i++) {
            expected.append("\n").append("  ".repeat(i - 1)).append("\\_ChildrenAreAProperty[thing]");
        }
        assertToString(node, equalTo(expected.toString()));
    }

    private Dummy dummy(Dummy... children) {
        return new ChildrenAreAProperty(randomSource(), List.of(children), "thing");
    }

    private FunctionLike fn(FunctionLike... children) {
        return new FunctionLike(randomSource(), List.of(children));
    }

    public void testToStringAChildIsAProperty() {
        NoChildren empty = new NoChildren(randomSource(), "thing");
        assertToString(new AChildIsAProperty(randomSource(), empty, "single"), equalTo("""
            AChildIsAProperty[single]
            \\_NoChildren[thing]"""));
    }

    public void testToStringFunctionLike() {
        assertToString(new NoChildren(randomSource(), fn()), equalTo("NoChildren[FunctionLike()]"));
        assertToString(new NoChildren(randomSource(), fn(fn(), fn())), equalTo("NoChildren[FunctionLike(FunctionLike(),FunctionLike())]"));
    }

    public void testToStringMaxLineWidth() {
        assertToString(new NoChildren(randomSource(), "a".repeat(110)), equalTo("NoChildren[" + "a".repeat(110) + "]"));
    }

    public void testToStringLongProperty() {
        assertToString(
            new NoChildren(randomSource(), "a".repeat(200)),
            equalTo("NoChildren[" + "a".repeat(110) + "\n" + "a".repeat(90) + "]"),
            equalTo("NoChildren[" + "a".repeat(200) + "]")
        );
    }

    public void testToStringVeryLongProperty() {
        assertToString(
            new NoChildren(randomSource(), "a".repeat(1000)),
            equalTo("NoChildren[" + ("a".repeat(110) + "\n").repeat(9) + "a".repeat(10) + "]"),
            equalTo("NoChildren[" + "a".repeat(1000) + "]")
        );
    }

    public void testToStringTruncatesAtMaxLines() {
        int len = Node.TO_STRING_MAX_LINES * Node.TO_STRING_MAX_WIDTH + 1;
        assertToString(
            new NoChildren(randomSource(), "a".repeat(len)),
            equalTo(
                "NoChildren["
                    + ("a".repeat(Node.TO_STRING_MAX_WIDTH) + "\n").repeat(Node.TO_STRING_MAX_LINES - 1)
                    + "a".repeat(Node.TO_STRING_MAX_WIDTH)
                    + "..."
                    + "]"
            ),
            equalTo("NoChildren[" + "a".repeat(len) + "]")
        );
    }

    public void testToStringNameId() {
        NameId nameId = new NameId();
        assertToString(new NoChildren(randomSource(), nameId), equalTo("NoChildren[#" + nameId + "]"));
    }

    public void testToStringList() {
        assertToString(
            new NoChildren(randomSource(), List.of("aaa", "bbb", "ccc", "ddd", "eee", "fff", "ggg", "hhh", "iii", "jjj")),
            equalTo("NoChildren[[aaa, bbb, ccc, ddd, eee, fff, ggg, hhh, iii, jjj]]")
        );
    }

    public void testToStringListWrapping() {
        assertToString(
            new NoChildren(
                randomSource(),
                List.of(
                    "aaaaaaaaaa",
                    "bbbbbbbbbb",
                    "cccccccccc",
                    "dddddddddd",
                    "eeeeeeeeee",
                    "ffffffffff",
                    "gggggggggg",
                    "hhhhhhhhhh",
                    "iiiiiiiiii",
                    "jjjjjjjjjj"
                )
            ),
            equalTo("""
                NoChildren[[aaaaaaaaaa, bbbbbbbbbb, cccccccccc, dddddddddd, eeeeeeeeee, ffffffffff, gggggggggg, hhhhhhhhhh, iiiiiiiiii, j
                jjjjjjjjj]]"""),
            equalTo(
                "NoChildren[[aaaaaaaaaa, bbbbbbbbbb, cccccccccc, dddddddddd, "
                    + "eeeeeeeeee, ffffffffff, gggggggggg, hhhhhhhhhh, iiiiiiiiii, jjjjjjjjjj]]"
            )
        );
    }

    public void testToStringListPathologicalWrapping() {
        List<String> strings = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            strings.add(String.valueOf((char) ('a' + (i % 26))));
        }
        assertToString(new NoChildren(randomSource(), strings), equalTo("""
            NoChildren[[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z, a, b, c, d, e, f, g, h, i, j, k
            , l, m, n, o, p, q, r, s, t, u, v, w, x, y, z, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u,\s
            v, w, x, y, z, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v]]"""), equalTo("NoChildren[" + strings + "]"));
    }

    private static void assertToString(Node<?> node, Matcher<String> expected) {
        assertToString(node, expected, expected);
    }

    private static void assertToString(Node<?> node, Matcher<String> limitedExpected, Matcher<String> fullExpected) {
        assertThat(node.toString(), limitedExpected);
        assertThat(node.toString(Node.NodeStringFormat.LIMITED), limitedExpected);
        assertThat(node.toString(Node.NodeStringFormat.FULL), fullExpected);
    }

    public void testRandomToString() {
        NoChildren node = new NoChildren(randomSource(), randomNestedList(3));
        String[] lines = node.toString().split("\n", -1);
        assertThat(lines.length, lessThanOrEqualTo(Node.TO_STRING_MAX_LINES));
        if (lines.length == 1) {
            assertThat(lines[0].length(), lessThanOrEqualTo(Node.TO_STRING_MAX_WIDTH + "NoChildren[]".length()));
            return;
        }
        assertThat(lines[0].length(), lessThanOrEqualTo(Node.TO_STRING_MAX_WIDTH + "NoChildren[".length()));
        for (int i = 1; i < lines.length - 1; i++) {
            assertThat(lines[i].length(), lessThanOrEqualTo(Node.TO_STRING_MAX_WIDTH));
        }
        assertThat(lines[lines.length - 1].length(), lessThanOrEqualTo(Node.TO_STRING_MAX_WIDTH + "...]".length()));
    }

    public void testWithNullChild() {
        List<Dummy> listWithNull = new ArrayList<>();
        listWithNull.add(null);
        var e = expectThrows(QlIllegalArgumentException.class, () -> new ChildrenAreAProperty(randomSource(), listWithNull, "single"));
        assertEquals("Null children are not allowed", e.getMessage());
    }

    public void testWithImmutableChildList() {
        // It's good enough that the node can be created without throwing a NPE
        var node = new ChildrenAreAProperty(randomSource(), List.of(), "single");
        assertEquals(node.children().size(), 0);
    }

    public void testTransformDownAsyncNoChildren() throws InterruptedException {
        NoChildren node = new NoChildren(randomSource(), "original");

        assertAsyncTransform(node, (n, listener) -> {
            NoChildren transformed = new NoChildren(n.source(), "transformed");
            listener.onResponse(transformed);
        }, result -> {
            assertEquals(NoChildren.class, result.getClass());
            assertEquals("transformed", result.thing());
        });
    }

    public void testTransformDownAsyncWithChildren() throws InterruptedException {
        NoChildren child1 = new NoChildren(randomSource(), "child1");
        NoChildren child2 = new NoChildren(randomSource(), "child2");
        ChildrenAreAProperty parent = new ChildrenAreAProperty(randomSource(), Arrays.asList(child1, child2), "parent");

        assertAsyncTransform(parent, (n, listener) -> {
            if (n instanceof NoChildren) {
                NoChildren nc = (NoChildren) n;
                if ("child1".equals(nc.thing())) {
                    listener.onResponse(new NoChildren(nc.source(), "transformed1"));
                } else {
                    listener.onResponse(n);
                }
            } else {
                listener.onResponse(n);
            }
        }, result -> {
            assertEquals(ChildrenAreAProperty.class, result.getClass());
            ChildrenAreAProperty transformed = (ChildrenAreAProperty) result;
            assertEquals(2, transformed.children().size());
            assertEquals("transformed1", transformed.children().get(0).thing());
            assertEquals("child2", transformed.children().get(1).thing());
        });
    }

    public void testTransformDownAsyncNoChange() throws InterruptedException {
        NoChildren node = new NoChildren(randomSource(), "unchanged");
        assertAsyncTransform(node, (n, listener) -> listener.onResponse(n), result -> assertSame(node, result));
    }

    private void assertAsyncTransform(Dummy node, BiConsumer<? super Dummy, ActionListener<Dummy>> rule, Consumer<Dummy> assertions)
        throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean listenerCalled = new AtomicBoolean(false);

        LatchedActionListener<Dummy> listener = new LatchedActionListener<>(ActionTestUtils.assertNoFailureListener(result -> {
            assertTrue("listener called more than once", listenerCalled.compareAndSet(false, true));
            assertions.accept(result);
        }), latch);

        node.transformDown(rule, listener);
        assertTrue("timed out after 5s", latch.await(5, TimeUnit.SECONDS));
    }

    public abstract static class Dummy extends Node<Dummy> {
        private final String thing;

        public Dummy(Source source, List<? extends Dummy> children, String thing) {
            super(source, new ArrayList<>(children));
            this.thing = thing;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getWriteableName() {
            throw new UnsupportedOperationException();
        }

        public String thing() {
            return thing;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (obj == null || obj.getClass() != getClass()) {
                return false;
            }
            Dummy other = (Dummy) obj;
            return thing.equals(other.thing) && children().equals(other.children());
        }

        @Override
        public int hashCode() {
            return Objects.hash(thing, children());
        }
    }

    public static class ChildrenAreAProperty extends Dummy {
        public ChildrenAreAProperty(Source source, List<Dummy> children, String thing) {
            super(source, children, thing);
        }

        @Override
        protected NodeInfo<ChildrenAreAProperty> info() {
            return NodeInfo.create(this, ChildrenAreAProperty::new, children(), thing());
        }

        @Override
        public ChildrenAreAProperty replaceChildren(List<Dummy> newChildren) {
            return new ChildrenAreAProperty(source(), newChildren, thing());
        }
    }

    public static class FunctionLike extends Dummy {
        public FunctionLike(Source source, List<? extends Dummy> children) {
            super(source, children, "");
        }

        @Override
        protected NodeInfo<FunctionLike> info() {
            return NodeInfo.create(this, FunctionLike::new, children());
        }

        @Override
        public FunctionLike replaceChildren(List<Dummy> newChildren) {
            return new FunctionLike(source(), newChildren);
        }

        @Override
        public void nodeString(StringBuilder sb, NodeStringFormat format) {
            sb.append(nodeName()).append("(");
            List<Dummy> kids = children();
            for (int i = 0; i < kids.size(); i++) {
                if (i > 0) {
                    sb.append(",");
                }
                kids.get(i).nodeString(sb, format);
            }
            sb.append(")");
        }
    }

    public static class AChildIsAProperty extends Dummy {
        public AChildIsAProperty(Source source, Dummy child, String thing) {
            super(source, singletonList(child), thing);
        }

        @Override
        protected NodeInfo<AChildIsAProperty> info() {
            return NodeInfo.create(this, AChildIsAProperty::new, child(), thing());
        }

        @Override
        public AChildIsAProperty replaceChildren(List<Dummy> newChildren) {
            return new AChildIsAProperty(source(), newChildren.get(0), thing());
        }

        public Dummy child() {
            return children().get(0);
        }
    }

    public static class NoChildren extends Dummy {
        private final Object thing;

        public NoChildren(Source source, Object thing) {
            super(source, emptyList(), thing.toString());
            this.thing = thing;
        }

        @Override
        protected NodeInfo<NoChildren> info() {
            return NodeInfo.create(this, NoChildren::new, thing);
        }

        @Override
        public Dummy replaceChildren(List<Dummy> newChildren) {
            throw new UnsupportedOperationException("no children to replace");
        }
    }

    private static List<Object> randomNestedList(int allowedDepth) {
        Supplier<Object> element = allowedDepth > 0
            ? () -> randomBoolean() ? randomNestedList(allowedDepth - 1) : randomAlphaOfLengthBetween(0, 10)
            : () -> randomAlphaOfLengthBetween(0, 10);
        return randomList(10, element);
    }

    // Returns an empty list. The returned list may be backed various implementations, some
    // allowing null some not - disallowing null disallows (throws NPE for) contains(null).
    private static <T> List<T> emptyList() {
        return randomFrom(List.of(), Collections.emptyList(), new ArrayList<>(), new LinkedList<>());
    }
}

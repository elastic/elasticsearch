/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.tree;

import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.ql.tree.SourceTests.randomSource;

public class NodeTests extends ESTestCase {
    public void testToString() {
        assertEquals("NoChildren[thing]", new NoChildren(randomSource(), "thing").toString());
        {
            ChildrenAreAProperty empty = new ChildrenAreAProperty(randomSource(), emptyList(), "thing");
            assertEquals("ChildrenAreAProperty[thing]", empty.toString());
            assertEquals("ChildrenAreAProperty[single]\n\\_ChildrenAreAProperty[thing]",
                new ChildrenAreAProperty(randomSource(), singletonList(empty), "single").toString());
            assertEquals("ChildrenAreAProperty[many]\n"
                       + "|_ChildrenAreAProperty[thing]\n"
                      + "\\_ChildrenAreAProperty[thing]",
                new ChildrenAreAProperty(randomSource(), Arrays.asList(empty, empty), "many").toString());
        }
        {
            NoChildren empty = new NoChildren(randomSource(), "thing");
            assertEquals("AChildIsAProperty[single]\n"
                      + "\\_NoChildren[thing]",
                new AChildIsAProperty(randomSource(), empty, "single").toString());
        }
    }

    public abstract static class Dummy extends Node<Dummy> {
        private final String thing;
        public Dummy(Source source, List<Dummy> children, String thing) {
            super(source, children);
            this.thing = thing;
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
            return thing.equals(other.thing)
                && children().equals(other.children());
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
        public NoChildren(Source source, String thing) {
            super(source, emptyList(), thing);
        }

        @Override
        protected NodeInfo<NoChildren> info() {
            return NodeInfo.create(this, NoChildren::new, thing());
        }

        @Override
        public Dummy replaceChildren(List<Dummy> newChildren) {
            throw new UnsupportedOperationException("no children to replace");
        }
    }
}

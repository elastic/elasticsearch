/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.tree;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Immutable tree structure.
 * The traversal is done depth-first, pre-order (first the node then its children), that is seeks up and then goes down.
 * Alternative method for post-order (children first, then node) is also offered, that is seeks down and then goes up.
 * 
 * Allows transformation which returns the same tree (if no change has been performed) or a new tree otherwise.
 * 
 * While it tries as much as possible to use functional Java, due to lack of parallelism,
 * the use of streams and iterators is not really useful and brings too much baggage which
 * might be used incorrectly. 
 * 
 * @param <T> node type
 */
public abstract class Node<T extends Node<T>> {

    private final Location location;
    private final List<T> children;

    public Node(List<T> children) {
        this(Location.EMPTY, children);
    }

    public Node(Location location, List<T> children) {
        this.location = (location != null ? location : Location.EMPTY);
        this.children = children;
    }

    public Location location() {
        return location;
    }

    public List<T> children() {
        return children;
    }

    @SuppressWarnings("unchecked")
    public void forEachDown(Consumer<? super T> action) {
        action.accept((T) this);
        children().forEach(c -> c.forEachDown(action));
    }

    @SuppressWarnings("unchecked")
    public <E extends T> void forEachDown(Consumer<? super E> action, final Class<E> typeToken) {
        forEachDown(t -> {
            if (typeToken.isInstance(t))  { 
                action.accept((E) t); 
            }
        });
    }

    @SuppressWarnings("unchecked")
    public void forEachUp(Consumer<? super T> action) {
        children().forEach(c -> c.forEachUp(action));
        action.accept((T) this);
    }

    @SuppressWarnings("unchecked")
    public <E extends T> void forEachUp(Consumer<? super E> action, final Class<E> typeToken) {
        forEachUp(t -> {
            if (typeToken.isInstance(t)) {
                action.accept((E) t);
            }
        });
    }

    public <E> void forEachPropertiesOnly(Consumer<? super E> rule, Class<E> typeToken) {
        forEachProperty(rule, typeToken);
    }

    public <E> void forEachPropertiesDown(Consumer<? super E> rule, Class<E> typeToken) {
        forEachDown(e -> e.forEachProperty(rule, typeToken));
    }

    public <E> void forEachPropertiesUp(Consumer<? super E> rule, Class<E> typeToken) {
        forEachUp(e -> e.forEachProperty(rule, typeToken));
    }

    @SuppressWarnings("unchecked")
    protected <E> void forEachProperty(Consumer<? super E> rule, Class<E> typeToken) {
        for (Object prop : NodeUtils.properties(this)) {
            // skip children (only properties are interesting)
            if (prop != children && !children.contains(prop) && typeToken.isInstance(prop)) {
                rule.accept((E) prop);
            }
        }
    }

    @SuppressWarnings("unchecked")
    public boolean anyMatch(Predicate<? super T> predicate) {
        boolean result = predicate.test((T) this);
        return result ? true : children().stream().anyMatch(c -> c.anyMatch(predicate));
    }

    // TODO: maybe add a flatMap (need to double check the Stream bit) 

    //
    // Transform methods
    //

    //
    // transform the node itself and its children
    //

    @SuppressWarnings("unchecked")
    public T transformDown(Function<? super T, ? extends T> rule) {
        T root = rule.apply((T) this);
        Node<T> node = this.equals(root) ? this : root;

        return node.transformChildren(child -> child.transformDown(rule));
    }

    @SuppressWarnings("unchecked")
    public <E extends T> T transformDown(Function<E, ? extends T> rule, final Class<E> typeToken) {
        // type filtering function
        return transformDown((t) -> (typeToken.isInstance(t) ? rule.apply((E) t) : t));
    }

    @SuppressWarnings("unchecked")
    public T transformUp(Function<? super T, ? extends T> rule) {
        T transformed = transformChildren(child -> child.transformUp(rule));
        T node = this.equals(transformed) ? (T) this : transformed;
        return rule.apply(node);
    }

    @SuppressWarnings("unchecked")
    public <E extends T> T transformUp(Function<E, ? extends T> rule, final Class<E> typeToken) {
        // type filtering function
        return transformUp((t) -> (typeToken.isInstance(t) ? rule.apply((E) t) : t));
    }

    @SuppressWarnings("unchecked")
    protected <R extends Function<? super T, ? extends T>> T transformChildren(Function<T, ? extends T> traversalOperation) {
        boolean childrenChanged = false;

        // stream() could be used but the code is just as complicated without any advantages
        // further more, it would include bring in all the associated stream/collector object creation even though in 
        // most cases the immediate tree would be quite small (0,1,2 elements)
        List<T> transformedChildren = new ArrayList<>(children().size());

        for (T child : children) {
            T next = traversalOperation.apply(child);
            if (!child.equals(next)) {
                childrenChanged = true;
            }
            else {
                // use the initial value
                next = child;
            }
            transformedChildren.add(next);
        }

        return (childrenChanged ? NodeUtils.copyTree(this, transformedChildren) : (T) this);
    }

    //
    // transform the node properties and use the tree only for navigation
    //

    public <E> T transformPropertiesOnly(Function<? super E, ? extends E> rule, Class<E> typeToken) {
        return transformNodeProps(rule, typeToken);
    }

    public <E> T transformPropertiesDown(Function<? super E, ? extends E> rule, Class<E> typeToken) {
        return transformDown(t -> t.transformNodeProps(rule, typeToken));
    }

    public <E> T transformPropertiesUp(Function<? super E, ? extends E> rule, Class<E> typeToken) {
        return transformUp(t -> t.transformNodeProps(rule, typeToken));
    }

    @SuppressWarnings("unchecked")
    protected <E> T transformNodeProps(Function<? super E, ? extends E> rule, Class<E> typeToken) {
        Object[] props = NodeUtils.properties(this);
        boolean changed = false;

        for (int i = 0; i < props.length; i++) {
            Object prop = props[i];
            // skip children (only properties are interesting)
            if (prop != children && !children.contains(prop) && typeToken.isInstance(prop)) {
                Object transformed = rule.apply((E) prop);
                if (!prop.equals(transformed)) {
                    changed = true;
                    props[i] = transformed;
                }
            }
        }

        return changed ? NodeUtils.cloneNode(this, props) : (T) this;
    }

    @Override
    public int hashCode() {
        return Objects.hash(children);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        Node<?> other = (Node<?>) obj;
        return Objects.equals(children(), other.children());
    }

    public String nodeName() {
        return getClass().getSimpleName();
    }

    public String nodeString() {
        return NodeUtils.nodeString(this);
    }

    @Override
    public String toString() {
        return NodeUtils.toString(this);
    }
}
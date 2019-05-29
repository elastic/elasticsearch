/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.tree;

import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import static java.util.Collections.emptyList;

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
    private static final int TO_STRING_MAX_PROP = 10;
    private static final int TO_STRING_MAX_WIDTH = 110;

    private final Source source;
    private final List<T> children;

    public Node(Source source, List<T> children) {
        this.source = (source != null ? source : Source.EMPTY);
        if (children.contains(null)) {
            throw new SqlIllegalArgumentException("Null children are not allowed");
        }
        this.children = children;
    }

    public Source source() {
        return source;
    }

    public Location sourceLocation() {
        return source.source();
    }

    public String sourceText() {
        return source.text();
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
        for (Object prop : info().properties()) {
            // skip children (only properties are interesting)
            if (prop != children && !children.contains(prop) && typeToken.isInstance(prop)) {
                rule.accept((E) prop);
            }
        }
    }

    @SuppressWarnings("unchecked")
    public boolean anyMatch(Predicate<? super T> predicate) {
        boolean result = predicate.test((T) this);
        if (!result) {
            for (T child : children) {
                if (child.anyMatch(predicate)) {
                    return true;
                }
            }
        }
        return result;
    }

    public List<T> collect(Predicate<? super T> predicate) {
        List<T> l = new ArrayList<>();
        forEachDown(n -> {
            if (predicate.test(n)) {
                l.add(n);
            }
        });
        return l.isEmpty() ? emptyList() : l;
    }

    public List<T> collectLeaves() {
        return collect(n -> n.children().isEmpty());
    }

    // parse the list in pre-order and on match, skip the child/branch and move on to the next child/branch
    public List<T> collectFirstChildren(Predicate<? super T> predicate) {
        List<T> matches = new ArrayList<>();
        doCollectFirst(predicate, matches);
        return matches;
    }

    @SuppressWarnings("unchecked")
    protected void doCollectFirst(Predicate<? super T> predicate, List<T> matches) {
        T t = (T) this;
        if (predicate.test(t)) {
            matches.add(t);
        } else {
            for (T child : children()) {
                child.doCollectFirst(predicate, matches);
            }
        }
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

        return (childrenChanged ? replaceChildren(transformedChildren) : (T) this);
    }

    /**
     * Replace the children of this node.
     */
    public abstract T replaceChildren(List<T> newChildren);

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

    /**
     * Transform this node's properties.
     * <p>
     * This always returns something of the same type as the current
     * node but since {@link Node} doesn't have a {@code SelfT} parameter
     * we return the closest thing we do have: {@code T}, which is the
     * root of the hierarchy for the this node.
     */
    protected final <E> T transformNodeProps(Function<? super E, ? extends E> rule, Class<E> typeToken) {
        return info().transform(rule, typeToken);
    }

    /**
     * Return the information about this node.
     */
    protected abstract NodeInfo<? extends T> info();

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

    /**
     * The values of all the properties that are important
     * to this {@link Node}.
     */
    public List<Object> nodeProperties() {
        return info().properties();
    }

    public String nodeString() {
        StringBuilder sb = new StringBuilder();
        sb.append(nodeName());
        sb.append("[");
        sb.append(propertiesToString(true));
        sb.append("]");
        return sb.toString();
    }

    @Override
    public String toString() {
        return treeString(new StringBuilder(), 0, new BitSet()).toString();
    }

    /**
     * Render this {@link Node} as a tree like
     * <pre>
     * {@code
     * Project[[i{f}#0]]
     * \_Filter[i{f}#1]
     *   \_SubQueryAlias[test]
     *     \_EsRelation[test][i{f}#2]
     * }
     * </pre>
     */
    final StringBuilder treeString(StringBuilder sb, int depth, BitSet hasParentPerDepth) {
        if (depth > 0) {
            // draw children
            for (int column = 0; column < depth; column++) {
                if (hasParentPerDepth.get(column)) {
                    sb.append("|");
                    // if not the last elder, adding padding (since each column has two chars ("|_" or "\_")
                    if (column < depth - 1) {
                        sb.append(" ");
                    }
                }
                else {
                    // if the child has no parent (elder on the previous level), it means its the last sibling
                    sb.append((column == depth - 1) ? "\\" : "  ");
                }
            }

            sb.append("_");
        }

        sb.append(nodeString());

        List<T> children = children();
        if (!children.isEmpty()) {
            sb.append("\n");
        }
        for (int i = 0; i < children.size(); i++) {
            T t = children.get(i);
            hasParentPerDepth.set(depth, i < children.size() - 1);
            t.treeString(sb, depth + 1, hasParentPerDepth);
            if (i < children.size() - 1) {
                sb.append("\n");
            }
        }
        return sb;
    }

    /**
     * Render the properties of this {@link Node} one by
     * one like {@code foo bar baz}. These go inside the
     * {@code [} and {@code ]} of the output of {@link #treeString}.
     */
    public String propertiesToString(boolean skipIfChild) {
        StringBuilder sb = new StringBuilder();

        List<?> children = children();
        // eliminate children (they are rendered as part of the tree)
        int remainingProperties = TO_STRING_MAX_PROP;
        int maxWidth = 0;
        boolean needsComma = false;

        List<Object> props = nodeProperties();
        for (Object prop : props) {
            // consider a property if it is not ignored AND
            // it's not a child (optional)
            if (!(skipIfChild && (children.contains(prop) || children.equals(prop)))) {
                if (remainingProperties-- < 0) {
                    sb.append("...").append(props.size() - TO_STRING_MAX_PROP).append("fields not shown");
                    break;
                }

                if (needsComma) {
                    sb.append(",");
                }
                String stringValue = Objects.toString(prop);
                if (maxWidth + stringValue.length() > TO_STRING_MAX_WIDTH) {
                    int cutoff = Math.max(0, TO_STRING_MAX_WIDTH - maxWidth);
                    sb.append(stringValue.substring(0, cutoff));
                    sb.append("\n");
                    stringValue = stringValue.substring(cutoff);
                    maxWidth = 0;
                }
                maxWidth += stringValue.length();
                sb.append(stringValue);

                needsComma = true;
            }
        }

        return sb.toString();
    }
}
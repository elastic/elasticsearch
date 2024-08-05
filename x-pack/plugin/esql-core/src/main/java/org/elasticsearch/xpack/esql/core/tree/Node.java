/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.tree;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
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
public abstract class Node<T extends Node<T>> implements NamedWriteable {
    private static final int TO_STRING_MAX_PROP = 10;
    private static final int TO_STRING_MAX_WIDTH = 110;

    private final Source source;
    private final List<T> children;

    public Node(Source source, List<T> children) {
        this.source = (source != null ? source : Source.EMPTY);
        if (containsNull(children)) {
            throw new QlIllegalArgumentException("Null children are not allowed");
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
    public <E extends T> void forEachDown(Class<E> typeToken, Consumer<? super E> action) {
        forEachDown(t -> {
            if (typeToken.isInstance(t)) {
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
    public <E extends T> void forEachUp(Class<E> typeToken, Consumer<? super E> action) {
        forEachUp(t -> {
            if (typeToken.isInstance(t)) {
                action.accept((E) t);
            }
        });
    }

    public <E> void forEachPropertyOnly(Class<E> typeToken, Consumer<? super E> rule) {
        forEachProperty(typeToken, rule);
    }

    public <E> void forEachPropertyDown(Class<E> typeToken, Consumer<? super E> rule) {
        forEachDown(e -> e.forEachProperty(typeToken, rule));
    }

    public <E> void forEachPropertyUp(Class<E> typeToken, Consumer<? super E> rule) {
        forEachUp(e -> e.forEachProperty(typeToken, rule));
    }

    @SuppressWarnings("unchecked")
    protected <E> void forEachProperty(Class<E> typeToken, Consumer<? super E> rule) {
        for (Object prop : info().properties()) {
            // skip children (only properties are interesting)
            if (prop != children && children.contains(prop) == false && typeToken.isInstance(prop)) {
                rule.accept((E) prop);
            }
        }
    }

    @SuppressWarnings("unchecked")
    public boolean anyMatch(Predicate<? super T> predicate) {
        boolean result = predicate.test((T) this);
        if (result == false) {
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
    public <E extends T> T transformDown(Class<E> typeToken, Function<E, ? extends T> rule) {
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
    public <E extends T> T transformUp(Class<E> typeToken, Function<E, ? extends T> rule) {
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
            if (child.equals(next)) {
                // use the initial value
                next = child;
            } else {
                childrenChanged = true;
            }
            transformedChildren.add(next);
        }

        return (childrenChanged ? replaceChildrenSameSize(transformedChildren) : (T) this);
    }

    public final T replaceChildrenSameSize(List<T> newChildren) {
        if (newChildren.size() != children.size()) {
            throw new QlIllegalArgumentException(
                "Expected the same number of children [" + children.size() + "], but received [" + newChildren.size() + "]"
            );
        }
        return replaceChildren(newChildren);
    }

    public abstract T replaceChildren(List<T> newChildren);

    //
    // transform the node properties and use the tree only for navigation
    //

    public <E> T transformPropertiesOnly(Class<E> typeToken, Function<? super E, ? extends E> rule) {
        return transformNodeProps(typeToken, rule);
    }

    public <E> T transformPropertiesDown(Class<E> typeToken, Function<? super E, ? extends E> rule) {
        return transformDown(t -> t.transformNodeProps(typeToken, rule));
    }

    public <E> T transformPropertiesUp(Class<E> typeToken, Function<? super E, ? extends E> rule) {
        return transformUp(t -> t.transformNodeProps(typeToken, rule));
    }

    /**
     * Transform this node's properties.
     * <p>
     * This always returns something of the same type as the current
     * node but since {@link Node} doesn't have a {@code SelfT} parameter
     * we return the closest thing we do have: {@code T}, which is the
     * root of the hierarchy for this node.
     */
    protected final <E> T transformNodeProps(Class<E> typeToken, Function<? super E, ? extends E> rule) {
        return info().transform(rule, typeToken);
    }

    /**
     * Return the information about this node.
     * <p>
     * Normally, you want to use one of the static {@code create} methods to implement this.
     * <p>
     * For {@link org.elasticsearch.xpack.esql.core.plan.QueryPlan}s, it is very important that
     * the properties contain all of the expressions and references relevant to this node, and
     * that all of the properties are used in the provided constructor; otherwise query plan
     * transformations like
     * {@link org.elasticsearch.xpack.esql.core.plan.QueryPlan#transformExpressionsOnly(Function)}
     * will not have an effect.
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
                } else {
                    // if the child has no parent (elder on the previous level), it means its the last sibling
                    sb.append((column == depth - 1) ? "\\" : "  ");
                }
            }

            sb.append("_");
        }

        sb.append(nodeString());

        @SuppressWarnings("HiddenField")
        List<T> children = children();
        if (children.isEmpty() == false) {
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

        @SuppressWarnings("HiddenField")
        List<?> children = children();
        // eliminate children (they are rendered as part of the tree)
        int remainingProperties = TO_STRING_MAX_PROP;
        int maxWidth = 0;
        boolean needsComma = false;

        List<Object> props = nodeProperties();
        for (Object prop : props) {
            // consider a property if it is not ignored AND
            // it's not a child (optional)
            if ((skipIfChild && (children.contains(prop) || children.equals(prop))) == false) {
                if (remainingProperties-- < 0) {
                    sb.append("...").append(props.size() - TO_STRING_MAX_PROP).append("fields not shown");
                    break;
                }

                if (needsComma) {
                    sb.append(",");
                }

                String stringValue = toString(prop);

                // : Objects.toString(prop);
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

    private static String toString(Object obj) {
        StringBuilder sb = new StringBuilder();
        toString(sb, obj);
        return sb.toString();
    }

    private static void toString(StringBuilder sb, Object obj) {
        if (obj instanceof Iterable) {
            sb.append("[");
            for (Iterator<?> it = ((Iterable<?>) obj).iterator(); it.hasNext();) {
                Object o = it.next();
                toString(sb, o);
                if (it.hasNext()) {
                    sb.append(", ");
                }
            }
            sb.append("]");
        } else if (obj instanceof Node<?>) {
            sb.append(((Node<?>) obj).nodeString());
        } else {
            sb.append(Objects.toString(obj));
        }
    }

    private <U> boolean containsNull(List<U> us) {
        // Use custom implementation because some implementations of `List.contains` (e.g. ImmutableCollections$AbstractImmutableList) throw
        // a NPE if any of the elements is null.
        for (U u : us) {
            if (u == null) {
                return true;
            }
        }
        return false;
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.tree;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.util.Holder;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
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
    /**
     * Maximum number of properties rendered by {@link #toString}.
     */
    private static final int TO_STRING_MAX_PROP = 10;
    /**
     * Maximum number of characters per line rendered by {@link #toString}.
     */
    public static final int TO_STRING_MAX_WIDTH = 110;
    /**
     * Maximum number of lines rendered by {@link #toString}.
     */
    public static final int TO_STRING_MAX_LINES = 25;

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

    public final List<T> children() {
        return children;
    }

    @SuppressWarnings("unchecked")
    public void forEachDown(Consumer<? super T> action) {
        action.accept((T) this);
        // please do not refactor it to a for-each loop to avoid
        // allocating iterator that performs concurrent modification checks and extra stack frames
        for (int c = 0, size = children.size(); c < size; c++) {
            children.get(c).forEachDown(action);
        }
    }

    /**
     * Same as forEachDown, but can end the traverse early, by setting the boolean argument in the action.
     */
    public boolean forEachDownMayReturnEarly(BiConsumer<? super T, Holder<Boolean>> action) {
        var breakEarly = new Holder<>(false);
        forEachDownMayReturnEarly(action, breakEarly);
        return breakEarly.get();
    }

    @SuppressWarnings("unchecked")
    void forEachDownMayReturnEarly(BiConsumer<? super T, Holder<Boolean>> action, Holder<Boolean> breakEarly) {
        action.accept((T) this, breakEarly);
        if (breakEarly.get()) {
            // Early return.
            return;
        }
        // please do not refactor it to a for-each loop to avoid
        // allocating iterator that performs concurrent modification checks and extra stack frames
        for (int c = 0, size = children.size(); c < size; c++) {
            children.get(c).forEachDownMayReturnEarly(action, breakEarly);
            if (breakEarly.get()) {
                // Early return.
                return;
            }
        }
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
        // please do not refactor it to a for-each loop to avoid
        // allocating iterator that performs concurrent modification checks and extra stack frames
        for (int c = 0, size = children.size(); c < size; c++) {
            children.get(c).forEachUp(action);
        }
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
            if (prop != children && typeToken.isInstance(prop) && children.contains(prop) == false) {
                rule.accept((E) prop);
            }
        }
    }

    @SuppressWarnings("unchecked")
    public boolean anyMatch(Predicate<? super T> predicate) {
        Deque<T> stack = new ArrayDeque<>();
        stack.push((T) this);
        while (stack.isEmpty() == false) {
            T node = stack.pop();
            if (predicate.test(node)) {
                return true;
            }
            List<T> nodeChildren = node.children();
            for (int c = nodeChildren.size() - 1; c >= 0; c--) {
                stack.push(nodeChildren.get(c));
            }
        }
        return false;
    }

    /**
     * Traverse the plan tree from the current node to the leaves, checking the given
     * predicate.  This function will short circuit and return early if it is able to.
     *
     * @param predicate condition to check against all nodes
     * @return true iff the given predicate is true for all nodes
     */
    public boolean allMatch(Predicate<? super T> predicate) {
        return anyMatch(Predicate.not(predicate)) == false;
    }

    /**
     * Traverse the plan tree from the current node to the leaves, checking the given
     * predicate.  This function will short circuit and return early if it is able to.
     *
     * @param predicate condition to check against all nodes
     * @return true iff the given predicate is false for all nodes
     */
    public boolean noneMatch(Predicate<? super T> predicate) {
        return anyMatch(predicate) == false;
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

    public <E extends T> List<E> collect(Class<E> typeToken) {
        return collect(typeToken, n -> true);
    }

    public <E extends T> List<E> collect(Class<E> typeToken, Predicate<? super E> predicate) {
        List<E> l = new ArrayList<>();
        forEachDown(n -> {
            if (typeToken.isInstance(n)) {
                E e = typeToken.cast(n);
                if (predicate.test(e)) {
                    l.add(e);
                }
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

    /**
     * Stack frame used by the iterative implementations of {@link #transformDown}, {@link #transformDownSkipBranch}, and
     * {@link #transformUp}. Each frame stands in for one level of the recursion those methods used to perform on the JVM call
     * stack; keeping an explicit {@link Deque} of these on the heap instead means traversal depth is bounded only by available
     * heap, not by the (small, fixed) thread stack size — see {@link #transformDown(Function)}.
     */
    private static final class Frame<T extends Node<T>> {
        /**
         * For {@link #transformDown}/{@link #transformDownSkipBranch}: this node with the rule already applied (pre-order).
         * For {@link #transformUp}: the original, not-yet-ruled node — the rule runs only after every child below has
         * completed.
         */
        final T node;
        final List<T> children;
        /** Completed value of each child; starts as a copy of {@link #children} and is overwritten index by index. */
        final List<T> results;
        int nextChild = 0;
        boolean anyChildChanged = false;

        Frame(T node) {
            this.node = node;
            this.children = node.children();
            this.results = new ArrayList<>(children);
        }
    }

    /**
     * Records {@code completed} — the fully-transformed value of {@code frame}'s child at {@code frame.nextChild} — into
     * {@code frame}, advancing it to the next child. Shared by the iterative {@code transformDown}/{@code transformUp}
     * loops: whenever a child frame finishes, its result is merged into its parent frame this way before the parent decides
     * whether to move to its next child or, having none left, complete itself.
     */
    private static <T extends Node<T>> void mergeChildResult(Frame<T> frame, T completed) {
        int idx = frame.nextChild;
        T originalChild = frame.children.get(idx);
        // The `==` check is a pure optimization, not a behavior change: equals() must be reflexive, so
        // completed == originalChild implies completed.equals(originalChild). It matters because Node's
        // equals()/subclass overrides recurse through the whole subtree with no identity fast-path — on a
        // deep, unchanged chain (the common case, since identity is preserved whenever nothing changed) this
        // avoids an O(depth) recursive equals() call at every single level.
        if (completed != originalChild && completed.equals(originalChild) == false) {
            frame.anyChildChanged = true;
        }
        frame.results.set(idx, completed);
        frame.nextChild++;
    }

    /** {@code rule.apply(node)}, but returns {@code node} itself (not the rule's result) when the two are merely equal. */
    private static <T extends Node<T>> T applyRulePreservingIdentity(Function<? super T, ? extends T> rule, T node) {
        T result = rule.apply(node);
        return node.equals(result) ? node : result;
    }

    @SuppressWarnings("unchecked")
    public T transformDown(Function<? super T, ? extends T> rule) {
        Deque<Frame<T>> stack = new ArrayDeque<>();
        stack.push(new Frame<>(applyRulePreservingIdentity(rule, (T) this)));
        T completed = null;

        while (stack.isEmpty() == false) {
            Frame<T> frame = stack.peek();
            if (completed != null) {
                mergeChildResult(frame, completed);
                completed = null;
            }
            if (frame.nextChild < frame.children.size()) {
                stack.push(new Frame<>(applyRulePreservingIdentity(rule, frame.children.get(frame.nextChild))));
            } else {
                stack.pop();
                completed = frame.anyChildChanged ? frame.node.replaceChildrenSameSize(frame.results) : frame.node;
            }
        }
        return completed;
    }

    @SuppressWarnings("unchecked")
    public T transformDownSkipBranch(BiFunction<? super T, Holder<Boolean>, ? extends T> rule) {
        Holder<Boolean> skipBranch = new Holder<>(Boolean.FALSE);
        return transformDownSkipBranch(skipBranch, rule);
    }

    /** {@code rule.apply(node, skipBranch)} with the same identity-preservation as {@link #applyRulePreservingIdentity}. */
    private static <T extends Node<T>> T applyRuleSkipBranchPreservingIdentity(
        BiFunction<? super T, Holder<Boolean>, ? extends T> rule,
        T node,
        Holder<Boolean> skipBranch
    ) {
        T result = rule.apply(node, skipBranch);
        return node.equals(result) ? node : result;
    }

    /** If {@code skipBranch} was set, consumes (resets) it and returns {@code true} — mirrors the check in the old recursive body. */
    private static boolean consumeSkipBranch(Holder<Boolean> skipBranch) {
        if (skipBranch.get()) {
            skipBranch.set(false);
            return true;
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    T transformDownSkipBranch(Holder<Boolean> skipBranch, BiFunction<? super T, Holder<Boolean>, ? extends T> rule) {
        T root = applyRuleSkipBranchPreservingIdentity(rule, (T) this, skipBranch);
        if (consumeSkipBranch(skipBranch)) {
            return root;
        }

        Deque<Frame<T>> stack = new ArrayDeque<>();
        stack.push(new Frame<>(root));
        T completed = null;

        while (stack.isEmpty() == false) {
            Frame<T> frame = stack.peek();
            if (completed != null) {
                mergeChildResult(frame, completed);
                completed = null;
            }
            if (frame.nextChild < frame.children.size()) {
                T child = applyRuleSkipBranchPreservingIdentity(rule, frame.children.get(frame.nextChild), skipBranch);
                if (consumeSkipBranch(skipBranch)) {
                    // Do not descend into ruledChild's children — it is already the final value for this branch. Looping back
                    // to the top with `completed` set merges it into `frame` exactly as a popped child frame would.
                    completed = child;
                } else {
                    stack.push(new Frame<>(child));
                }
            } else {
                stack.pop();
                completed = frame.anyChildChanged ? frame.node.replaceChildrenSameSize(frame.results) : frame.node;
            }
        }
        return completed;
    }

    @SuppressWarnings("unchecked")
    public <E extends T> T transformDown(Class<E> typeToken, Function<E, ? extends T> rule) {
        return transformDown((t) -> (typeToken.isInstance(t) ? rule.apply((E) t) : t));
    }

    @SuppressWarnings("unchecked")
    public <E extends T> T transformDown(Predicate<Node<?>> nodePredicate, Function<E, ? extends T> rule) {
        return transformDown((t) -> (nodePredicate.test(t) ? rule.apply((E) t) : t));
    }

    /**
     * Asynchronous variant of {@link #transformDown(Function)} that allows the transformation rule to perform
     * async I/O operations (e.g., transport actions) without blocking the caller thread.
     * <p>
     * Children are transformed sequentially, not concurrently, one after another in order.
     * This method is intended for cases where async I/O is needed during transformation, not for parallel
     * processing.
     * <p>
     * Like the synchronous variant, the traversal keeps its state on the heap, so its depth is not bounded by the thread stack,
     * whether the rule completes its listener synchronously or asynchronously (on any thread).
     */
    @SuppressWarnings("unchecked")
    public void transformDown(BiConsumer<? super T, ActionListener<T>> rule, ActionListener<T> listener) {
        new AsyncTransformDown<T>(rule, listener).run((T) this);
    }

    /**
     * Iterative driver of the asynchronous {@link #transformDown(BiConsumer, ActionListener)}. It is a trampoline: {@link #run}
     * applies the rule node after node in a loop. If the rule completes its listener before returning, the loop carries on
     * inline, so synchronous completions never grow the call stack. If the rule has not completed yet, the loop returns and
     * the {@link Step} that is completed later continues the traversal from whichever thread completes it.
     */
    private static final class AsyncTransformDown<T extends Node<T>> {
        private final BiConsumer<? super T, ActionListener<T>> rule;
        private final ActionListener<T> listener;
        private final Deque<Frame<T>> stack = new ArrayDeque<>();

        AsyncTransformDown(BiConsumer<? super T, ActionListener<T>> rule, ActionListener<T> listener) {
            this.rule = rule;
            this.listener = listener;
        }

        /** Applies the rule to {@code toRule}, and keeps going until the traversal is finished, failed or waiting on the rule. */
        void run(T toRule) {
            while (toRule != null) {
                Step step = new Step(toRule);
                try {
                    rule.accept(toRule, step);
                } catch (Exception e) {
                    step.state.set(Step.ABANDONED);
                    listener.onFailure(e);
                    return;
                }
                if (step.state.compareAndSet(Step.RUNNING, Step.WAITING)) {
                    // The rule is completing asynchronously: the step resumes the traversal when it is done.
                    return;
                }
                toRule = onRuleResult(step);
            }
        }

        /**
         * Consumes the outcome of a rule application and advances the traversal.
         *
         * @return the next node to apply the rule to, or {@code null} if the traversal is finished (the listener has been notified)
         */
        private T onRuleResult(Step step) {
            try {
                if (step.failure != null) {
                    listener.onFailure(step.failure);
                    return null;
                }
                // Same identity-preservation as applyRulePreservingIdentity: keep the original node if the rule returned an equal one.
                stack.push(new Frame<>(step.input.equals(step.result) ? step.input : step.result));
                return advance();
            } catch (Exception e) {
                listener.onFailure(e);
                return null;
            }
        }

        /**
         * Pops completed frames, merging them into their parents, until a node whose rule has not been applied yet is found.
         *
         * @return that node, or {@code null} if the root completed (the listener has been notified)
         */
        private T advance() {
            T completed = null;
            while (true) {
                Frame<T> frame = stack.peek();
                if (completed != null) {
                    mergeChildResult(frame, completed);
                    completed = null;
                }
                if (frame.nextChild < frame.children.size()) {
                    return frame.children.get(frame.nextChild);
                }
                stack.pop();
                completed = frame.anyChildChanged ? frame.node.replaceChildrenSameSize(frame.results) : frame.node;
                if (stack.isEmpty()) {
                    listener.onResponse(completed);
                    return null;
                }
            }
        }

        /** One rule application. Completed at most once; whoever observes the completion second continues the traversal. */
        private final class Step implements ActionListener<T> {
            static final int RUNNING = 0;
            static final int WAITING = 1;
            static final int DONE = 2;
            static final int ABANDONED = 3;

            final T input;
            final AtomicInteger state = new AtomicInteger(RUNNING);
            // Written before the state transition that publishes it, read only after observing that transition.
            T result;
            Exception failure;

            Step(T input) {
                this.input = input;
            }

            @Override
            public void onResponse(T response) {
                this.result = response;
                complete();
            }

            @Override
            public void onFailure(Exception e) {
                this.failure = e;
                complete();
            }

            private void complete() {
                if (state.compareAndSet(RUNNING, DONE)) {
                    // Completed before the rule returned: run() carries on inline.
                    return;
                }
                if (state.compareAndSet(WAITING, DONE)) {
                    T next = onRuleResult(this);
                    if (next != null) {
                        run(next);
                    }
                }
                // Otherwise the traversal was abandoned after the rule threw, or the rule completed the listener twice: ignore.
            }
        }
    }

    @SuppressWarnings("unchecked")
    public T transformUp(Function<? super T, ? extends T> rule) {
        Deque<Frame<T>> stack = new ArrayDeque<>();
        stack.push(new Frame<>((T) this));
        T completed = null;

        while (stack.isEmpty() == false) {
            Frame<T> frame = stack.peek();
            if (completed != null) {
                mergeChildResult(frame, completed);
                completed = null;
            }
            if (frame.nextChild < frame.children.size()) {
                stack.push(new Frame<>(frame.children.get(frame.nextChild)));
            } else {
                stack.pop();
                T childReplaced = frame.anyChildChanged ? frame.node.replaceChildrenSameSize(frame.results) : frame.node;
                // No identity-preservation check on the rule's own output here — this asymmetry with transformDown is
                // intentional and matches the previous recursive implementation.
                completed = rule.apply(childReplaced);
            }
        }
        return completed;
    }

    @SuppressWarnings("unchecked")
    public <E extends T> T transformUp(Class<E> typeToken, Function<E, ? extends T> rule) {
        return transformUp((t) -> (typeToken.isInstance(t) ? rule.apply((E) t) : t));
    }

    @SuppressWarnings("unchecked")
    public <E extends T> T transformUp(Predicate<Node<?>> nodePredicate, Function<E, ? extends T> rule) {
        return transformUp((t) -> (nodePredicate.test(t) ? rule.apply((E) t) : t));
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
     * Normally, you want to use one of the static {@code create} methods to implement this.
     * <p>
     * For {@code QueryPlan}s, it is very important that
     * the properties contain all of the expressions and references relevant to this node, and
     * that all the properties are used in the provided constructor; otherwise query plan
     * transformations like
     * {@code QueryPlan#transformExpressionsOnly(Function)}
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

    /**
     * Output-shape configuration for the {@code nodeString} render pipeline. Controls truncation,
     * width, and property-count limits. Orthogonal to identifier mapping — anonymization or any
     * other identifier-substitution variant flows through a separately-passed
     * {@link NodeStringMapper}.
     */
    public enum NodeStringFormat {
        /** Bounded width / lines / property count for human-readable debug toString. The default. */
        LIMITED(TO_STRING_MAX_PROP, TO_STRING_MAX_WIDTH, TO_STRING_MAX_LINES),
        /** No truncation; renders everything. */
        FULL(Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE);

        final int maxProperties;
        final int maxWidth;
        final int maxLines;

        NodeStringFormat(int maxProperties, int maxWidth, int maxLines) {
            this.maxProperties = maxProperties;
            this.maxWidth = maxWidth;
            this.maxLines = maxLines;
        }
    }

    /**
     * Render this {@link Node} to a {@link String} with the
     * {@link NodeStringFormat#LIMITED limited} format and the identity mapper. This does not
     * include this node's {@link #children()}.
     */
    public final String nodeString() {
        StringBuilder sb = new StringBuilder();
        nodeString(sb, NodeStringFormat.LIMITED, NodeStringMapper.IDENTITY);
        return sb.toString();
    }

    /**
     * Append this {@link Node}'s string representation to {@code sb}. This
     * does not include this node's {@link #children()}.
     * @param sb     target for the string
     * @param format output-shape configuration (width / lines / property count)
     * @param mapper identifier-mapping strategy (raw via {@link NodeStringMapper#IDENTITY},
     *               anonymized via the failure-path anonymizer, etc.)
     */
    public void nodeString(StringBuilder sb, NodeStringFormat format, NodeStringMapper mapper) {
        sb.append(nodeName());
        sb.append("[");
        propertiesToString(sb, true, format, mapper);
        sb.append("]");
    }

    @Override
    public String toString() {
        return toString(NodeStringFormat.LIMITED, NodeStringMapper.IDENTITY);
    }

    public String toString(NodeStringFormat format) {
        return toString(format, NodeStringMapper.IDENTITY);
    }

    public String toString(NodeStringFormat format, NodeStringMapper mapper) {
        return new NodeToString(format, mapper).treeString(this, 0).toString();
    }

    protected void propertiesToString(StringBuilder sb, boolean skipIfChild, NodeStringFormat format, NodeStringMapper mapper) {
        new NodePropertiesToString(sb, format, mapper, this, skipIfChild).propertiesToString();
    }

    private <U> boolean containsNull(List<U> us) {
        // Use custom implementation because some implementations of `List.contains` (e.g. ImmutableCollections$AbstractImmutableList)
        // throw
        // a NPE if any of the elements is null.
        for (U u : us) {
            if (u == null) {
                return true;
            }
        }
        return false;
    }
}

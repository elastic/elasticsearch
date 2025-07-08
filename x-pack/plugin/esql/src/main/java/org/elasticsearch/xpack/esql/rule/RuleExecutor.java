/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.rule;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.esql.core.tree.Node;
import org.elasticsearch.xpack.esql.core.tree.NodeUtils;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public abstract class RuleExecutor<TreeType extends Node<TreeType>> {

    private final Logger log = LogManager.getLogger(getClass());

    public static class Limiter {
        public static final Limiter DEFAULT = new Limiter(100);
        public static final Limiter ONCE = new Limiter(1) {

            @Override
            boolean reached(int runs) {
                return runs >= 1;
            }
        };

        private final int runs;

        public Limiter(int maximumRuns) {
            this.runs = maximumRuns;
        }

        boolean reached(int numberOfRuns) {
            if (numberOfRuns >= this.runs) {
                throw new RuleExecutionException("Rule execution limit [{}] reached", numberOfRuns);
            }
            return false;
        }
    }

    public static class Batch<TreeType extends Node<TreeType>> {
        private final String name;
        private final Rule<?, TreeType>[] rules;
        private final Limiter limit;

        @SafeVarargs
        @SuppressWarnings("varargs")
        public Batch(String name, Limiter limit, Rule<?, TreeType>... rules) {
            this.name = name;
            this.limit = limit;
            this.rules = rules;
        }

        @SafeVarargs
        public Batch(String name, Rule<?, TreeType>... rules) {
            this(name, Limiter.DEFAULT, rules);
        }

        public String name() {
            return name;
        }

        public Batch<TreeType> with(Rule<?, TreeType>[] rules) {
            return new Batch<>(name, limit, rules);
        }

        public Rule<?, TreeType>[] rules() {
            return rules;
        }
    }

    private Iterable<Batch<TreeType>> batches = null;

    protected abstract Iterable<RuleExecutor.Batch<TreeType>> batches();

    public class Transformation {
        private final TreeType before, after;
        private final String name;
        private Boolean lazyHasChanged;

        Transformation(String name, TreeType before, TreeType after) {
            this.name = name;
            this.before = before;
            this.after = after;
        }

        public boolean hasChanged() {
            if (lazyHasChanged == null) {
                lazyHasChanged = before.equals(after) == false;
            }
            return lazyHasChanged;
        }

        public String name() {
            return name;
        }

        public TreeType before() {
            return before;
        }

        public TreeType after() {
            return after;
        }
    }

    public class ExecutionInfo {

        private final TreeType before, after;
        private final Map<Batch<TreeType>, List<Transformation>> transformations;

        ExecutionInfo(TreeType before, TreeType after, Map<Batch<TreeType>, List<Transformation>> transformations) {
            this.before = before;
            this.after = after;
            this.transformations = transformations;
        }

        public TreeType before() {
            return before;
        }

        public TreeType after() {
            return after;
        }

        public Map<Batch<TreeType>, List<Transformation>> transformations() {
            return transformations;
        }
    }

    protected final void execute(TreeType plan, ActionListener<TreeType> listener) {
        executeWithInfo(plan, ActionListener.wrap(executionInfo -> listener.onResponse(executionInfo.after()), listener::onFailure));
    }

    protected final void executeWithInfo(TreeType plan, ActionListener<ExecutionInfo> listener) {
        AtomicReference<TreeType> currentPlan = new AtomicReference<>(plan);

        long totalDuration = 0;
        Map<Batch<TreeType>, List<Transformation>> transformations = new LinkedHashMap<>();

        if (batches == null) {
            batches = batches();
        }

        executeBatches(plan, currentPlan, transformations, batches.iterator(), totalDuration, listener);
    }

    private void executeBatches(
        TreeType originalPlan,
        AtomicReference<TreeType> currentPlan,
        Map<Batch<TreeType>, List<Transformation>> transformations,
        java.util.Iterator<Batch<TreeType>> batchIterator,
        long totalDuration,
        ActionListener<ExecutionInfo> listener
    ) {
        if (batchIterator.hasNext() == false) {
            TreeType finalPlan = currentPlan.get();
            if (false == finalPlan.equals(originalPlan) && log.isDebugEnabled()) {
                log.debug(
                    "Tree transformation took {}\n{}",
                    TimeValue.timeValueMillis(totalDuration),
                    NodeUtils.diffString(originalPlan, finalPlan)
                );
            }
            listener.onResponse(new ExecutionInfo(originalPlan, finalPlan, transformations));
            return;
        }

        Batch<TreeType> batch = batchIterator.next();
        List<Transformation> tfs = new ArrayList<>();
        transformations.put(batch, tfs);

        long batchStart = System.currentTimeMillis();
        executeBatch(batch, currentPlan, tfs, 0, batchStart, ActionListener.wrap(batchDuration -> {
            long newTotalDuration = totalDuration + batchDuration;
            executeBatches(originalPlan, currentPlan, transformations, batchIterator, newTotalDuration, listener);
        }, listener::onFailure));
    }

    private void executeBatch(
        Batch<TreeType> batch,
        AtomicReference<TreeType> currentPlan,
        List<Transformation> tfs,
        int batchRuns,
        long batchStart,
        ActionListener<Long> listener
    ) {
        int currentBatchRuns = batchRuns + 1;

        executeRules(currentPlan, tfs, batch.rules, 0, false, ActionListener.wrap(batchHasChanged -> {
            long batchDuration = System.currentTimeMillis() - batchStart;

            if (batchHasChanged) {
                try {
                    if (batch.limit.reached(currentBatchRuns) == false) {
                        // Continue with another batch iteration - reset the batch start time
                        executeBatch(batch, currentPlan, tfs, currentBatchRuns, System.currentTimeMillis(), listener);
                        return;
                    }
                } catch (RuleExecutionException e) {
                    listener.onFailure(e);
                    return;
                }
            }

            // Batch is complete
            if (log.isTraceEnabled()) {
                TreeType before = currentPlan.get();
                TreeType after = currentPlan.get();
                if (tfs.isEmpty() == false) {
                    before = tfs.get(0).before;
                    after = tfs.get(tfs.size() - 1).after;
                }
                log.trace(
                    "Batch {} applied took {}\n{}",
                    batch.name,
                    TimeValue.timeValueMillis(batchDuration),
                    NodeUtils.diffString(before, after)
                );
            }
            listener.onResponse(batchDuration);
        }, listener::onFailure));
    }

    private void executeRules(
        AtomicReference<TreeType> currentPlan,
        List<Transformation> tfs,
        Rule<?, TreeType>[] rules,
        int ruleIndex,
        boolean hasChanged,
        ActionListener<Boolean> listener
    ) {
        if (ruleIndex >= rules.length) {
            listener.onResponse(hasChanged);
            return;
        }

        Rule<?, TreeType> rule = rules[ruleIndex];
        if (log.isTraceEnabled()) {
            log.trace("About to apply rule {}", rule);
        }

        TreeType planBeforeRule = currentPlan.get();
        applyRule(rule, planBeforeRule, listener.delegateFailureAndWrap((l, transformedPlan) -> {
            Transformation tf = new Transformation(rule.name(), planBeforeRule, transformedPlan);
            tfs.add(tf);
            currentPlan.set(tf.after());

            boolean ruleHasChanged = tf.hasChanged();
            if (ruleHasChanged) {
                if (log.isTraceEnabled()) {
                    log.trace("Rule {} applied\n{}", rule, NodeUtils.diffString(tf.before(), tf.after()));
                }
            } else {
                if (log.isTraceEnabled()) {
                    log.trace("Rule {} applied w/o changes", rule);
                }
            }

            executeRules(currentPlan, tfs, rules, ruleIndex + 1, hasChanged || ruleHasChanged, l);
        }));
    }

    protected void applyRule(Rule<?, TreeType> rule, TreeType plan, ActionListener<TreeType> listener) {
        try {
            rule.apply(plan, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.rule;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.esql.core.tree.Node;
import org.elasticsearch.xpack.esql.core.tree.NodeUtils;

import java.util.Objects;
import java.util.function.Function;

public abstract class RuleExecutor<TreeType extends Node<TreeType>> {

    private final Logger log = LogManager.getLogger(getClass());
    /**
     * Sub-logger intended to show only changes made by the rules. Intended for debugging.
     *
     * Enable it like this for the respective optimizers and the analyzer, resp. all inheritors of this class:
     * <pre>{@code
     * PUT localhost:9200/_cluster/settings
     *
     * {
     *   "transient" : {
     *     "logger.org.elasticsearch.xpack.esql.analysis.Analyzer.changes": "TRACE",
     *     "logger.org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer.changes": "TRACE",
     *     "logger.org.elasticsearch.xpack.esql.optimizer.LocalLogicalPlanOptimizer.changes": "TRACE",
     *     "logger.org.elasticsearch.xpack.esql.optimizer.PhysicalPlanOptimizer.changes": "TRACE",
     *     "logger.org.elasticsearch.xpack.esql.optimizer.LocalPhysicalPlanOptimizer.changes": "TRACE"
     *   }
     * }
     * }</pre>
     */
    private final Logger changeLog = LogManager.getLogger(getClass().getName() + ".changes");

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

    protected final TreeType execute(TreeType plan) {
        TreeType currentPlan = plan;

        long totalDuration = 0;

        if (batches == null) {
            batches = batches();
        }

        for (Batch<TreeType> batch : batches) {
            int batchRuns = 0;

            boolean hasChanged = false;
            long batchStart = System.currentTimeMillis();
            long batchDuration = 0;

            TreeType before = currentPlan;
            // run each batch until no change occurs or the limit is reached
            do {
                hasChanged = false;
                batchRuns++;

                for (Rule<?, TreeType> rule : batch.rules) {
                    if (log.isTraceEnabled()) {
                        log.trace("About to apply rule {}", rule);
                    }

                    TreeType beforeRule = currentPlan;
                    TreeType afterRule = transform(rule).apply(currentPlan);
                    currentPlan = afterRule;

                    if (Objects.equals(beforeRule, afterRule) == false) {
                        hasChanged = true;
                        if (changeLog.isTraceEnabled()) {
                            changeLog.trace("Rule {} applied with change\n{}", rule, NodeUtils.diffString(beforeRule, afterRule));
                        }
                    } else {
                        if (log.isTraceEnabled()) {
                            log.trace("Rule {} applied w/o changes", rule);
                        }
                    }
                }
                batchDuration = System.currentTimeMillis() - batchStart;
            } while (hasChanged && batch.limit.reached(batchRuns) == false);
            TreeType after = currentPlan;

            totalDuration += batchDuration;

            if (log.isTraceEnabled()) {
                log.trace(
                    "Batch {} applied took {}\n{}",
                    batch.name,
                    TimeValue.timeValueMillis(batchDuration),
                    NodeUtils.diffString(before, after)
                );
            }
        }

        if (false == currentPlan.equals(plan) && log.isDebugEnabled()) {
            log.debug("Tree transformation took {}\n{}", TimeValue.timeValueMillis(totalDuration), NodeUtils.diffString(plan, currentPlan));
        }

        return currentPlan;
    }

    protected Function<TreeType, TreeType> transform(Rule<?, TreeType> rule) {
        return rule::apply;
    }
}

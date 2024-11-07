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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

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

        Transformation(String name, TreeType plan, Function<TreeType, TreeType> transform) {
            this.name = name;
            this.before = plan;
            this.after = transform.apply(before);
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

    protected final TreeType execute(TreeType plan) {
        return executeWithInfo(plan).after;
    }

    protected final ExecutionInfo executeWithInfo(TreeType plan) {
        TreeType currentPlan = plan;

        long totalDuration = 0;

        Map<Batch<TreeType>, List<Transformation>> transformations = new LinkedHashMap<>();
        if (batches == null) {
            batches = batches();
        }

        for (Batch<TreeType> batch : batches) {
            int batchRuns = 0;
            List<Transformation> tfs = new ArrayList<>();
            transformations.put(batch, tfs);

            boolean hasChanged = false;
            long batchStart = System.currentTimeMillis();
            long batchDuration = 0;

            // run each batch until no change occurs or the limit is reached
            do {
                hasChanged = false;
                batchRuns++;

                for (Rule<?, TreeType> rule : batch.rules) {
                    if (log.isTraceEnabled()) {
                        log.trace("About to apply rule {}", rule);
                    }
                    Transformation tf = new Transformation(rule.name(), currentPlan, transform(rule));
                    tfs.add(tf);
                    currentPlan = tf.after;

                    if (tf.hasChanged()) {
                        hasChanged = true;
                        if (log.isTraceEnabled()) {
                            log.trace("Rule {} applied\n{}", rule, NodeUtils.diffString(tf.before, tf.after));
                        }
                    } else {
                        if (log.isTraceEnabled()) {
                            log.trace("Rule {} applied w/o changes", rule);
                        }
                    }
                }
                batchDuration = System.currentTimeMillis() - batchStart;
            } while (hasChanged && batch.limit.reached(batchRuns) == false);

            totalDuration += batchDuration;

            if (log.isTraceEnabled()) {
                TreeType before = plan;
                TreeType after = plan;
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
        }

        if (false == currentPlan.equals(plan) && log.isDebugEnabled()) {
            log.debug("Tree transformation took {}\n{}", TimeValue.timeValueMillis(totalDuration), NodeUtils.diffString(plan, currentPlan));
        }

        return new ExecutionInfo(plan, currentPlan, transformations);
    }

    protected Function<TreeType, TreeType> transform(Rule<?, TreeType> rule) {
        return rule::apply;
    }
}

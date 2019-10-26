/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.rule;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.sql.tree.Node;
import org.elasticsearch.xpack.sql.tree.NodeUtils;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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

        boolean reached(int runs) {
            if (runs >= this.runs) {
                throw new RuleExecutionException("Rule execution limit [{}] reached", runs);
            }
            return false;
        }
    }

    public class Batch {
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
    }

    private final Iterable<Batch> batches = batches();

    protected abstract Iterable<RuleExecutor<TreeType>.Batch> batches();

    public class Transformation {
        private final TreeType before, after;
        private final Rule<?, TreeType> rule;
        private Boolean lazyHasChanged;

        Transformation(TreeType plan, Rule<?, TreeType> rule) {
            this.rule = rule;
            before = plan;
            after = rule.apply(before);
        }

        public boolean hasChanged() {
            if (lazyHasChanged == null) {
                lazyHasChanged = !before.equals(after);
            }
            return lazyHasChanged;
        }

        public String ruleName() {
            return rule.name();
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
        private final Map<Batch, List<Transformation>> transformations;

        ExecutionInfo(TreeType before, TreeType after, Map<Batch, List<Transformation>> transformations) {
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

        public Map<Batch, List<Transformation>> transformations() {
            return transformations;
        }
    }

    protected TreeType execute(TreeType plan) {
        return executeWithInfo(plan).after;
    }

    protected ExecutionInfo executeWithInfo(TreeType plan) {
        TreeType currentPlan = plan;

        long totalDuration = 0;

        Map<Batch, List<Transformation>> transformations = new LinkedHashMap<>();

        for (Batch batch : batches) {
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
                    Transformation tf = new Transformation(currentPlan, rule);
                    tfs.add(tf);
                    currentPlan = tf.after;

                    if (tf.hasChanged()) {
                        hasChanged = true;
                        if (log.isTraceEnabled()) {
                            log.trace("Rule {} applied\n{}", rule, NodeUtils.diffString(tf.before, tf.after));
                        }
                    }
                    else {
                        if (log.isTraceEnabled()) {
                            log.trace("Rule {} applied w/o changes", rule);
                        }
                    }
                }
                batchDuration = System.currentTimeMillis() - batchStart;
            } while (hasChanged && !batch.limit.reached(batchRuns));

            totalDuration += batchDuration;

            if (log.isTraceEnabled()) {
                TreeType before = plan;
                TreeType after = plan;
                if (!tfs.isEmpty()) {
                    before = tfs.get(0).before;
                    after = tfs.get(tfs.size() - 1).after;
                }
                log.trace("Batch {} applied took {}\n{}",
                    batch.name, TimeValue.timeValueMillis(batchDuration), NodeUtils.diffString(before, after));
            }
        }

        if (false == currentPlan.equals(plan) && log.isDebugEnabled()) {
            log.debug("Tree transformation took {}\n{}",
                TimeValue.timeValueMillis(totalDuration), NodeUtils.diffString(plan, currentPlan));
        }

        return new ExecutionInfo(plan, currentPlan, transformations);
    }
}

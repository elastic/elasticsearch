/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.rule;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.esql.core.tree.Node;

public abstract class ParameterizedRuleExecutor<TreeType extends Node<TreeType>, Context> extends RuleExecutor<TreeType> {

    private final Context context;

    protected ParameterizedRuleExecutor(Context context) {
        this.context = context;
    }

    protected Context context() {
        return context;
    }

    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    protected void applyRule(Rule<?, TreeType> rule, TreeType plan, ActionListener<TreeType> listener) {
        if (rule instanceof ParameterizedRule pr) {
            pr.apply(plan, context, listener);
        } else {
            rule.apply(plan, listener);
        }
    }
}

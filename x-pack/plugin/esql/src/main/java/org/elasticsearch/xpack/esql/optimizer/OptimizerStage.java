/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.Build;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Identifies the six optimizer stages that ES|QL runs a query through. Used together with the
 * {@code disable_optimizer_rules} {@link org.elasticsearch.xpack.esql.plugin.QueryPragmas pragma} to scope which
 * stage a disabled rule applies to.
 *
 * <p>Pragma entries take the form {@code "<pragmaKey>:<RuleSimpleName>"} to disable a rule in one stage, or a bare
 * {@code "<RuleSimpleName>"} to disable it in every stage.</p>
 */
public enum OptimizerStage {
    GLOBAL_LOGICAL("logical"),
    LOCAL_LOGICAL("local_logical"),
    GLOBAL_PHYSICAL("physical"),
    LOCAL_PHYSICAL("local_physical"),
    LOOKUP_LOGICAL("lookup_logical"),
    LOOKUP_PHYSICAL("lookup_physical");

    private final String pragmaKey;

    OptimizerStage(String pragmaKey) {
        this.pragmaKey = pragmaKey;
    }

    /** Returns the stable string key used in pragma entries, e.g. {@code "local_logical"}. */
    public String pragmaKey() {
        return pragmaKey;
    }

    /**
     * Returns the set of optimizer rule simple names that should be skipped in {@code stage}, derived from the
     * request's {@link org.elasticsearch.xpack.esql.plugin.QueryPragmas#disableOptimizerRules()} list.
     *
     * <p>This method is a no-op (returns an empty set) on release builds; the disable mechanism is snapshot-only so
     * that diagnostic levers never ship in production releases.</p>
     *
     * <p>Accepts two pragma entry formats:</p>
     * <ul>
     *   <li>Bare {@code "RuleName"} — disables the rule in every stage.</li>
     *   <li>Stage-scoped {@code "stage-key:RuleName"} — disables the rule only in the named stage,
     *       for example {@code "local_logical:InferIsNotNull"}.</li>
     * </ul>
     *
     * <p><strong>Note on name matching:</strong> names are matched against {@link Class#getSimpleName()}. If a rule
     * is replaced by a
     * {@link org.elasticsearch.xpack.esql.optimizer.rules.logical.OptimizerRules.LocalAware#local() LocalAware.local()}
     * variant in a local optimizer stage, the variant's own class name is what the pragma must specify, not the global
     * rule's class name.</p>
     */
    public static Set<String> disabledRuleNames(Configuration configuration, OptimizerStage stage) {
        if (Build.current().isSnapshot() == false) {
            return Set.of();
        }
        List<String> entries = configuration.pragmas().disableOptimizerRules();
        if (entries.isEmpty()) {
            return Set.of();
        }
        Set<String> names = new HashSet<>();
        for (String entry : entries) {
            int colon = entry.indexOf(':');
            if (colon < 0) {
                // bare name → all stages
                names.add(entry);
            } else if (entry.substring(0, colon).equals(stage.pragmaKey)) {
                // stage-scoped → this stage only
                names.add(entry.substring(colon + 1));
            }
        }
        return names;
    }
}

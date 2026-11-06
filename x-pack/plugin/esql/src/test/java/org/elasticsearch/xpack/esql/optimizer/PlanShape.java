/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.CompoundOutputEvalExec;
import org.elasticsearch.xpack.esql.plan.physical.DistributedHashJoinExec;
import org.elasticsearch.xpack.esql.plan.physical.EnrichExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.LocalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.MergeExec;
import org.elasticsearch.xpack.esql.plan.physical.MvExpandExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.RegexExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.SampleExec;
import org.elasticsearch.xpack.esql.plan.physical.ShuffleExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;

import java.util.List;
import java.util.Locale;
import java.util.StringJoiner;

/** Renders physical plans by the data movement and operators that determine their execution shape. */
public final class PlanShape {
    private PlanShape() {}

    /** Returns a stable rendering of the data-movement shape of a physical plan. */
    public static String of(PhysicalPlan plan) {
        StringBuilder result = new StringBuilder();
        append(plan, 0, result);
        return result.toString();
    }

    private static void append(PhysicalPlan plan, int depth, StringBuilder result) {
        if (isTransparent(plan) && plan.children().size() == 1) {
            append(plan.children().getFirst(), depth, result);
            return;
        }

        result.append("  ".repeat(depth)).append(label(plan)).append('\n');
        for (PhysicalPlan child : plan.children()) {
            append(child, depth + 1, result);
        }
    }

    private static boolean isTransparent(PhysicalPlan plan) {
        return plan instanceof FilterExec
            || plan instanceof ProjectExec
            || plan instanceof EvalExec
            || plan instanceof MvExpandExec
            || plan instanceof RegexExtractExec
            || plan instanceof CompoundOutputEvalExec
            || plan instanceof EnrichExec
            || plan instanceof SampleExec;
    }

    private static String label(PhysicalPlan plan) {
        if (plan instanceof ShuffleExchangeExec exchange) {
            String keys = names(exchange.keys());
            String suffix = exchange.inBetweenAggs() ? "*" : "";
            return switch (exchange.distribution()) {
                case HASH -> "HASH" + suffix + " [" + keys + "]";
                case GATHER -> "GATHER" + suffix;
                case BROADCAST -> "BROADCAST";
            };
        }
        if (plan instanceof ExchangeExec exchange) {
            return "EXCHANGE" + (exchange.inBetweenAggs() ? "*" : "");
        }
        if (plan instanceof DistributedHashJoinExec join) {
            String suffix = join.mode() == DistributedHashJoinExec.Mode.BROADCAST ? " BROADCAST" : "";
            return "DHJ [" + names(join.leftFields()) + "] = [" + names(join.rightFields()) + "]" + suffix;
        }
        if (plan instanceof AggregateExec aggregate) {
            StringJoiner groups = new StringJoiner(", ");
            for (Expression grouping : aggregate.groupings()) {
                groups.add(grouping instanceof Attribute attribute ? attribute.name() : grouping.toString());
            }
            return "AGG " + aggregate.getMode() + " [" + groups + "]";
        }
        if (plan instanceof LimitExec limit) {
            return "LIMIT " + limitText(limit.limit().sourceText());
        }
        if (plan instanceof TopNExec topN) {
            return "TOPN " + limitText(topN.limit().sourceText());
        }
        if (plan instanceof MergeExec merge) {
            return merge.kind().name();
        }
        if (plan instanceof FragmentExec fragment) {
            List<String> indices = fragment.fragment().collect(EsRelation.class).stream().map(EsRelation::indexPattern).toList();
            if (indices.isEmpty() && fragment.fragment().anyMatch(ExternalRelation.class::isInstance)) {
                return "FRAGMENT EXTERNAL";
            }
            return "FRAGMENT " + String.join(", ", indices);
        }
        if (plan instanceof ExternalSourceExec) {
            return "EXTERNAL";
        }
        if (plan instanceof LocalSourceExec) {
            return "LOCAL";
        }
        String name = plan.getClass().getSimpleName();
        if (name.endsWith("Exec")) {
            name = name.substring(0, name.length() - "Exec".length());
        }
        return name.toUpperCase(Locale.ROOT);
    }

    private static String names(List<Attribute> attributes) {
        StringJoiner names = new StringJoiner(", ");
        attributes.forEach(attribute -> names.add(attribute.name()));
        return names.toString();
    }

    private static String limitText(String sourceText) {
        return sourceText.startsWith("LIMIT ") ? sourceText.substring("LIMIT ".length()) : sourceText;
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.promql;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.promql.AcrossSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;
import org.elasticsearch.xpack.esql.plan.logical.promql.WithinSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.Selector;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * A fake resolver that simulates resolving PromQL queries by creating an EsRelation
 * with fields based on the labels and metrics found in the PromQL logical plan.
 * This is only used for testing.
 */
public class PromqlFakeResolver extends Rule<LogicalPlan, LogicalPlan> {

    /**
     * PromQL functions that translate to ES|QL functions that only accept counter metrics.
     */
    private static final Set<String> COUNTER_FUNCTIONS = Set.of("rate", "irate", "increase", "deriv", "idelta", "histogram_quantile");
    public static final FieldAttribute TIMESTAMP = new FieldAttribute(
        Source.EMPTY,
        null,
        null,
        "@timestamp",
        new EsField("@timestamp", DataType.DATETIME, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
    );

    @Override
    public LogicalPlan apply(LogicalPlan logicalPlan) {
        List<PromqlCommand> promqlCommand = logicalPlan.collect(PromqlCommand.class);
        if (promqlCommand.isEmpty()) {
            return logicalPlan;
        }

        return resolve(logicalPlan, promqlCommand.getFirst());
    }

    public LogicalPlan resolve(LogicalPlan logicalPlan, PromqlCommand promqlCommand) {
        Set<String> labels = new HashSet<>();
        Set<String> gauges = new HashSet<>();
        Set<String> counters = new HashSet<>();
        collectLabelsAndMetrics(promqlCommand.promqlPlan(), labels, gauges, counters);
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(TIMESTAMP);
        for (String label : labels) {
            attributes.add(
                new FieldAttribute(
                    Source.EMPTY,
                    null,
                    null,
                    label,
                    new EsField(label, DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.DIMENSION)
                )
            );
        }
        for (String gauge : gauges) {
            attributes.add(
                new FieldAttribute(
                    Source.EMPTY,
                    null,
                    null,
                    gauge,
                    new EsField(gauge, DataType.DOUBLE, Map.of(), true, EsField.TimeSeriesFieldType.METRIC)
                )
            );
        }
        for (String counter : counters) {
            attributes.add(
                new FieldAttribute(
                    Source.EMPTY,
                    null,
                    null,
                    counter,
                    new EsField(counter, DataType.COUNTER_LONG, Map.of(), true, EsField.TimeSeriesFieldType.METRIC)
                )
            );
        }
        return logicalPlan.transformUp(
            UnresolvedRelation.class,
            ur -> new EsRelation(
                Source.EMPTY,
                "promql_data",
                IndexMode.TIME_SERIES,
                Map.of(),
                Map.of(),
                Map.of("promql_data", IndexMode.TIME_SERIES),
                attributes
            )
        );
    }

    public void collectLabelsAndMetrics(LogicalPlan plan, Set<String> labels, Set<String> metrics, Set<String> counters) {
        plan.transformDownSkipBranch((lp, skipBranch) -> {
            switch (lp) {
                case Selector selector -> {
                    Optional.ofNullable(selector.series()).ifPresent(series -> metrics.add(series.sourceText()));
                    selector.labels().forEach(label -> labels.add(label.sourceText()));
                }
                case WithinSeriesAggregate within when COUNTER_FUNCTIONS.contains(within.functionName()) -> {
                    skipBranch.set(Boolean.TRUE);
                    collectLabelsAndMetrics(within.child(), labels, counters, counters);
                }
                case AcrossSeriesAggregate across -> across.groupings().stream().map(Expression::sourceText).forEach(labels::add);
                default -> {
                }
            }
            return lp;
        });
    }
}

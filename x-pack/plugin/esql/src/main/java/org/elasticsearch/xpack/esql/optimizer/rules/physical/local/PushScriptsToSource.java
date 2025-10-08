/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.script.Script;
import org.elasticsearch.search.sort.ScriptSortBuilder;
import org.elasticsearch.search.sort.ScriptSortBuilder.ScriptSortType;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PushScriptsToSource extends PhysicalOptimizerRules.ParameterizedOptimizerRule<EvalExec, LocalPhysicalOptimizerContext> {

    @Override
    protected PhysicalPlan rule(EvalExec evalExec, LocalPhysicalOptimizerContext ctx) {
        PhysicalPlan plan = evalExec;
        if (evalExec.child() instanceof EsQueryExec esQueryExec && esQueryExec.canPushSorts()) {
            List<Alias> aliases = evalExec.fields();
            boolean aliasPushed = false;
            List<Alias> nonPushedAliases = new ArrayList<>();
            EvalExec scriptEval = null;
            for (Alias alias : aliases) {
                if (aliasPushed == false && alias.child().pushableOptions() == Expression.PushableOptions.PREFERRED) {
                    if (esQueryExec.sorts() != null
                        && esQueryExec.sorts()
                            .stream()
                            .anyMatch(s -> s instanceof ScriptSort ss && ss.field().name().equals(alias.name() + "_script"))) {
                        // Already added as a sort, skip
                        nonPushedAliases.add(alias);
                        continue;
                    }

                    // Create a script from the expression
                    String scriptText = alias.child().asScript();
                    Script script = new Script(scriptText);

                    // Create an EsField for the computed value
                    DataType dataType = alias.child().dataType();
                    EsField esField = new EsField(
                        alias.name(),
                        dataType,
                        Map.of(),
                        false,
                        EsField.TimeSeriesFieldType.NONE
                    );

                    // Create a FieldAttribute for the computed value
                    FieldAttribute fieldAttr = new FieldAttribute(
                        alias.source(),
                        alias.name() + "_script",
                        esField
                    );

                    // Create a script sort that computes the value
                    // We use NUMBER type for numeric results - adjust based on actual type if needed
                    ScriptSortType sortType = alias.child().dataType().isNumeric()
                        ? ScriptSortType.NUMBER
                        : ScriptSortType.STRING;

                    ScriptSort scriptSort = new ScriptSort(
                        fieldAttr,
                        script,
                        sortType,
                        Order.OrderDirection.ASC  // Direction doesn't matter, we just want the value
                    );

                    // Add the sort to the query exec
                    List<EsQueryExec.Sort> newSorts = new ArrayList<>();
                    newSorts.add(scriptSort);
                    EsQueryExec scriptQueryExec = esQueryExec.withSorts(newSorts);
                    // TODO Keep previous sorts

                    // Add the field attribute to attrs so it's available in the output
                    scriptQueryExec.attrs().add(fieldAttr);

                    // Create an alias that references the field
                    Alias fieldAlias = alias.replaceChild(fieldAttr);

                    scriptEval = new EvalExec(evalExec.source(), scriptQueryExec, List.of(fieldAlias));
                    aliasPushed = true;
                } else {
                    nonPushedAliases.add(alias);
                }
            }
            if (aliasPushed) {
                if (nonPushedAliases.isEmpty()) {
                    plan = scriptEval;
                } else {
                    plan = new EvalExec(evalExec.source(), scriptEval, nonPushedAliases);
                }
            }
        }

        return plan;
    }

    /**
     * Custom Sort implementation that uses a script to compute the sort value.
     * The sort value is made available through a FieldAttribute.
     */
    static class ScriptSort implements EsQueryExec.Sort {
        private final FieldAttribute field;
        private final Script script;
        private final ScriptSortType type;
        private final Order.OrderDirection direction;

        ScriptSort(FieldAttribute field, Script script, ScriptSortType type, Order.OrderDirection direction) {
            this.field = field;
            this.script = script;
            this.type = type;
            this.direction = direction;
        }

        @Override
        public SortBuilder<?> sortBuilder() {
            ScriptSortBuilder builder = new ScriptSortBuilder(script, type);
            builder.order(direction == Order.OrderDirection.ASC ? SortOrder.ASC : SortOrder.DESC);
            return builder;
        }

        @Override
        public Order.OrderDirection direction() {
            return direction;
        }

        @Override
        public FieldAttribute field() {
            return field;
        }

        @Override
        public DataType resulType() {
            return field.dataType();
        }
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Min;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EsStarTreeQueryExec;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class StarTreeEligibilityCheckerTests extends ESTestCase {

    private static final String STAR_TREE_NAME = "default";
    private static final Set<String> GROUPING_FIELDS = Set.of("country", "browser", "timestamp");
    private static final Set<String> VALUES = Set.of("bytes", "latency");

    public void testSimpleAggregationWithNoGroupBy() {
        // STATS sum(bytes)
        FieldAttribute bytesField = createFieldAttribute("bytes", DataType.LONG);
        Sum sumExpr = new Sum(Source.EMPTY, bytesField);
        Alias sumAlias = new Alias(Source.EMPTY, "sum_bytes", sumExpr);

        List<Expression> groupings = new ArrayList<>();
        List<NamedExpression> aggregates = new ArrayList<>();
        aggregates.add(sumAlias);

        AggregateExec aggregate = createAggregateExec(groupings, aggregates);

        StarTreeEligibilityChecker.EligibilityResult result = StarTreeEligibilityChecker.checkEligibility(
            aggregate,
            GROUPING_FIELDS,
            VALUES,
            STAR_TREE_NAME,
            null // context not used for basic checks
        );

        assertThat(result.eligible(), is(true));
        assertThat(result.groupByFields().size(), equalTo(0));
        assertThat(result.aggregations().size(), equalTo(1));
        assertThat(result.aggregations().get(0).valueField(), equalTo("bytes"));
        assertThat(result.aggregations().get(0).type(), equalTo(EsStarTreeQueryExec.StarTreeAggType.SUM));
    }

    public void testAggregationWithDimensionGroupBy() {
        // STATS sum(bytes) BY country
        FieldAttribute bytesField = createFieldAttribute("bytes", DataType.LONG);
        FieldAttribute countryField = createFieldAttribute("country", DataType.KEYWORD);

        Sum sumExpr = new Sum(Source.EMPTY, bytesField);
        Alias sumAlias = new Alias(Source.EMPTY, "sum_bytes", sumExpr);

        List<Expression> groupings = new ArrayList<>();
        groupings.add(countryField);
        List<NamedExpression> aggregates = new ArrayList<>();
        aggregates.add(countryField);
        aggregates.add(sumAlias);

        AggregateExec aggregate = createAggregateExec(groupings, aggregates);

        StarTreeEligibilityChecker.EligibilityResult result = StarTreeEligibilityChecker.checkEligibility(
            aggregate,
            GROUPING_FIELDS,
            VALUES,
            STAR_TREE_NAME,
            null
        );

        assertThat(result.eligible(), is(true));
        assertThat(result.groupByFields().size(), equalTo(1));
        assertThat(result.groupByFields().get(0), equalTo("country"));
        assertThat(result.aggregations().size(), equalTo(1));
    }

    public void testAggregationWithNonDimensionGroupBy() {
        // STATS sum(bytes) BY non_dimension_field
        FieldAttribute bytesField = createFieldAttribute("bytes", DataType.LONG);
        FieldAttribute nonDimensionField = createFieldAttribute("other_field", DataType.KEYWORD);

        Sum sumExpr = new Sum(Source.EMPTY, bytesField);
        Alias sumAlias = new Alias(Source.EMPTY, "sum_bytes", sumExpr);

        List<Expression> groupings = new ArrayList<>();
        groupings.add(nonDimensionField);
        List<NamedExpression> aggregates = new ArrayList<>();
        aggregates.add(nonDimensionField);
        aggregates.add(sumAlias);

        AggregateExec aggregate = createAggregateExec(groupings, aggregates);

        StarTreeEligibilityChecker.EligibilityResult result = StarTreeEligibilityChecker.checkEligibility(
            aggregate,
            GROUPING_FIELDS,
            VALUES,
            STAR_TREE_NAME,
            null
        );

        assertThat(result.eligible(), is(false));
    }

    public void testAggregationWithNonMetricField() {
        // STATS sum(non_metric_field)
        FieldAttribute nonMetricField = createFieldAttribute("other_field", DataType.LONG);

        Sum sumExpr = new Sum(Source.EMPTY, nonMetricField);
        Alias sumAlias = new Alias(Source.EMPTY, "sum_other", sumExpr);

        List<Expression> groupings = new ArrayList<>();
        List<NamedExpression> aggregates = new ArrayList<>();
        aggregates.add(sumAlias);

        AggregateExec aggregate = createAggregateExec(groupings, aggregates);

        StarTreeEligibilityChecker.EligibilityResult result = StarTreeEligibilityChecker.checkEligibility(
            aggregate,
            GROUPING_FIELDS,
            VALUES,
            STAR_TREE_NAME,
            null
        );

        assertThat(result.eligible(), is(false));
    }

    public void testMultipleAggregations() {
        // STATS sum(bytes), count(bytes), min(latency), max(latency) BY country
        FieldAttribute bytesField = createFieldAttribute("bytes", DataType.LONG);
        FieldAttribute latencyField = createFieldAttribute("latency", DataType.LONG);
        FieldAttribute countryField = createFieldAttribute("country", DataType.KEYWORD);

        Sum sumExpr = new Sum(Source.EMPTY, bytesField);
        Count countExpr = new Count(Source.EMPTY, bytesField);
        Min minExpr = new Min(Source.EMPTY, latencyField);
        Max maxExpr = new Max(Source.EMPTY, latencyField);

        Alias sumAlias = new Alias(Source.EMPTY, "sum_bytes", sumExpr);
        Alias countAlias = new Alias(Source.EMPTY, "count_bytes", countExpr);
        Alias minAlias = new Alias(Source.EMPTY, "min_latency", minExpr);
        Alias maxAlias = new Alias(Source.EMPTY, "max_latency", maxExpr);

        List<Expression> groupings = new ArrayList<>();
        groupings.add(countryField);
        List<NamedExpression> aggregates = new ArrayList<>();
        aggregates.add(countryField);
        aggregates.add(sumAlias);
        aggregates.add(countAlias);
        aggregates.add(minAlias);
        aggregates.add(maxAlias);

        AggregateExec aggregate = createAggregateExec(groupings, aggregates);

        StarTreeEligibilityChecker.EligibilityResult result = StarTreeEligibilityChecker.checkEligibility(
            aggregate,
            GROUPING_FIELDS,
            VALUES,
            STAR_TREE_NAME,
            null
        );

        assertThat(result.eligible(), is(true));
        assertThat(result.aggregations().size(), equalTo(4));
    }

    public void testMultipleDimensionGroupBy() {
        // STATS sum(bytes) BY country, browser
        FieldAttribute bytesField = createFieldAttribute("bytes", DataType.LONG);
        FieldAttribute countryField = createFieldAttribute("country", DataType.KEYWORD);
        FieldAttribute browserField = createFieldAttribute("browser", DataType.KEYWORD);

        Sum sumExpr = new Sum(Source.EMPTY, bytesField);
        Alias sumAlias = new Alias(Source.EMPTY, "sum_bytes", sumExpr);

        List<Expression> groupings = new ArrayList<>();
        groupings.add(countryField);
        groupings.add(browserField);
        List<NamedExpression> aggregates = new ArrayList<>();
        aggregates.add(countryField);
        aggregates.add(browserField);
        aggregates.add(sumAlias);

        AggregateExec aggregate = createAggregateExec(groupings, aggregates);

        StarTreeEligibilityChecker.EligibilityResult result = StarTreeEligibilityChecker.checkEligibility(
            aggregate,
            GROUPING_FIELDS,
            VALUES,
            STAR_TREE_NAME,
            null
        );

        assertThat(result.eligible(), is(true));
        assertThat(result.groupByFields().size(), equalTo(2));
        assertThat(result.groupByFields(), org.hamcrest.Matchers.containsInAnyOrder("country", "browser"));
    }

    private FieldAttribute createFieldAttribute(String name, DataType dataType) {
        EsField field = new EsField(name, dataType, Map.of(), false, EsField.TimeSeriesFieldType.NONE);
        return new FieldAttribute(Source.EMPTY, name, field);
    }

    private AggregateExec createAggregateExec(List<Expression> groupings, List<NamedExpression> aggregates) {
        // Create a dummy child plan (EsQueryExec)
        EsQueryExec child = new EsQueryExec(Source.EMPTY, "test-index", IndexMode.STANDARD, List.of(), null, List.of(), 100, List.of());

        return new AggregateExec(Source.EMPTY, child, groupings, aggregates, AggregatorMode.SINGLE, List.of(), null);
    }
}

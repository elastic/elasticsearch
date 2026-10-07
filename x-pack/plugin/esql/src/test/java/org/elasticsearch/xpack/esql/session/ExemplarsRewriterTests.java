/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.UnmappedResolution;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.DateEsField;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;

public class ExemplarsRewriterTests extends ESTestCase {

    public void testExemplarsAreSortedByTimestampDescending() {
        LogicalPlan plan = analyze(ExemplarsSettings.ENABLED);

        Limit limit = as(plan, Limit.class);
        assertEquals(1000, as(limit.limit(), Literal.class).value());
        OrderBy orderBy = as(limit.child(), OrderBy.class);
        Order order = as(orderBy.order().getFirst(), Order.class);
        assertEquals("@timestamp", as(order.child(), Attribute.class).name());
        assertEquals(Order.OrderDirection.DESC, order.direction());
    }

    public void testExemplarsLimit() {
        LogicalPlan plan = analyze(new ExemplarsSettings(true, 5));

        Limit maximumLimit = as(plan, Limit.class);
        Limit configuredLimit = as(maximumLimit.child(), Limit.class);
        assertEquals(5, as(configuredLimit.limit(), Literal.class).value());
        as(configuredLimit.child(), OrderBy.class);
    }

    private LogicalPlan analyze(ExemplarsSettings settings) {
        Analyzer analyzer = EsqlTestUtils.analyzer()
            .addIndex("metrics-test", resolution("metrics-test", metricsMapping()))
            .unmappedResolution(UnmappedResolution.NULLIFY)
            .buildAnalyzer();
        LogicalPlan metricsPlan = analyzer.analyze(EsqlTestUtils.TEST_PARSER.parseQuery("TS metrics-test | STATS AVG(cpu_time)"));
        LogicalPlan plan = analyzer.analyze(
            ExemplarsRewriter.exemplarsQuery(metricsPlan, resolution("exemplars-test", exemplarsMapping()), settings)
        );
        if (settings.limit() == null) {
            assertWarnings("No limit defined, adding default limit of [1000]");
        }
        return plan;
    }

    private static IndexResolution resolution(String indexName, Map<String, EsField> mapping) {
        return IndexResolution.valid(
            new EsIndex(
                indexName,
                mapping,
                Map.of(indexName, new IndexProperties(IndexMode.TIME_SERIES, 0)),
                Map.of("", List.of(indexName)),
                Map.of("", List.of(indexName))
            )
        );
    }

    private static Map<String, EsField> metricsMapping() {
        Map<String, EsField> mapping = exemplarsMapping();
        mapping.put("cpu_time", new EsField("cpu_time", DataType.DOUBLE, Map.of(), true, EsField.TimeSeriesFieldType.METRIC));
        return mapping;
    }

    private static Map<String, EsField> exemplarsMapping() {
        Map<String, EsField> mapping = new LinkedHashMap<>();
        mapping.put("@timestamp", DateEsField.dateEsField("@timestamp", Map.of(), true, EsField.TimeSeriesFieldType.NONE));
        return mapping;
    }
}

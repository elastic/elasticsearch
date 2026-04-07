/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.TestOptimizer;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.index.EsIndexGenerator;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.stats.SearchStats;
import org.junit.BeforeClass;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.unboundLogicalOptimizerContext;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;

public class AbstractLocalLogicalPlanOptimizerTests extends ESTestCase {

    protected static LogicalPlanOptimizer logicalOptimizer;

    @BeforeClass
    protected static void init() {
        logicalOptimizer = new LogicalPlanOptimizer(unboundLogicalOptimizerContext());
    }

    protected static TestOptimizer testAnalyzer() {
        return EsqlTestUtils.optimizer().addIndex("test", "mapping-basic.json").addLanguagesLookup();
    }

    protected static TestOptimizer allTypes() {
        return EsqlTestUtils.optimizer().addIndex("test_all", "mapping-all-types.json");
    }

    protected static TestOptimizer ts() {
        return EsqlTestUtils.optimizer().addK8s().addIndex("k8s-downsampled", "k8s-downsampled-mappings.json", IndexMode.TIME_SERIES);
    }

    protected static TestOptimizer metrics() {
        Map<String, EsField> metricsMapping = Map.of(
            "dimension_1",
            new EsField("dimension_1", DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.DIMENSION),
            "dimension_2",
            new EsField("dimension_2", DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.DIMENSION),
            "metric_1",
            new EsField("metric_1", DataType.LONG, Map.of(), true, EsField.TimeSeriesFieldType.METRIC),
            "metric_2",
            new EsField("metric_2", DataType.LONG, Map.of(), true, EsField.TimeSeriesFieldType.METRIC),
            "@timestamp",
            new EsField("@timestamp", DataType.DATETIME, Map.of(), true, EsField.TimeSeriesFieldType.NONE),
            "_tsid",
            new EsField("_tsid", DataType.TSID_DATA_TYPE, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );
        return EsqlTestUtils.optimizer()
            .addIndex(EsIndexGenerator.esIndex("test", metricsMapping, Map.of("test", IndexMode.TIME_SERIES)))
            .addLanguagesLookup()
            .addEnrichPolicy(EnrichPolicy.MATCH_TYPE, "languages_idx", "id", "languages_idx", "mapping-languages.json");
    }

    protected LogicalPlan optimize(LogicalPlan analyzed) {
        return logicalOptimizer.optimize(analyzed);
    }

    protected LogicalPlan localPlan(String query) {
        return testAnalyzer().localPlan(query);
    }

    protected LogicalPlan localPlan(LogicalPlan plan, SearchStats searchStats) {
        return localPlan(plan, EsqlTestUtils.TEST_CFG, searchStats);
    }

    protected LogicalPlan localPlan(LogicalPlan plan, Configuration configuration, SearchStats searchStats) {
        var localContext = new LocalLogicalOptimizerContext(configuration, FoldContext.small(), searchStats);
        return new LocalLogicalPlanOptimizer(localContext).localOptimize(plan);
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }
}

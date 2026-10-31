/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.common.Strings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.VersionMode;
import org.elasticsearch.xpack.esql.action.ColumnInfoImpl;
import org.elasticsearch.xpack.esql.core.expression.UnsupportedAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.DateEsField;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.InvalidMappedField;
import org.elasticsearch.xpack.esql.core.type.InvalidMappedTsField;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.core.type.DataType.AGGREGATE_METRIC_DOUBLE;
import static org.hamcrest.Matchers.containsString;

/**
 * Covers the two paths behind the {@code TS} editor complaints. A pure type conflict ({@code double} vs
 * {@code aggregate_metric_double}) surfaces in discovery ({@code TS idx | LIMIT 0}) as {@code unsupported}
 * with both original types plus a {@code suggested_cast}, while the aggregation itself resolves — so Kibana
 * must consult {@code suggested_cast} instead of treating the conflict as fatal. A time-series role conflict
 * (dimension vs metric) carries no hint and fails the aggregation itself, which needs an ES-side decision.
 */
public class TsAggregateMetricDoubleConflictTests extends AnalyzerTestCase {

    public TsAggregateMetricDoubleConflictTests(VersionMode versionMode) {
        super(versionMode);
    }

    private TestAnalyzer conflictedTsdb() {
        LinkedHashMap<String, Set<String>> typesToIndices = new LinkedHashMap<>();
        typesToIndices.put(DataType.DOUBLE.typeName(), Set.of("idx-a"));
        typesToIndices.put(AGGREGATE_METRIC_DOUBLE.typeName(), Set.of("idx-b"));
        EsField metric = new InvalidMappedField("metric", typesToIndices);
        EsField timestamp = DateEsField.dateEsField("@timestamp", Map.of(), true, EsField.TimeSeriesFieldType.NONE);
        EsIndex index = new EsIndex(
            "idx",
            Map.of("metric", metric, "@timestamp", timestamp),
            Map.of("idx-a", new IndexProperties(IndexMode.TIME_SERIES, 1), "idx-b", new IndexProperties(IndexMode.TIME_SERIES, 1)),
            Map.of(),
            Map.of()
        );
        return analyzer().addIndex(IndexResolution.valid(index));
    }

    public void testDiscoveryQueryLeavesConflictUnsupported() throws Exception {
        LogicalPlan plan = conflictedTsdb().query("TS idx | LIMIT 0");
        logger.info("plan: {}", plan);
        List<UnsupportedAttribute> found = new ArrayList<>();
        plan.forEachExpressionDown(UnsupportedAttribute.class, ua -> {
            if (ua.name().equals("metric")) {
                found.add(ua);
            }
        });
        assertFalse("expected [metric] to surface as unsupported (this is what Kibana sees)", found.isEmpty());
        List<String> originalTypes = found.getLast().field().getOriginalTypes();
        logger.info("originalTypes: {}", originalTypes);
        assertTrue(originalTypes.contains(DataType.DOUBLE.typeName()));
        assertTrue(originalTypes.contains(AGGREGATE_METRIC_DOUBLE.typeName()));

        var col = new ColumnInfoImpl("metric", DataType.UNSUPPORTED, originalTypes);
        String json;
        try (var builder = JsonXContent.contentBuilder()) {
            col.toXContent(builder, null);
            json = Strings.toString(builder);
        }
        logger.info("column json: {}", json);
        // Kibana's conflict check must consult this instead of treating unsupported as fatal.
        assertTrue("expected suggested_cast in [" + json + "]", json.contains("\"suggested_cast\":\"aggregate_metric_double\""));
    }

    public void testStatsAvgOverConflictResolves() {
        // The query itself runs: the implicit AMD cast rule resolves the conflict under the aggregate.
        LogicalPlan plan = conflictedTsdb().query("TS idx | STATS AVG(metric)");
        logger.info("plan: {}", plan);
        assertTrue("expected the STATS query to resolve", plan.resolved());
    }

    private TestAnalyzer roleConflictedTsdb() {
        // Same field, different time-series roles across indices (dimension vs metric).
        // This is not a type conflict, so no cast can reconcile it.
        EsField host = new InvalidMappedTsField("host", "Field [host] has conflicting time-series roles across indices");
        EsField timestamp = DateEsField.dateEsField("@timestamp", Map.of(), true, EsField.TimeSeriesFieldType.NONE);
        EsIndex index = new EsIndex(
            "idx",
            Map.of("host", host, "@timestamp", timestamp),
            Map.of("idx-a", new IndexProperties(IndexMode.TIME_SERIES, 1), "idx-b", new IndexProperties(IndexMode.TIME_SERIES, 1)),
            Map.of(),
            Map.of()
        );
        return analyzer().addIndex(IndexResolution.valid(index));
    }

    public void testRoleConflictCarriesNoHint() throws Exception {
        LogicalPlan plan = roleConflictedTsdb().query("TS idx | LIMIT 0");
        logger.info("plan: {}", plan);
        List<UnsupportedAttribute> found = new ArrayList<>();
        plan.forEachExpressionDown(UnsupportedAttribute.class, ua -> {
            if (ua.name().equals("host")) {
                found.add(ua);
            }
        });
        assertFalse("expected [host] to surface as unsupported", found.isEmpty());
        List<String> originalTypes = found.getLast().field().getOriginalTypes();
        logger.info("originalTypes: {}", originalTypes);
        assertTrue("expected no original types on a role conflict, got " + originalTypes, originalTypes.isEmpty());

        var col = new ColumnInfoImpl("host", DataType.UNSUPPORTED, originalTypes);
        String json;
        try (var builder = JsonXContent.contentBuilder()) {
            col.toXContent(builder, null);
            json = Strings.toString(builder);
        }
        logger.info("column json: {}", json);
        // Nothing for Kibana to consult here: no original types, no suggested_cast.
        assertFalse("expected no suggested_cast in [" + json + "]", json.contains("suggested_cast"));
    }

    public void testStatsOverRoleConflictFails() {
        // Unlike the type conflict, the aggregation itself cannot run: no cast reconciles roles.
        var e = expectThrows(VerificationException.class, () -> roleConflictedTsdb().query("TS idx | STATS AVG(host)"));
        logger.info("expected failure: {}", e.getMessage());
        assertThat(e.getMessage(), containsString("[host]"));
    }
}

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
import org.elasticsearch.xpack.esql.expression.function.aggregate.ImplicitUnionCastKibanaDefinitionTests;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.core.type.DataType.AGGREGATE_METRIC_DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.not;

/**
 * Pins how a {@code TS} field that conflicts across backing indices behaves, as seen by Kibana. A type conflict
 * ({@code double} vs {@code aggregate_metric_double}) surfaces in discovery as {@code unsupported} with both original
 * types, yet every consumer listed in {@code implicit_union_casts.json} resolves over it. A time-series role conflict
 * (dimension vs metric) carries no original types and fails verification.
 */
public class TsAggregateMetricDoubleConflictTests extends AnalyzerTestCase {

    private static final Map<String, String> COMPARISONS = Map.of(
        "equals",
        "==",
        "not_equals",
        "!=",
        "greater_than",
        ">",
        "greater_than_or_equal",
        ">=",
        "less_than",
        "<",
        "less_than_or_equal",
        "<="
    );

    public TsAggregateMetricDoubleConflictTests(VersionMode versionMode) {
        super(versionMode);
    }

    public void testDiscoveryLeavesTypeConflictUnsupported() throws IOException {
        List<String> originalTypes = discoveredOriginalTypes(typeConflictedTsdb(), "metric");
        assertThat(originalTypes, containsInAnyOrder(DOUBLE.typeName(), AGGREGATE_METRIC_DOUBLE.typeName()));
        assertThat(columnJson("metric", originalTypes), containsString("\"suggested_cast\":\"aggregate_metric_double\""));
    }

    public void testPublishedCastsResolveOverTypeConflict() {
        assertThat(ImplicitUnionCastKibanaDefinitionTests.conflictTypes(), hasItems(DOUBLE.esNameIfPossible()));
        for (Map.Entry<String, DataType> cast : ImplicitUnionCastKibanaDefinitionTests.implicitUnionCasts().entrySet()) {
            String operator = COMPARISONS.get(cast.getKey());
            LogicalPlan plan = operator == null
                ? typeConflictedTsdb().query("TS idx | STATS r = " + cast.getKey() + "(metric)")
                : typeConflictedTsdb().query("TS idx | EVAL r = metric " + operator + " 0 | KEEP r | LIMIT 10");
            assertTrue("expected [" + cast.getKey() + "] to resolve over the conflict", plan.resolved());
            assertThat("return type of [" + cast.getKey() + "]", plan.output().getFirst().dataType(), equalTo(cast.getValue()));
        }
    }

    public void testDiscoveryLeavesRoleConflictWithoutHint() throws IOException {
        List<String> originalTypes = discoveredOriginalTypes(roleConflictedTsdb(), "host");
        assertThat(originalTypes, empty());
        assertThat(columnJson("host", originalTypes), not(containsString("suggested_cast")));
    }

    public void testStatsOverRoleConflictFails() {
        var e = expectThrows(VerificationException.class, () -> roleConflictedTsdb().query("TS idx | STATS AVG(host)"));
        assertThat(e.getMessage(), containsString("[host]"));
    }

    private TestAnalyzer typeConflictedTsdb() {
        LinkedHashMap<String, Set<String>> typesToIndices = new LinkedHashMap<>();
        typesToIndices.put(DOUBLE.typeName(), Set.of("idx-a"));
        typesToIndices.put(AGGREGATE_METRIC_DOUBLE.typeName(), Set.of("idx-b"));
        return tsdb(new InvalidMappedField("metric", typesToIndices));
    }

    private TestAnalyzer roleConflictedTsdb() {
        return tsdb(new InvalidMappedTsField("host", "Field [host] has conflicting time-series roles across indices"));
    }

    private TestAnalyzer tsdb(EsField conflicted) {
        EsField timestamp = DateEsField.dateEsField("@timestamp", Map.of(), true, EsField.TimeSeriesFieldType.NONE);
        EsIndex index = new EsIndex(
            "idx",
            Map.of(conflicted.getName(), conflicted, "@timestamp", timestamp),
            Map.of("idx-a", new IndexProperties(IndexMode.TIME_SERIES, 1), "idx-b", new IndexProperties(IndexMode.TIME_SERIES, 1)),
            Map.of(),
            Map.of()
        );
        return analyzer().addIndex(IndexResolution.valid(index));
    }

    private static List<String> discoveredOriginalTypes(TestAnalyzer analyzer, String field) {
        LogicalPlan plan = analyzer.query("TS idx | LIMIT 0");
        List<UnsupportedAttribute> found = new ArrayList<>();
        plan.forEachExpressionDown(UnsupportedAttribute.class, ua -> {
            if (ua.name().equals(field)) {
                found.add(ua);
            }
        });
        assertThat("expected [" + field + "] to surface as unsupported", found, not(empty()));
        return found.getLast().field().getOriginalTypes();
    }

    private static String columnJson(String name, List<String> originalTypes) throws IOException {
        try (var builder = JsonXContent.contentBuilder()) {
            new ColumnInfoImpl(name, DataType.UNSUPPORTED, originalTypes).toXContent(builder, null);
            return Strings.toString(builder);
        }
    }
}

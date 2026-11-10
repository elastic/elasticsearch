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
import org.elasticsearch.xpack.esql.expression.function.aggregate.ImplicitUnionCastKibanaDefinitionTests.Conversion;
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
import static org.hamcrest.Matchers.not;

/**
 * Pins how a field that conflicts across backing indices behaves, as seen by Kibana. A type conflict
 * ({@code double} vs {@code aggregate_metric_double}) surfaces in discovery as {@code unsupported} with both original
 * types, yet resolves under every conversion published in {@code implicit_union_casts.json}. A time-series role conflict
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

    /**
     * Kibana type-checks each published function as if the field were {@code convertsTo}, so over every
     * {@code aggregate_metric_double}/numeric conflict the function must resolve, and return the same type as it does
     * over a plain {@code convertsTo} field.
     */
    public void testPublishedConversionsMatchAnalyzer() {
        Map<String, Conversion> byFunction = ImplicitUnionCastKibanaDefinitionTests.implicitConversions()
            .get(AGGREGATE_METRIC_DOUBLE.esNameIfPossible());
        for (IndexMode mode : new IndexMode[] { IndexMode.STANDARD, IndexMode.TIME_SERIES }) {
            for (DataType numeric : DataType.values()) {
                if (numeric.isNumeric() == false) {
                    continue;
                }
                for (Map.Entry<String, Conversion> function : byFunction.entrySet()) {
                    String query = query(mode, function.getKey());
                    String context = query + " over [" + numeric.typeName() + ", aggregate_metric_double]";
                    LogicalPlan conflicted = index(typeConflict(numeric), mode).query(query);
                    assertTrue("expected " + context + " to resolve", conflicted.resolved());
                    assertThat(context, conflicted.output().getFirst().dataType(), equalTo(function.getValue().returnType()));
                    LogicalPlan converted = index(plainField(function.getValue().convertsTo()), mode).query(query);
                    assertThat(
                        query + " over [" + function.getValue().convertsTo().typeName() + "]",
                        converted.output().getFirst().dataType(),
                        equalTo(function.getValue().returnType())
                    );
                }
            }
        }
    }

    private static String query(IndexMode mode, String function) {
        String source = mode == IndexMode.TIME_SERIES ? "TS idx" : "FROM idx";
        String operator = COMPARISONS.get(function);
        return operator == null
            ? source + " | STATS r = " + function + "(metric) | LIMIT 10"
            : source + " | EVAL r = metric " + operator + " 0 | KEEP r | LIMIT 10";
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
        return index(typeConflict(DOUBLE), IndexMode.TIME_SERIES);
    }

    private TestAnalyzer roleConflictedTsdb() {
        return index(
            new InvalidMappedTsField("host", "Field [host] has conflicting time-series roles across indices"),
            IndexMode.TIME_SERIES
        );
    }

    private static EsField typeConflict(DataType numeric) {
        LinkedHashMap<String, Set<String>> typesToIndices = new LinkedHashMap<>();
        typesToIndices.put(numeric.typeName(), Set.of("idx-a"));
        typesToIndices.put(AGGREGATE_METRIC_DOUBLE.typeName(), Set.of("idx-b"));
        return new InvalidMappedField("metric", typesToIndices);
    }

    private static EsField plainField(DataType type) {
        return new EsField("metric", type, Map.of(), true, EsField.TimeSeriesFieldType.METRIC);
    }

    private TestAnalyzer index(EsField field, IndexMode mode) {
        EsField timestamp = DateEsField.dateEsField("@timestamp", Map.of(), true, EsField.TimeSeriesFieldType.NONE);
        EsIndex index = new EsIndex(
            "idx",
            Map.of(field.getName(), field, "@timestamp", timestamp),
            Map.of("idx-a", new IndexProperties(mode, 1), "idx-b", new IndexProperties(mode, 1)),
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

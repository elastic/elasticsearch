/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.analysis.EnrichResolution;
import org.elasticsearch.xpack.esql.analysis.Verifier;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.session.EsqlConfiguration;
import org.elasticsearch.xpack.esql.stats.Metrics;
import org.elasticsearch.xpack.esql.stats.SearchStats;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeRegistry;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DateUtils;
import org.elasticsearch.xpack.ql.type.EsField;
import org.elasticsearch.xpack.ql.type.TypesTests;
import org.elasticsearch.xpack.ql.util.StringUtils;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyList;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.xpack.ql.TestUtils.of;
import static org.hamcrest.Matchers.instanceOf;

public final class EsqlTestUtils {

    public static class TestSearchStats extends SearchStats {

        public TestSearchStats() {
            super(emptyList());
        }

        @Override
        public long count() {
            return -1;
        }

        @Override
        public long count(String field) {
            return exists(field) ? -1 : 0;
        }

        @Override
        public long count(String field, BytesRef value) {
            return exists(field) ? -1 : 0;
        }

        @Override
        public boolean exists(String field) {
            return true;
        }

        @Override
        public byte[] min(String field, DataType dataType) {
            return null;
        }

        @Override
        public byte[] max(String field, DataType dataType) {
            return null;
        }

        @Override
        public boolean isSingleValue(String field) {
            return false;
        }

        @Override
        public boolean isIndexed(String field) {
            return exists(field);
        }
    }

    public static final TestSearchStats TEST_SEARCH_STATS = new TestSearchStats();

    public static final EsqlConfiguration TEST_CFG = configuration(new QueryPragmas(Settings.EMPTY));

    public static final Verifier TEST_VERIFIER = new Verifier(new Metrics());

    private EsqlTestUtils() {}

    public static EsqlConfiguration configuration(QueryPragmas pragmas, String query) {
        return new EsqlConfiguration(
            DateUtils.UTC,
            Locale.US,
            null,
            null,
            pragmas,
            EsqlPlugin.QUERY_RESULT_TRUNCATION_MAX_SIZE.getDefault(Settings.EMPTY),
            EsqlPlugin.QUERY_RESULT_TRUNCATION_DEFAULT_SIZE.getDefault(Settings.EMPTY),
            query,
            false
        );
    }

    public static EsqlConfiguration configuration(QueryPragmas pragmas) {
        return configuration(pragmas, StringUtils.EMPTY);
    }

    public static EsqlConfiguration configuration(String query) {
        return configuration(new QueryPragmas(Settings.EMPTY), query);
    }

    public static Literal L(Object value) {
        return of(value);
    }

    public static LogicalPlan emptySource() {
        return new LocalRelation(Source.EMPTY, emptyList(), LocalSupplier.EMPTY);
    }

    public static LogicalPlan localSource(BlockFactory blockFactory, List<Attribute> fields, List<Object> row) {
        return new LocalRelation(Source.EMPTY, fields, LocalSupplier.of(BlockUtils.fromListRow(blockFactory, row)));
    }

    public static <T> T as(Object node, Class<T> type) {
        Assert.assertThat(node, instanceOf(type));
        return type.cast(node);
    }

    public static Map<String, EsField> loadMapping(String name) {
        return TypesTests.loadMapping(EsqlDataTypeRegistry.INSTANCE, name, true);
    }

    public static EnrichResolution emptyPolicyResolution() {
        return new EnrichResolution();
    }

    public static SearchStats statsForExistingField(String... names) {
        return fieldMatchingExistOrMissing(true, names);
    }

    public static SearchStats statsForMissingField(String... names) {
        return fieldMatchingExistOrMissing(false, names);
    }

    private static SearchStats fieldMatchingExistOrMissing(boolean exists, String... names) {
        return new TestSearchStats() {
            private final Set<String> fields = Set.of(names);

            @Override
            public boolean exists(String field) {
                return fields.contains(field) == exists;
            }
        };
    }

    public static List<List<Object>> getValuesList(EsqlQueryResponse results) {
        return getValuesList(results.values());
    }

    public static List<List<Object>> getValuesList(Iterator<Iterator<Object>> values) {
        var valuesList = new ArrayList<List<Object>>();
        values.forEachRemaining(row -> {
            var rowValues = new ArrayList<>();
            row.forEachRemaining(rowValues::add);
            valuesList.add(rowValues);
        });
        return valuesList;
    }

    public static List<String> withDefaultLimitWarning(List<String> warnings) {
        List<String> result = warnings == null ? new ArrayList<>() : new ArrayList<>(warnings);
        result.add("No limit defined, adding default limit of [500]");
        return result;
    }

    /**
     * Generates a random enrich command with or without explicit parameters
     */
    public static String randomEnrichCommand(String name, Enrich.Mode mode, String matchField, List<String> enrichFields) {
        String onField = " ";
        String withFields = " ";

        List<String> before = new ArrayList<>();
        List<String> after = new ArrayList<>();

        if (randomBoolean()) {
            // => RENAME new_match_field=match_field | ENRICH name ON new_match_field | RENAME new_match_field AS match_field
            String newMatchField = "my_" + matchField;
            before.add("RENAME " + matchField + " AS " + newMatchField);
            onField = " ON " + newMatchField;
            after.add("RENAME " + newMatchField + " AS " + matchField);
        } else if (randomBoolean()) {
            onField = " ON " + matchField;
        }
        if (randomBoolean()) {
            List<String> fields = new ArrayList<>();
            for (String f : enrichFields) {
                if (randomBoolean()) {
                    fields.add(f);
                } else {
                    // ENRICH name WITH new_a=a,b|new_c=c | RENAME new_a AS a | RENAME new_c AS c
                    fields.add("new_" + f + "=" + f);
                    after.add("RENAME new_" + f + " AS " + f);
                }
            }
            withFields = " WITH " + String.join(",", fields);
        }
        String enrich = "ENRICH ";
        if (mode != Enrich.Mode.ANY || randomBoolean()) {
            enrich += " _" + mode + ":";
        }
        enrich += name;
        enrich += onField;
        enrich += withFields;
        List<String> all = new ArrayList<>(before);
        all.add(enrich);
        all.addAll(after);
        return String.join(" | ", all);
    }
}

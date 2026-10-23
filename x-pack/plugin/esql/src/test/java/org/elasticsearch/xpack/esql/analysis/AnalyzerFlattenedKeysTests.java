/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesIndexResponse;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.fieldcaps.IndexFieldCapabilities;
import org.elasticsearch.action.fieldcaps.IndexFieldCapabilitiesBuilder;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.KeywordEsField;
import org.elasticsearch.xpack.esql.core.type.PotentiallyUnmappedKeywordEsField;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.FlattenedKey;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.FlattenedKeys;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.session.ConfigurationBuilder;
import org.elasticsearch.xpack.esql.session.IndexResolver;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

/**
 * A name under a {@code flattened} field is a {@code keyword} field of its own, the key of that name, and a pattern of
 * {@code KEEP} or {@code DROP} under such a field keeps or drops its keys.
 */
public class AnalyzerFlattenedKeysTests extends ESTestCase {
    private static final Configuration FLATTENED_KEYS = new ConfigurationBuilder(EsqlTestUtils.TEST_CFG).flattenedKeys(true).build();

    public void testKeyIsAKeyword() {
        FieldAttribute key = relationField(
            analyzer(FLATTENED_KEYS, "attrs.bar").query("FROM test | WHERE attrs.bar == \"x\" | KEEP attrs.bar | LIMIT 10"),
            "attrs.bar"
        );
        assertThat(key.dataType(), equalTo(DataType.KEYWORD));
        assertThat(key.field(), instanceOf(KeywordEsField.class));
        assertThat(key.field(), not(instanceOf(PotentiallyUnmappedKeywordEsField.class)));
    }

    public void testKeyIsAKeywordWhenUnmappedFieldsAreLoaded() {
        LogicalPlan plan = analyzer(FLATTENED_KEYS, "attrs.bar").statement(
            "SET unmapped_fields=\"load\"; FROM test | KEEP attrs.bar | LIMIT 10"
        );
        // The field is flattened in every index, so the name is not a field that may be unmapped
        assertThat(relationField(plan, "attrs.bar").field(), not(instanceOf(PotentiallyUnmappedKeywordEsField.class)));
    }

    public void testKeyNeedsTheQueryToReadKeys() {
        analyzer(EsqlTestUtils.TEST_CFG).error("FROM test | KEEP attrs.bar | LIMIT 10", containsString("Unknown column [attrs.bar]"));
    }

    public void testKeyNeedsEveryNodeToReadKeys() {
        TransportVersion before = TransportVersionUtils.getPreviousVersion(Configuration.ESQL_SHARD_RESOLVED_FIELDS);
        analyzer(FLATTENED_KEYS).minimumTransportVersion(before)
            .error("FROM test | KEEP attrs.bar | LIMIT 10", containsString("Unknown column [attrs.bar]"));
    }

    public void testDataNodesReadKeysOnlyIfThePlanDoes() {
        TransportVersion before = TransportVersionUtils.getPreviousVersion(Configuration.ESQL_SHARD_RESOLVED_FIELDS);
        assertThat(FLATTENED_KEYS.forMinimumVersion(before).flattenedKeys(), equalTo(false));
        assertThat(FLATTENED_KEYS.forMinimumVersion(null).flattenedKeys(), equalTo(false));
        assertThat(FLATTENED_KEYS.forMinimumVersion(TransportVersion.current()), sameInstance(FLATTENED_KEYS));
        assertThat(EsqlTestUtils.TEST_CFG.forMinimumVersion(before), sameInstance(EsqlTestUtils.TEST_CFG));
    }

    public void testOlderNodesReadFlattenedFieldsAsBefore() {
        TransportVersion before = TransportVersionUtils.getPreviousVersion(Configuration.ESQL_SHARD_RESOLVED_FIELDS);
        LogicalPlan plan = analyzer(FLATTENED_KEYS).minimumTransportVersion(before).query("FROM test | KEEP attrs | LIMIT 10");
        assertThat(names(plan), contains("attrs"));
    }

    public void testOlderNodesDoNotWarnForMappedSubfields() {
        TransportVersion before = TransportVersionUtils.getPreviousVersion(Configuration.ESQL_SHARD_RESOLVED_FIELDS);
        // attrs.bar is a subfield the flattened field maps, which reads the same on every node
        KeywordEsField bar = new KeywordEsField(
            "attrs.bar",
            Map.of(),
            true,
            Short.MAX_VALUE,
            false,
            false,
            EsField.TimeSeriesFieldType.NONE
        );
        LogicalPlan plan = analyzerWith(FLATTENED_KEYS, bar).minimumTransportVersion(before)
            .query("FROM test | WHERE attrs.bar == \"x\" | STATS c = COUNT(attrs) | LIMIT 10");
        assertThat(names(plan), contains("c"));
    }

    public void testOlderNodesWarnForPatternsOfKeys() {
        TransportVersion before = TransportVersionUtils.getPreviousVersion(Configuration.ESQL_SHARD_RESOLVED_FIELDS);
        // attrs.bar is a field of another index, so the pattern matches a column
        KeywordEsField bar = new KeywordEsField(
            "attrs.bar",
            Map.of(),
            true,
            Short.MAX_VALUE,
            false,
            false,
            EsField.TimeSeriesFieldType.NONE
        );
        LogicalPlan plan = analyzerWith(FLATTENED_KEYS, bar).minimumTransportVersion(before).query("FROM test | KEEP attrs.b* | LIMIT 10");
        assertThat(names(plan), contains("attrs.bar"));
        assertWarnings(Verifier.FLATTENED_SUBFIELDS_NOT_READ_WARNING);
    }

    public void testOlderNodesWarnForNamedKeys() {
        TransportVersion before = TransportVersionUtils.getPreviousVersion(Configuration.ESQL_SHARD_RESOLVED_FIELDS);
        analyzer(FLATTENED_KEYS).minimumTransportVersion(before)
            .statement("SET unmapped_fields=\"nullify\"; FROM test | KEEP attrs.bar | LIMIT 10");
        assertWarnings(Verifier.FLATTENED_SUBFIELDS_NOT_READ_WARNING);
    }

    public void testAKeyOfARenamedContainer() {
        LogicalPlan plan = analyzer(FLATTENED_KEYS).query("FROM test | RENAME attrs AS a | WHERE a.bar == \"x\" | KEEP a.bar | LIMIT 10");
        assertThat(names(plan), contains("a.bar"));
        assertThat(containerKeys(plan), contains("bar"));
    }

    public void testADroppedKeyIsNotReadFromTheContainer() {
        // attrs.bar is a field of the index: once dropped, it is no column, as for a keyword field
        analyzer(FLATTENED_KEYS, "attrs.bar").error(
            "FROM test | DROP attrs.bar | WHERE attrs.bar == \"x\" | LIMIT 10",
            containsString("Unknown column [attrs.bar]")
        );
    }

    public void testAKeyOfAComputedContainer() {
        LogicalPlan plan = analyzer(FLATTENED_KEYS).query("FROM test | EVAL a = attrs | KEEP a.bar | LIMIT 10");
        assertThat(names(plan), contains("a.bar"));
        assertThat(containerKeys(plan), contains("bar"));
    }

    public void testAColumnWinsOverTheKeyOfAContainer() {
        LogicalPlan plan = analyzer(FLATTENED_KEYS).query("FROM test | EVAL a = attrs, a.bar = \"mine\" | KEEP a.bar | LIMIT 10");
        assertThat(names(plan), contains("a.bar"));
        assertThat(containerKeys(plan), empty());
    }

    public void testAPatternOfKeysOfARenamedContainer() {
        List<FlattenedKeys> filters = flattenedKeys(analyzer(FLATTENED_KEYS).query("FROM test | RENAME attrs AS a | KEEP a.b* | LIMIT 10"));
        assertThat(filters.size(), equalTo(1));
        assertThat(filters.getFirst().includes(), contains("b*"));
    }

    public void testDropANameRemovesTheKey() {
        LogicalPlan plan = analyzer(FLATTENED_KEYS, "attrs.secret").query("FROM test | DROP attrs.secret | LIMIT 10");
        assertThat(names(plan), equalTo(names(analyzer(FLATTENED_KEYS).query("FROM test | LIMIT 10"))));
        assertThat(flattenedKeys(plan).getFirst().excludes(), contains("secret"));
    }

    public void testKeepAPatternOfKeys() {
        List<FlattenedKeys> filters = flattenedKeys(analyzer(FLATTENED_KEYS).query("FROM test | KEEP attrs.b*, attrs.c.* | LIMIT 10"));
        assertThat(filters.size(), equalTo(1));
        assertThat(filters.getFirst().includes(), contains("b*", "c.*"));
        assertThat(filters.getFirst().excludes(), empty());
    }

    public void testDropAPatternOfKeys() {
        List<FlattenedKeys> filters = flattenedKeys(analyzer(FLATTENED_KEYS).query("FROM test | DROP attrs.s* | LIMIT 10"));
        assertThat(filters.size(), equalTo(1));
        assertThat(filters.getFirst().includes(), empty());
        assertThat(filters.getFirst().excludes(), contains("s*"));
    }

    public void testDropKeepsTheColumnOrder() {
        LogicalPlan plan = analyzer(FLATTENED_KEYS).query("FROM test | DROP attrs.s* | LIMIT 10");
        assertThat(names(plan), equalTo(names(analyzer(FLATTENED_KEYS).query("FROM test | LIMIT 10"))));
    }

    public void testAPatternOfKeysKeepsTheKeysTheQueryNames() {
        LogicalPlan plan = analyzer(FLATTENED_KEYS).query("FROM test | WHERE attrs.bar == \"x\" | KEEP attrs.b* | LIMIT 10");
        assertThat(names(plan), contains("attrs", "attrs.bar"));
        List<FlattenedKeys> filters = flattenedKeys(plan);
        assertThat(filters.size(), equalTo(1));
        assertThat(filters.getFirst().includes(), contains("b*"));
    }

    public void testAKeyNamedAfterAPatternOfKeys() {
        LogicalPlan plan = analyzer(FLATTENED_KEYS).query("FROM test | KEEP attrs.b* | WHERE attrs.bar == \"x\" | LIMIT 10");
        assertThat(names(plan), contains("attrs", "attrs.bar"));
    }

    public void testAKeyNamedAfterDroppingTheField() {
        LogicalPlan plan = analyzer(FLATTENED_KEYS, "attrs.bar").query(
            "FROM test | DROP attrs | WHERE attrs.bar == \"x\" | KEEP attrs.bar | LIMIT 10"
        );
        assertThat(names(plan), contains("attrs.bar"));
    }

    public void testDropAPatternOfKeysDropsTheKeysTheQueryNames() {
        LogicalPlan plan = analyzer(FLATTENED_KEYS).query("FROM test | WHERE attrs.secret == \"x\" | DROP attrs.s* | LIMIT 10");
        assertThat(names(plan), equalTo(names(analyzer(FLATTENED_KEYS).query("FROM test | LIMIT 10"))));
        assertThat(flattenedKeys(plan).getFirst().excludes(), contains("s*"));
    }

    public void testAFieldKeptWholeKeepsEveryKey() {
        assertThat(flattenedKeys(analyzer(FLATTENED_KEYS).query("FROM test | KEEP attrs, attrs.b* | LIMIT 10")), empty());
        assertThat(flattenedKeys(analyzer(FLATTENED_KEYS).query("FROM test | KEEP attr*, attrs.b* | LIMIT 10")), empty());
        assertThat(flattenedKeys(analyzer(FLATTENED_KEYS).query("FROM test | KEEP *, attrs.b* | LIMIT 10")), empty());
    }

    public void testAPatternOfColumnsIsNotAPatternOfKeys() {
        assertThat(flattenedKeys(analyzer(FLATTENED_KEYS).query("FROM test | KEEP na* | LIMIT 10")), empty());
    }

    public void testPatternsOfKeysNeedTheQueryToReadKeys() {
        analyzer(EsqlTestUtils.TEST_CFG).error(
            "FROM test | KEEP attrs.b* | LIMIT 10",
            containsString("No matches found for pattern [attrs.b*]")
        );
    }

    /**
     * An analyzer over {@code test} as index resolution builds it for a query that uses {@code namedFields}: each one under the
     * {@code flattened} field {@code attrs} is a {@code keyword} field of the index, the key of that name.
     */
    private static TestAnalyzer analyzer(Configuration configuration, String... namedFields) {
        Map<String, IndexFieldCapabilities> fields = Map.of(
            "attrs",
            new IndexFieldCapabilitiesBuilder("attrs", "flattened").build(),
            "name",
            new IndexFieldCapabilitiesBuilder("name", "keyword").build()
        );
        FieldCapabilitiesResponse caps = FieldCapabilitiesResponse.builder()
            .withIndexResponses(List.of(new FieldCapabilitiesIndexResponse("test", "test", fields, true, IndexMode.STANDARD, 1, 1L, 1L)))
            .build();
        var fieldsInfo = new IndexResolver.FieldsInfo(
            caps,
            TransportVersion.current(),
            false,
            false,
            false,
            false,
            false,
            true,
            Set.of(namedFields)
        );
        return EsqlTestUtils.analyzer()
            .configuration(configuration)
            .minimumTransportVersion(TransportVersion.current())
            .addIndex(IndexResolver.mergedMappings("test", false, fieldsInfo, false, IndexResolver.DO_NOT_GROUP).get());
    }

    private static TestAnalyzer analyzer(Configuration configuration) {
        return analyzerWith(configuration);
    }

    /** An analyzer over the index {@code test}, which maps {@code attrs} as a flattened field, {@code name} and {@code others}. */
    private static TestAnalyzer analyzerWith(Configuration configuration, EsField... others) {
        Map<String, EsField> mapping = new HashMap<>();
        mapping.put("attrs", new EsField("attrs", DataType.FLATTENED, Map.of(), true, false, EsField.TimeSeriesFieldType.NONE));
        mapping.put("name", new KeywordEsField("name", Map.of(), true, Short.MAX_VALUE, false, false, EsField.TimeSeriesFieldType.NONE));
        for (EsField other : others) {
            mapping.put(other.getName(), other);
        }
        return EsqlTestUtils.analyzer()
            .configuration(configuration)
            .minimumTransportVersion(TransportVersion.current())
            .addIndex(new EsIndex("test", mapping, Map.of("test", new IndexProperties(IndexMode.STANDARD, 1)), Map.of(), Map.of()));
    }

    private static FieldAttribute relationField(LogicalPlan plan, String name) {
        List<FieldAttribute> fields = new ArrayList<>();
        plan.forEachDown(EsRelation.class, relation -> {
            relation.output().forEach(attribute -> {
                if (attribute instanceof FieldAttribute fa && fa.name().equals(name)) {
                    fields.add(fa);
                }
            });
        });
        assertThat(fields.size(), equalTo(1));
        return fields.getFirst();
    }

    private static List<String> names(LogicalPlan plan) {
        return plan.output().stream().map(Attribute::name).toList();
    }

    /** The keys {@code plan} reads from container columns. */
    private static List<String> containerKeys(LogicalPlan plan) {
        List<String> keys = new ArrayList<>();
        plan.forEachExpressionDown(FlattenedKey.class, read -> keys.add(read.key()));
        return keys;
    }

    private static List<FlattenedKeys> flattenedKeys(LogicalPlan plan) {
        List<FlattenedKeys> filters = new ArrayList<>();
        plan.forEachExpressionDown(FlattenedKeys.class, filters::add);
        return filters;
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.querydsl.query;

import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.tree.SourceTests;
import org.elasticsearch.xpack.esql.core.util.StringUtils;
import org.elasticsearch.xpack.kql.query.KqlQueryBuilder;

import java.time.ZoneId;
import java.time.zone.ZoneRulesException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.index.query.AbstractQueryBuilder.BOOST_FIELD;
import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;
import static org.elasticsearch.xpack.kql.query.KqlQueryBuilder.CASE_INSENSITIVE_FIELD;
import static org.elasticsearch.xpack.kql.query.KqlQueryBuilder.DEFAULT_FIELD_FIELD;
import static org.elasticsearch.xpack.kql.query.KqlQueryBuilder.TIME_ZONE_FIELD;
import static org.hamcrest.Matchers.equalTo;

public class KqlQueryTests extends ESTestCase {
    static KqlQuery randomKqkQueryQuery() {
        Map<String, Object> options = new HashMap<>();

        if (randomBoolean()) {
            options.put(CASE_INSENSITIVE_FIELD.getPreferredName(), randomBoolean());
        }

        if (randomBoolean()) {
            options.put(DEFAULT_FIELD_FIELD.getPreferredName(), randomIdentifier());
        }

        if (randomBoolean()) {
            options.put(TIME_ZONE_FIELD.getPreferredName(), randomZone().getId());
        }

        if (randomBoolean()) {
            options.put(BOOST_FIELD.getPreferredName(), randomFloat() * 5.0f + 0.1f); // Random float between 0.1 and 5.1
        }

        return new KqlQuery(SourceTests.randomSource(), randomAlphaOfLength(5), Collections.unmodifiableMap(options));
    }

    public void testEqualsAndHashCode() {
        for (int runs = 0; runs < 100; runs++) {
            checkEqualsAndHashCode(randomKqkQueryQuery(), KqlQueryTests::copy, KqlQueryTests::mutate);
        }
    }

    private static KqlQuery copy(KqlQuery query) {
        return new KqlQuery(query.source(), query.query(), query.options());
    }

    private static KqlQuery mutate(KqlQuery query) {
        List<Function<KqlQuery, KqlQuery>> options = Arrays.asList(
            q -> new KqlQuery(SourceTests.mutate(q.source()), q.query(), q.options()),
            q -> new KqlQuery(q.source(), randomValueOtherThan(q.query(), () -> randomAlphaOfLength(5)), q.options()),
            q -> new KqlQuery(q.source(), q.query(), mutateOptions(q.options()))
        );

        return randomFrom(options).apply(query);
    }

    private static Map<String, Object> mutateOptions(Map<String, Object> options) {
        Map<String, Object> mutatedOptions = new HashMap<>(options);
        if (options.isEmpty() == false && randomBoolean()) {
            mutatedOptions = options.entrySet()
                .stream()
                .filter(entry -> randomBoolean())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }

        while (mutatedOptions.equals(options)) {
            if (randomBoolean()) {
                mutatedOptions = mutateOption(mutatedOptions, CASE_INSENSITIVE_FIELD.getPreferredName(), () -> randomBoolean());
            }

            if (randomBoolean()) {
                mutatedOptions = mutateOption(mutatedOptions, DEFAULT_FIELD_FIELD.getPreferredName(), () -> randomIdentifier());
            }

            if (randomBoolean()) {
                mutatedOptions = mutateOption(mutatedOptions, TIME_ZONE_FIELD.getPreferredName(), () -> randomZone().getId());
            }

            if (randomBoolean()) {
                mutatedOptions = mutateOption(mutatedOptions, "boost", () -> randomFloat() * 5.0f + 0.1f);
            }
        }

        return Collections.unmodifiableMap(mutatedOptions);
    }

    private static Map<String, Object> mutateOption(Map<String, Object> options, String optionName, Supplier<Object> valueSupplier) {
        options = new HashMap<>(options);
        options.put(optionName, randomValueOtherThan(options.get(optionName), valueSupplier));
        return options;
    }

    public void testQueryBuilding() {
        KqlQueryBuilder qb = getBuilder(Map.of("case_insensitive", false));
        assertThat(qb.caseInsensitive(), equalTo(false));

        qb = getBuilder(Map.of("case_insensitive", false, "time_zone", "UTC", "default_field", "foo"));
        assertThat(qb.caseInsensitive(), equalTo(false));
        assertThat(qb.timeZone(), equalTo(ZoneId.of("UTC")));
        assertThat(qb.defaultField(), equalTo("foo"));

        qb = getBuilder(Map.of("boost", 2.5f));
        assertThat(qb.boost(), equalTo(2.5f));

        qb = getBuilder(Map.of("case_insensitive", true, "boost", 1.5f, "default_field", "content"));
        assertThat(qb.caseInsensitive(), equalTo(true));
        assertThat(qb.boost(), equalTo(1.5f));
        assertThat(qb.defaultField(), equalTo("content"));

        Exception e = expectThrows(IllegalArgumentException.class, () -> getBuilder(Map.of("pizza", "yummy")));
        assertThat(e.getMessage(), equalTo("illegal kql query option [pizza]"));

        e = expectThrows(ZoneRulesException.class, () -> getBuilder(Map.of("time_zone", "aoeu")));
        assertThat(e.getMessage(), equalTo("Unknown time-zone ID: aoeu"));
    }

    private static KqlQueryBuilder getBuilder(Map<String, Object> options) {
        final Source source = new Source(1, 1, StringUtils.EMPTY);
        final KqlQuery kqlQuery = new KqlQuery(source, "eggplant", options);
        return (KqlQueryBuilder) kqlQuery.asBuilder();
    }

    public void testToString() {
        final Source source = new Source(1, 1, StringUtils.EMPTY);
        final KqlQuery kqlQuery = new KqlQuery(source, "eggplant", Map.of());
        assertEquals("KqlQuery@1:2[eggplant]", kqlQuery.toString());
    }

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>(ClusterModule.getNamedWriteables());
        entries.add(new NamedWriteableRegistry.Entry(KqlQueryBuilder.class, KqlQueryBuilder.NAME, KqlQueryBuilder::new));
        return new NamedWriteableRegistry(entries);
    }
}

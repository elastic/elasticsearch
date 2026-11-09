/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator;

import org.elasticsearch.Build;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.generator.command.CommandGenerator.CommandDescription;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

public class EsqlQueryGeneratorTests extends ESTestCase {
    private static final List<Column> FUNNEL_EVENTS = List.of(
        new Column("@timestamp", "date", List.of()),
        new Column("event_nanos", "date_nanos", List.of()),
        new Column("user", "keyword", List.of()),
        new Column("session", "keyword", List.of()),
        new Column("device", "keyword", List.of()),
        new Column("country", "keyword", List.of()),
        new Column("action", "keyword", List.of()),
        new Column("amount", "double", List.of()),
        new Column("tags", "keyword", List.of())
    );

    public void testGeneratedFunnelsResolve() {
        assumeTrue("WINDOW_FUNNEL is only available in snapshot builds", Build.current().isSnapshot());
        TestAnalyzer analyzer = analyzer().addIndex("funnel_events", "mapping-funnel_events.json");
        for (int i = 0; i < 200; i++) {
            String funnel = EsqlQueryGenerator.windowFunnel(FUNNEL_EVENTS, false);
            assertThat(funnel, startsWith("window_funnel("));
            analyzer.query("FROM funnel_events | STATS level = " + funnel + " BY user | LIMIT 10");
            analyzer.query("FROM funnel_events | INLINE STATS level = " + funnel + " BY user | LIMIT 10");
        }
    }

    public void testGeneratedFunnelsWithUnmappedFieldsResolve() {
        assumeTrue("WINDOW_FUNNEL is only available in snapshot builds", Build.current().isSnapshot());
        TestAnalyzer analyzer = analyzer().addIndex("funnel_events", "mapping-funnel_events.json");
        for (int i = 0; i < 200; i++) {
            String funnel = EsqlQueryGenerator.windowFunnel(FUNNEL_EVENTS, true);
            analyzer.statement("SET unmapped_fields=\"nullify\"; FROM funnel_events | STATS level = " + funnel + " | LIMIT 10");
        }
    }

    public void testNoFunnelWithoutTimestamp() {
        assumeTrue("WINDOW_FUNNEL is only available in snapshot builds", Build.current().isSnapshot());
        List<Column> noTimestamp = FUNNEL_EVENTS.stream().filter(c -> c.type().startsWith("date") == false).toList();
        assertThat(EsqlQueryGenerator.windowFunnel(noTimestamp, randomBoolean()), nullValue());
    }

    public void testAggGeneratesFunnels() {
        assumeTrue("WINDOW_FUNNEL is only available in snapshot builds", Build.current().isSnapshot());
        List<CommandDescription> from = List.of(new CommandDescription("from", null, "FROM funnel_events", Map.of()));
        int funnels = 0;
        for (int i = 0; i < 2000; i++) {
            if (EsqlQueryGenerator.agg(FUNNEL_EVENTS, from).contains("window_funnel(")) {
                funnels++;
            }
        }
        assertThat(funnels, greaterThan(0));
    }

    public void testAggGeneratesNoFunnelsInTimeSeries() {
        List<CommandDescription> ts = List.of(new CommandDescription("ts", null, "TS funnel_events", Map.of()));
        for (int i = 0; i < 2000; i++) {
            assertThat(EsqlQueryGenerator.agg(FUNNEL_EVENTS, ts), not(containsString("window_funnel(")));
            assertThat(EsqlQueryGenerator.agg(FUNNEL_EVENTS, null), not(containsString("window_funnel(")));
        }
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator.command.pipe;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.generator.Column;
import org.elasticsearch.xpack.esql.generator.command.CommandGenerator.CommandDescription;
import org.elasticsearch.xpack.esql.generator.command.source.FromGenerator;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.generator.command.CommandGenerator.EMPTY_DESCRIPTION;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThan;

public class HighlightGeneratorTests extends ESTestCase {

    private static final List<Column> COLUMNS = List.of(
        new Column("message", "text", List.of()),
        new Column("_index", "keyword", List.of()),
        new Column("_id", "keyword", List.of())
    );
    private static final CommandDescription FROM = new CommandDescription("from", FromGenerator.INSTANCE, "from idx", Map.of());
    private static final CommandDescription FROM_UNION = new CommandDescription(
        "from",
        FromGenerator.INSTANCE,
        "from idx, (from idx)",
        Map.of(FromGenerator.HAS_UNION_ALL, true)
    );
    private static final CommandDescription FORK = new CommandDescription(ForkGenerator.FORK, ForkGenerator.INSTANCE, " | fork", Map.of());

    public void testMetadataColumnsAreNotHighlighted() {
        for (int i = 0; i < 1000; i++) {
            CommandDescription description = generate(List.of(FROM));
            assertNotSame(EMPTY_DESCRIPTION, description);
            assertFalse(description.commandString(), description.commandString().contains("_index"));
            assertFalse(description.commandString(), description.commandString().contains("_id"));
            @SuppressWarnings("unchecked")
            List<String> generated = (List<String>) description.context().get(HighlightGenerator.HIGHLIGHT_COLUMNS);
            assertThat(description.commandString(), generated, everyItem(endsWith("message")));
        }
    }

    public void testAnalyzerIsNamedOnceAfterAFork() {
        CommandDescription limit = new CommandDescription(LimitGenerator.LIMIT, LimitGenerator.INSTANCE, " | limit 5", Map.of());
        for (int i = 0; i < 200; i++) {
            String command = generate(List.of(FROM, FORK, limit)).commandString();
            assertThat(command, command.split("\"analyzer\": ", -1).length, equalTo(2));
        }
    }

    public void testAnalyzerIsAlwaysNamedAfterAUnionSource() {
        for (int i = 0; i < 200; i++) {
            assertThat(generate(List.of(FROM_UNION)).commandString(), containsString("\"analyzer\": "));
        }
    }

    public void testAnalyzerIsAlwaysNamedAfterDedupOrStats() {
        CommandDescription dedup = new CommandDescription(DedupGenerator.DEDUP, DedupGenerator.INSTANCE, " | dedup", Map.of());
        CommandDescription stats = new CommandDescription(
            StatsGenerator.STATS,
            StatsGenerator.INSTANCE,
            " | stats c = count(*) by message",
            Map.of()
        );
        for (int i = 0; i < 200; i++) {
            assertThat(generate(List.of(FROM, dedup)).commandString(), containsString("\"analyzer\": "));
            assertThat(generate(List.of(FROM, stats)).commandString(), containsString("\"analyzer\": "));
        }
    }

    public void testAnalyzerIsOptionalWithoutAMerge() {
        int withoutAnalyzer = 0;
        for (int i = 0; i < 200; i++) {
            withoutAnalyzer += generate(List.of(FROM)).commandString().contains("\"analyzer\": ") ? 0 : 1;
        }
        assertThat(withoutAnalyzer, greaterThan(0));
    }

    private static CommandDescription generate(List<CommandDescription> previousCommands) {
        return HighlightGenerator.INSTANCE.generate(previousCommands, COLUMNS, null, null, null);
    }
}

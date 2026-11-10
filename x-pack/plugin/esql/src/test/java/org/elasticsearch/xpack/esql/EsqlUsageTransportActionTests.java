/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.watcher.common.stats.Counters;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

public class EsqlUsageTransportActionTests extends ESTestCase {

    public void testEachQueryPathBecomesOneObject() {
        Counters counters = new Counters();
        counters.inc("query_paths.kibana.discover.from.total", 3);
        counters.inc("query_paths.kibana.discover.from.failed", 1);
        counters.inc("query_paths.kibana.discover.from.commands.where", 2);
        counters.inc("query_paths.fleet._none.row.total", 1);
        Object paths = EsqlUsageTransportAction.usageMap(counters).get("query_paths");
        assertThat(
            paths,
            equalTo(List.of(path("fleet", "_none", "row", 1, 0, Map.of()), path("kibana", "discover", "from", 3, 1, Map.of("where", 2L))))
        );
    }

    public void testQueryPathsAreSortedByCallerSourceAndLeadingCommand() {
        Counters counters = new Counters();
        counters.inc("query_paths.rest.python.ts.total", 1);
        counters.inc("query_paths.rest.curl.ts.total", 1);
        counters.inc("query_paths.rest.curl.from.total", 1);
        counters.inc("query_paths.kibana.lens.from.total", 1);
        Object paths = EsqlUsageTransportAction.usageMap(counters).get("query_paths");
        assertThat(
            paths,
            equalTo(
                List.of(
                    path("kibana", "lens", "from", 1, 0, Map.of()),
                    path("rest", "curl", "from", 1, 0, Map.of()),
                    path("rest", "curl", "ts", 1, 0, Map.of()),
                    path("rest", "python", "ts", 1, 0, Map.of())
                )
            )
        );
    }

    public void testQueryPathsFromSeveralNodesAreSummed() {
        Counters first = new Counters();
        first.inc("query_paths.rest.go.from.total", 2);
        first.inc("query_paths.rest.go.from.commands.keep", 2);
        Counters second = new Counters();
        second.inc("query_paths.rest.go.from.total", 3);
        second.inc("query_paths.rest.go.from.failed", 1);
        second.inc("query_paths.rest.go.from.commands.keep", 1);
        Object paths = EsqlUsageTransportAction.usageMap(Counters.merge(List.of(first, second))).get("query_paths");
        assertThat(paths, equalTo(List.of(path("rest", "go", "from", 5, 1, Map.of("keep", 3L)))));
    }

    public void testNoQueryPathsLeavesTheKeyOut() {
        Counters counters = new Counters();
        counters.inc("queries.rest.total", 1);
        assertThat(EsqlUsageTransportAction.usageMap(counters), not(hasKey("query_paths")));
    }

    @SuppressWarnings("unchecked")
    public void testOtherUsageKeysStayNested() {
        Counters counters = new Counters();
        counters.inc("queries.rest.total", 4);
        counters.inc("features.where", 2);
        counters.inc("query_paths.rest.other.from.total", 4);
        Map<String, Object> usage = EsqlUsageTransportAction.usageMap(counters);
        assertThat((Map<String, Object>) usage.get("queries"), hasEntry("rest", Map.of("total", 4L)));
        assertThat((Map<String, Object>) usage.get("features"), hasEntry("where", 2L));
    }

    private static Map<String, Object> path(
        String caller,
        String source,
        String leadingCommand,
        long total,
        long failed,
        Map<String, Long> commands
    ) {
        return Map.of(
            "caller",
            caller,
            "source",
            source,
            "leading_command",
            leadingCommand,
            "total",
            total,
            "failed",
            failed,
            "commands",
            commands
        );
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.action.AbstractSequenceIntegTestCase;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;

/** Copying arguments for per-argument correlation must preserve the original array through stage optimization. */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class SequenceExpandedArgumentIT extends AbstractSequenceIntegTestCase {
    public void testNormalizedInstallerArgumentKeepsOriginalArguments() {
        String index = "sequence-expanded-installer-arguments";
        String host = "fixture-host";
        String user = "fixture-user";
        String installer = "C:\\Downloads\\Fixture.MSI";
        String origin = installer + ":Zone.Identifier";
        List<String> arguments = List.of("C:\\Windows\\System32\\msiexec.exe", "/i", installer);
        assertAcked(
            indicesAdmin().prepareCreate(index)
                .setSettings(indexSettings(1, 0))
                .setMapping(
                    "@timestamp",
                    "type=date",
                    "host.id",
                    "type=keyword",
                    "user.id",
                    "type=keyword",
                    "event.action",
                    "type=keyword",
                    "event.type",
                    "type=keyword",
                    "file.path",
                    "type=keyword",
                    "file.extension",
                    "type=keyword",
                    "process.name",
                    "type=keyword",
                    "process.args",
                    "type=keyword"
                )
        );
        ensureGreen(index);
        assertFalse(
            client().prepareBulk()
                .add(
                    prepareIndex(index).setId("download")
                        .setSource(
                            Map.of(
                                "@timestamp",
                                "2026-01-01T00:00:00Z",
                                "host",
                                Map.of("id", host),
                                "user",
                                Map.of("id", user),
                                "event",
                                Map.of("action", List.of("creation")),
                                "file",
                                Map.of("path", origin, "extension", "msi")
                            )
                        )
                )
                .add(
                    prepareIndex(index).setId("execution")
                        .setSource(
                            Map.of(
                                "@timestamp",
                                "2026-01-01T00:00:01Z",
                                "host",
                                Map.of("id", host),
                                "user",
                                Map.of("id", user),
                                "event",
                                Map.of("type", List.of("start")),
                                "process",
                                Map.of("name", "msiexec.exe", "args", arguments)
                            )
                        )
                )
                .get()
                .hasFailures()
        );
        refresh(index);
        String query = """
            SEQUENCE BY host.id, Esql.artifact_path, user.id WITHIN 10 minutes PRIMARY download
              (FROM %s METADATA _id
               | WHERE MV_CONTAINS(event.action, "creation") AND file.extension == "msi"
               | EVAL Esql.artifact_path = REPLACE(TO_LOWER(file.path), ":zone.identifier", "")
               | KEEP host.id, user.id, Esql.artifact_path, file.path, _id) AS download,
              (FROM %s
               | WHERE MV_CONTAINS(event.type, "start") AND process.name == "msiexec.exe"
               | EVAL Esql.artifact_path = process.args
               | MV_EXPAND Esql.artifact_path
               | EVAL Esql.artifact_path = REPLACE(TO_LOWER(Esql.artifact_path), ":zone.identifier", "")
               | WHERE Esql.artifact_path LIKE "*.msi"
               | KEEP host.id, user.id, Esql.artifact_path, process.args) AS execution
            | EVAL execution_args = MEMBER(execution, process.args)
            | KEEP _id, host.id, user.id, Esql.artifact_path, file.path, execution_args
            | LIMIT 10
            """.formatted(index, index);
        try (var response = run(query)) {
            assertFalse(response.isPartial());
            var rows = getValuesList(response);
            assertEquals(1, rows.size());
            var row = rows.getFirst();
            assertEquals(List.of("download", host, user, "c:\\downloads\\fixture.msi", origin), row.subList(0, 5));
            assertTrue(row.get(5) instanceof List<?>);
            var actualArguments = (List<?>) row.get(5);
            assertEquals(arguments.stream().sorted().toList(), actualArguments.stream().map(String.class::cast).sorted().toList());
        }
    }
}

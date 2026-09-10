/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.core.PathUtils;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.hamcrest.Matchers.empty;

/**
 * Guards the "Common settings" table in {@code esql-data-federation-datasets.md} against silent omissions.
 *
 * <p>Every key in {@link FileDataSourceValidator#COORDINATOR_DATASET_KEYS} and the format-agnostic
 * {@code schema_sample_size} must either have a table row (a line starting with {@code | `<key>`}) or an
 * explicit {@code %} comment recording the reason for its absence (a line starting with
 * {@code % <key> }). The existing {@code schema_sample_size} and {@code hive_partitioning} comments are
 * the template; this test enforces the convention so a new setting cannot silently repeat the
 * {@code partition_path} gap — where a fully-supported setting had no row and no comment, and a team member
 * concluded from the table alone that the setting did not exist.
 */
public class FileDataSourceValidatorDocCoverageTests extends ESTestCase {

    private static final String DOC_PATH = "docs/reference/query-languages/esql/esql-data-federation-datasets.md";

    public void testEveryBaseSettingIsDocumentedOrExplicitlyExcluded() throws IOException {
        // schema_sample_size is in DATASET_FIELDS but not COORDINATOR_DATASET_KEYS — it is consumed by
        // format readers rather than the coordinator. Include it explicitly so the full base set is covered.
        Set<String> allBaseKeys = new HashSet<>(FileDataSourceValidator.COORDINATOR_DATASET_KEYS);
        allBaseKeys.add("schema_sample_size");

        Path docFile = findDocFile();
        List<String> lines = Files.readAllLines(docFile);

        Set<String> undocumented = new TreeSet<>();
        for (String key : allBaseKeys) {
            boolean hasRow = lines.stream().anyMatch(l -> l.startsWith("| `" + key + "`"));
            boolean hasComment = lines.stream().anyMatch(l -> l.startsWith("% " + key + " "));
            if (hasRow == false && hasComment == false) {
                undocumented.add(key);
            }
        }
        assertThat(
            "The following base dataset settings have neither a table row nor a % comment in ["
                + DOC_PATH
                + "]. Add a row documenting the setting, or add a % comment explaining its deliberate absence:",
            undocumented,
            empty()
        );
    }

    /**
     * Locates the repo root from the test working directory (Gradle sets it to
     * {@code <module>/build/testrun/<task>}; IDEs use the module or repo root). Walks up, accepting
     * the level where the doc file exists.
     */
    private static Path findDocFile() {
        Path cur = PathUtils.get("").toAbsolutePath();
        for (int i = 0; i < 12 && cur != null; i++, cur = cur.getParent()) {
            Path candidate = cur.resolve(DOC_PATH);
            if (Files.exists(candidate)) {
                return candidate;
            }
        }
        throw new AssertionError(
            "cannot locate [" + DOC_PATH + "] from [" + PathUtils.get("").toAbsolutePath() + "] — the doc coverage test needs the repo docs"
        );
    }
}

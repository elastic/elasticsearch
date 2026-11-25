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
import java.util.List;
import java.util.TreeSet;

import static org.hamcrest.Matchers.empty;

/**
 * Guards the settings reference in {@code esql-data-federation-dataset-settings.md} against silent omissions.
 *
 * <p>Every key in {@link FileDataSourceValidator#DATASET_FIELDS} must either have a reference entry or an
 * explicit {@code %} comment recording the reason for its absence (a line starting with {@code % <key> }).
 * A reference entry is a definition-list term (a line starting with {@code `<key>`} whose next nonblank
 * line starts with {@code :   }) or a table row (a line starting with {@code | `<key>`}). The existing
 * {@code hive_partitioning} comment is the template; this test enforces the convention so a new setting
 * cannot silently repeat the {@code partition_path} gap — where a fully-supported setting had no entry and no
 * comment, and a team member concluded from the reference alone that the setting did not exist.
 */
public class FileDataSourceValidatorDocCoverageTests extends ESTestCase {

    private static final String DOC_PATH = "docs/reference/query-languages/esql/esql-data-federation-dataset-settings.md";

    public void testEveryBaseSettingIsDocumentedOrExplicitlyExcluded() throws IOException {
        Path docFile = findDocFile();
        List<String> lines = Files.readAllLines(docFile);

        var undocumented = new TreeSet<String>();
        for (String key : FileDataSourceValidator.DATASET_FIELDS) {
            boolean hasEntry = hasDefinitionTerm(lines, key) || lines.stream().anyMatch(l -> l.startsWith("| `" + key + "`"));
            boolean hasComment = lines.stream().anyMatch(l -> l.startsWith("% " + key + " "));
            if (hasEntry == false && hasComment == false) {
                undocumented.add(key);
            }
        }
        assertThat(
            "The following base dataset settings have neither a reference entry nor a % comment in ["
                + DOC_PATH
                + "]. Add a reference entry documenting the setting, or add a % comment explaining its deliberate absence:",
            undocumented,
            empty()
        );
    }

    /**
     * True when a line starts with the backtick-quoted key and the next nonblank line opens a definition
     * ({@code :   }). Requiring the definition marker keeps prose that merely begins with the key from counting.
     */
    private static boolean hasDefinitionTerm(List<String> lines, String key) {
        String term = "`" + key + "`";
        for (int i = 0; i < lines.size(); i++) {
            if (lines.get(i).startsWith(term) == false) {
                continue;
            }
            for (int j = i + 1; j < lines.size(); j++) {
                String next = lines.get(j);
                if (next.isBlank() == false) {
                    if (next.startsWith(":   ")) {
                        return true;
                    }
                    break;
                }
            }
        }
        return false;
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

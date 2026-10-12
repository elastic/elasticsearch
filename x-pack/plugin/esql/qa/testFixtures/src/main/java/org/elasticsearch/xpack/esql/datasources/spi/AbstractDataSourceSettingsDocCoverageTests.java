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
import java.util.Set;
import java.util.TreeSet;

import static org.hamcrest.Matchers.empty;

/**
 * Per-provider base test that keeps the data source settings reference in step with the code. Every
 * data source type should have a sibling test class extending this base.
 *
 * <p>The reference page has one {@code ##} section per data source type. The base fails when a
 * setting the provider accepts has no definition-list entry in that provider's section, so adding a
 * setting without documenting it breaks the build. Settings that are deliberately left out of the
 * public docs go in {@link #excludedSettings()} rather than in the docs source.
 */
public abstract class AbstractDataSourceSettingsDocCoverageTests extends ESTestCase {

    private static final String DOC_PATH = "docs/reference/query-languages/esql/esql-data-federation-data-source-settings.md";

    /** Returns the name of every setting the provider accepts on a data source. */
    protected abstract Set<String> settingNames();

    /** Returns the title of the provider's {@code ##} section in the reference page, without its anchor (e.g. {@code "Amazon S3"}). */
    protected abstract String docSectionTitle();

    /** Returns settings that are intentionally undocumented. Each entry should carry a comment explaining why. */
    protected Set<String> excludedSettings() {
        return Set.of();
    }

    public void testEverySettingIsDocumentedOrExcluded() throws IOException {
        List<String> section = readSection(Files.readAllLines(findDocFile()), docSectionTitle());
        var undocumented = new TreeSet<String>();
        for (String key : settingNames()) {
            if (excludedSettings().contains(key) == false && hasDefinitionTerm(section, key) == false) {
                undocumented.add(key);
            }
        }
        assertThat(
            "The following ["
                + docSectionTitle()
                + "] data source settings have no reference entry in ["
                + DOC_PATH
                + "]. Add a reference entry documenting the setting, or add it to excludedSettings() with a comment explaining why:",
            undocumented,
            empty()
        );
    }

    public void testExcludedSettingsAreAccepted() {
        var unknown = new TreeSet<>(excludedSettings());
        unknown.removeAll(settingNames());
        assertThat("excludedSettings() lists settings the provider doesn't accept, so remove them:", unknown, empty());
    }

    private static List<String> readSection(List<String> lines, String title) {
        int start = -1;
        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i);
            if (line.equals("## " + title) || line.startsWith("## " + title + " [")) {
                start = i + 1;
                break;
            }
        }
        if (start < 0) {
            throw new AssertionError("cannot find a [## " + title + "] section in [" + DOC_PATH + "]");
        }
        int end = lines.size();
        for (int i = start; i < lines.size(); i++) {
            if (lines.get(i).startsWith("## ")) {
                end = i;
                break;
            }
        }
        return lines.subList(start, end);
    }

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

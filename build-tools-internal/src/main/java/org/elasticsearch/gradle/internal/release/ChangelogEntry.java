/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.release;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * This class models the contents of a changelog YAML file. We validate it using a JSON Schema.
 * <ul>
 *   <li><code>buildSrc/src/main/resources/changelog-schema.json</code></li>
 *   <li><a href="https://json-schema.org/understanding-json-schema/">Understanding JSON Schema</a></li>
 * </ul>
 */
public class ChangelogEntry {
    private Integer pr;
    private List<Integer> issues;
    private String area;
    private String type;
    private String summary;
    private Highlight highlight;
    private Breaking breaking;
    private Deprecation deprecation;

    private static final ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());

    /**
     * Create a new instance by parsing the supplied file
     * @param file the YAML file to parse
     * @return a new instance
     */
    public static ChangelogEntry parse(File file) {
        try {
            return yamlMapper.readValue(file, ChangelogEntry.class);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public Integer getPr() {
        return pr;
    }

    public void setPr(Integer pr) {
        this.pr = pr;
    }

    public List<Integer> getIssues() {
        return issues;
    }

    public void setIssues(List<Integer> issues) {
        this.issues = issues;
    }

    public String getArea() {
        return area;
    }

    public void setArea(String area) {
        this.area = area;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getSummary() {
        return summary;
    }

    public void setSummary(String summary) {
        this.summary = summary;
    }

    public Highlight getHighlight() {
        return highlight;
    }

    public void setHighlight(Highlight highlight) {
        this.highlight = highlight;
        if (this.highlight != null) this.highlight.pr = this.pr;
    }

    public Breaking getBreaking() {
        return breaking;
    }

    public void setBreaking(Breaking breaking) {
        this.breaking = breaking;
    }

    public Deprecation getDeprecation() {
        return deprecation;
    }

    public void setDeprecation(Deprecation deprecation) {
        this.deprecation = deprecation;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ChangelogEntry that = (ChangelogEntry) o;
        return Objects.equals(pr, that.pr)
            && Objects.equals(issues, that.issues)
            && Objects.equals(area, that.area)
            && Objects.equals(type, that.type)
            && Objects.equals(summary, that.summary)
            && Objects.equals(highlight, that.highlight)
            && Objects.equals(breaking, that.breaking);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pr, issues, area, type, summary, highlight, breaking);
    }

    @Override
    public String toString() {
        return String.format(
            Locale.ROOT,
            "ChangelogEntry{pr=%d, issues=%s, area='%s', type='%s', summary='%s', highlight=%s, breaking=%s, deprecation=%s}",
            pr,
            issues,
            area,
            type,
            summary,
            highlight,
            breaking,
            deprecation
        );
    }

    public static class Highlight {
        private boolean notable;
        private String title;
        private String body;
        private Integer pr;

        public boolean isNotable() {
            return notable;
        }

        public void setNotable(boolean notable) {
            this.notable = notable;
        }

        public String getTitle() {
            return title;
        }

        public void setTitle(String title) {
            this.title = title;
        }

        public String getBody() {
            return body;
        }

        public void setBody(String body) {
            this.body = body;
        }

        public String getAnchor() {
            return generatedAnchor(this.title);
        }

        public Integer getPr() {
            return pr;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Highlight highlight = (Highlight) o;
            return Objects.equals(notable, highlight.notable)
                && Objects.equals(title, highlight.title)
                && Objects.equals(body, highlight.body);
        }

        @Override
        public int hashCode() {
            return Objects.hash(notable, title, body);
        }

        @Override
        public String toString() {
            return String.format(Locale.ROOT, "Highlight{notable=%s, title='%s', body='%s'}", notable, title, body);
        }
    }

    public static class Breaking extends CompatibilityChange {}

    public static class Deprecation extends CompatibilityChange {}

    abstract static class CompatibilityChange {
        private String area;
        private String title;
        private String details;
        private String impact;
        private boolean notable;
        private boolean essSettingChange;

        public String getArea() {
            return area;
        }

        public void setArea(String area) {
            this.area = area;
        }

        public String getTitle() {
            return title;
        }

        public void setTitle(String title) {
            this.title = title;
        }

        public String getDetails() {
            return details;
        }

        public void setDetails(String details) {
            this.details = details;
        }

        public String getImpact() {
            return impact;
        }

        public void setImpact(String impact) {
            this.impact = impact;
        }

        public boolean isNotable() {
            return notable;
        }

        public void setNotable(boolean notable) {
            this.notable = notable;
        }

        public String getAnchor() {
            return generatedAnchor(this.title);
        }

        public boolean isEssSettingChange() {
            return essSettingChange;
        }

        public void setEssSettingChange(boolean essSettingChange) {
            this.essSettingChange = essSettingChange;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CompatibilityChange breaking = (CompatibilityChange) o;
            return notable == breaking.isNotable()
                && Objects.equals(area, breaking.getArea())
                && Objects.equals(title, breaking.getTitle())
                && Objects.equals(details, breaking.getDetails())
                && Objects.equals(impact, breaking.getImpact())
                && Objects.equals(essSettingChange, breaking.isEssSettingChange());
        }

        @Override
        public int hashCode() {
            return Objects.hash(area, title, details, impact, notable, essSettingChange);
        }

        @Override
        public String toString() {
            return String.format(
                "%s{area='%s', title='%s', details='%s', impact='%s', notable=%s, essSettingChange=%s}",
                this.getClass().getSimpleName(),
                area,
                title,
                details,
                impact,
                notable,
                essSettingChange
            );
        }
    }

    private static String generatedAnchor(String input) {
        final List<String> excludes = List.of("the", "is", "a", "and", "now", "that");

        final String[] words = input.toLowerCase(Locale.ROOT)
            .replaceAll("'", "")
            .replaceAll("[^\\w]+", "_")
            .replaceFirst("^_+", "")
            .replaceFirst("_+$", "")
            .split("_+");
        return Arrays.stream(words).filter(word -> excludes.contains(word) == false).collect(Collectors.joining("_"));
    }
}

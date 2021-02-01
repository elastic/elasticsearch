/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.indices;

import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.util.Objects;

/**
 * A system index descriptor describes one or more system indices. It can match a number of indices using
 * a pattern. For system indices that are managed externally to Elasticsearch, this is enough. For system
 * indices that are managed internally to Elasticsearch, a descriptor can also include information for
 * creating the system index, upgrading its mappings, and creating an alias.
 */
public class SystemIndexDescriptor {
    /** A pattern, either with a wildcard or simple regex. Indices that match one of these patterns are considered system indices. */
    private final String indexPattern;

    /**
     * For internally-managed indices, specifies the name of the concrete index to create and update. This is required
     * since the {@link #indexPattern} can match many indices.
     */
    private final String primaryIndex;

    /** A description of the index or indices */
    private final String description;

    /** Used to determine whether an index name matches the {@link #indexPattern} */
    private final CharacterRunAutomaton indexPatternAutomaton;

    /** For internally-managed indices, contains the index mappings JSON */
    private final String mappings;

    /** For internally-managed indices, contains the index settings */
    private final Settings settings;

    /** For internally-managed indices, an optional alias to create */
    private final String aliasName;

    /** For internally-managed indices, an optional {@link IndexMetadata#INDEX_FORMAT_SETTING} value to expect */
    private final int indexFormat;

    /**
     * For internally-managed indices, specifies a key name under <code>_meta</code> in the index mappings
     * that contains the index's mappings' version.
     */
    private final String versionMetaKey;

    /** For internally-managed indices, specifies the origin to use when creating or updating the index */
    private final String origin;

    /**
     * Creates a descriptor for system indices matching the supplied pattern. These indices will not be managed
     * by Elasticsearch internally.
     * @param indexPattern The pattern of index names that this descriptor will be used for. Must start with a '.' character.
     * @param description The name of the plugin responsible for this system index.
     */
    public SystemIndexDescriptor(String indexPattern, String description) {
        this(indexPattern, null, description, null, null, null, 0, null, null);
    }

    /**
     * Creates a descriptor for system indices matching the supplied pattern. These indices will be managed
     * by Elasticsearch internally if mappings or settings are provided.
     *
     * @param indexPattern The pattern of index names that this descriptor will be used for. Must start with a '.' character.
     * @param description The name of the plugin responsible for this system index.
     * @param mappings The mappings to apply to this index when auto-creating, if appropriate
     * @param settings The settings to apply to this index when auto-creating, if appropriate
     * @param aliasName An alias for the index, or null
     * @param indexFormat A value for the `index.format` setting. Pass 0 or higher.
     * @param versionMetaKey a mapping key under <code>_meta</code> where a version can be found, which indicates the
     *                       Elasticsearch version when the index was created.
     * @param origin the client origin to use when creating this index.
     */
    SystemIndexDescriptor(
        String indexPattern,
        String primaryIndex,
        String description,
        String mappings,
        Settings settings,
        String aliasName,
        int indexFormat,
        String versionMetaKey,
        String origin
    ) {
        Objects.requireNonNull(indexPattern, "system index pattern must not be null");
        if (indexPattern.length() < 2) {
            throw new IllegalArgumentException(
                "system index pattern provided as [" + indexPattern + "] but must at least 2 characters in length"
            );
        }
        if (indexPattern.charAt(0) != '.') {
            throw new IllegalArgumentException(
                "system index pattern provided as [" + indexPattern + "] but must start with the character [.]"
            );
        }
        if (indexPattern.charAt(1) == '*') {
            throw new IllegalArgumentException(
                "system index pattern provided as ["
                    + indexPattern
                    + "] but must not start with the character sequence [.*] to prevent conflicts"
            );
        }

        if (primaryIndex != null) {
            if (primaryIndex.charAt(0) != '.') {
                throw new IllegalArgumentException(
                    "system primary index provided as [" + primaryIndex + "] but must start with the character [.]"
                );
            }
            if (primaryIndex.matches("^\\.[\\w-]+$") == false) {
                throw new IllegalArgumentException(
                    "system primary index provided as [" + primaryIndex + "] but cannot contain special characters or patterns"
                );
            }
        }

        if (indexFormat < 0) {
            throw new IllegalArgumentException("Index format cannot be negative");
        }

        Strings.requireNonEmpty(indexPattern, "indexPattern must be supplied");

        if (mappings != null || settings != null) {
            Strings.requireNonEmpty(primaryIndex, "Must supply primaryIndex if mappings or settings are defined");
            Strings.requireNonEmpty(versionMetaKey, "Must supply versionMetaKey if mappings or settings are defined");
            Strings.requireNonEmpty(origin, "Must supply origin if mappings or settings are defined");
        }

        this.indexPattern = indexPattern;
        this.primaryIndex = primaryIndex;

        final Automaton automaton = buildAutomaton(indexPattern, aliasName);
        this.indexPatternAutomaton = new CharacterRunAutomaton(automaton);

        this.description = description;
        this.mappings = mappings;
        this.settings = settings;
        this.aliasName = aliasName;
        this.indexFormat = indexFormat;
        this.versionMetaKey = versionMetaKey;
        this.origin = origin;
    }

    /**
     * @return The pattern of index names that this descriptor will be used for.
     */
    public String getIndexPattern() {
        return indexPattern;
    }

    /**
     * @return The concrete name of an index being managed internally to Elasticsearch. Will be {@code null}
     * for indices managed externally to Elasticsearch.
     */
    public String getPrimaryIndex() {
        return primaryIndex;
    }

    /**
     * Checks whether an index name matches the system index name pattern for this descriptor.
     * @param index The index name to be checked against the index pattern given at construction time.
     * @return True if the name matches the pattern, false otherwise.
     */
    public boolean matchesIndexPattern(String index) {
        return indexPatternAutomaton.run(index);
    }

    /**
     * @return A short description of the purpose of this system index.
     */
    public String getDescription() {
        return description;
    }

    @Override
    public String toString() {
        return "SystemIndexDescriptor[pattern=[" + indexPattern + "], description=[" + description + "], aliasName=[" + aliasName + "]]";
    }

    public String getMappings() {
        return mappings;
    }

    public Settings getSettings() {
        return settings;
    }

    public String getAliasName() {
        return aliasName;
    }

    public int getIndexFormat() {
        return this.indexFormat;
    }

    public String getVersionMetaKey() {
        return this.versionMetaKey;
    }

    public boolean isAutomaticallyManaged() {
        return this.mappings != null || this.settings != null;
    }

    public String getOrigin() {
        return this.origin;
    }

    // TODO: getThreadpool()
    // TODO: Upgrade handling (reindex script?)

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String indexPattern;
        private String primaryIndex;
        private String description;
        private XContentBuilder mappingsBuilder = null;
        private Settings settings = null;
        private String aliasName = null;
        private int indexFormat = 0;
        private String versionMetaKey = null;
        private String origin = null;

        private Builder() {}

        public Builder setIndexPattern(String indexPattern) {
            this.indexPattern = indexPattern;
            return this;
        }

        public Builder setPrimaryIndex(String primaryIndex) {
            this.primaryIndex = primaryIndex;
            return this;
        }

        public Builder setDescription(String description) {
            this.description = description;
            return this;
        }

        public Builder setMappings(XContentBuilder mappingsBuilder) {
            this.mappingsBuilder = mappingsBuilder;
            return this;
        }

        public Builder setSettings(Settings settings) {
            this.settings = settings;
            return this;
        }

        public Builder setAliasName(String aliasName) {
            this.aliasName = aliasName;
            return this;
        }

        public Builder setIndexFormat(int indexFormat) {
            this.indexFormat = indexFormat;
            return this;
        }

        public Builder setVersionMetaKey(String versionMetaKey) {
            this.versionMetaKey = versionMetaKey;
            return this;
        }

        public Builder setOrigin(String origin) {
            this.origin = origin;
            return this;
        }

        public SystemIndexDescriptor build() {
            String mappings = mappingsBuilder == null ? null : Strings.toString(mappingsBuilder);

            return new SystemIndexDescriptor(
                indexPattern,
                primaryIndex,
                description,
                mappings,
                settings,
                aliasName,
                indexFormat,
                versionMetaKey,
                origin
            );
        }
    }

    /**
     * Builds an automaton for matching index names against this descriptor's index pattern.
     * If this descriptor has an alias name, the automaton will also try to match against
     * the alias as well.
     */
    static Automaton buildAutomaton(String pattern, String alias) {
        final String patternAsRegex = patternToRegex(pattern);
        final String aliasAsRegex = alias == null ? null : patternToRegex(alias);

        final Automaton patternAutomaton = new RegExp(patternAsRegex).toAutomaton();

        if (aliasAsRegex == null) {
            return patternAutomaton;
        }

        final Automaton aliasAutomaton = new RegExp(aliasAsRegex).toAutomaton();

        return Operations.union(patternAutomaton, aliasAutomaton);
    }

    /**
     * Translate a simple string pattern into a regular expression, suitable for creating a
     * {@link RegExp} instance. This exists because although
     * {@link org.elasticsearch.common.regex.Regex#simpleMatchToAutomaton(String)} is useful
     * for simple patterns, it doesn't support character ranges.
     * @param input the string to translate
     * @return the translate string
     */
    private static String patternToRegex(String input) {
        String output = input;
        output = output.replaceAll("\\.", "\\.");
        output = output.replaceAll("\\*", ".*");
        return output;
    }
}

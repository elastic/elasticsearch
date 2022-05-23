/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.RegExp;
import org.elasticsearch.cluster.metadata.Metadata;

import java.util.List;
import java.util.Objects;

/**
 * An "associated index" is an index that is related to or derived from a system
 * index, but should not be considered a system index, usually because it is
 * meant to be visible to users. However, if it goes out of sync with its
 * related system indices, it may become unreliable or useless. Hence, when
 * taking a snapshot of a feature, we want the associated index to be included
 * in the snapshot, and, likewise, we want an associated index to be restored
 * when a feature snapshot is restored.
 */
public class AssociatedIndexDescriptor implements IndexPatternMatcher {
    /** A pattern, either with a wildcard or simple regex.*/
    private final String indexPattern;

    /** A description of the index or indices */
    private final String description;

    /** Used to determine whether an index name matches the {@link #indexPattern} */
    private final CharacterRunAutomaton indexPatternAutomaton;

    /**
     * Create a descriptor for an index associated with a feature
     * @param indexPattern Pattern for associated indices, which can include wildcards
     *                     or regex-like character classes
     * @param description A short description of the purpose of this index
     */
    public AssociatedIndexDescriptor(String indexPattern, String description) {
        Objects.requireNonNull(indexPattern, "associated index pattern must not be null");
        if (indexPattern.length() < 2) {
            throw new IllegalArgumentException(
                "associated index pattern provided as [" + indexPattern + "] but must at least 2 characters in length"
            );
        }
        if (indexPattern.charAt(0) != '.') {
            throw new IllegalArgumentException(
                "associated index pattern provided as [" + indexPattern + "] but must start with the character [.]"
            );
        }
        if (indexPattern.charAt(1) == '*') {
            throw new IllegalArgumentException(
                "associated index pattern provided as ["
                    + indexPattern
                    + "] but must not start with the character sequence [.*] to prevent conflicts"
            );
        }

        this.indexPattern = indexPattern;

        final Automaton automaton = buildAutomaton(indexPattern);
        this.indexPatternAutomaton = new CharacterRunAutomaton(automaton);

        this.description = description;
    }

    /**
     * @return The pattern of index names that this descriptor will be used for.
     */
    @Override
    public String getIndexPattern() {
        return indexPattern;
    }

    /**
     * @return A short description of the purpose of this system index.
     */
    public String getDescription() {
        return description;
    }

    /**
     * Builds an automaton for matching index names against this descriptor's index pattern.
     */
    static Automaton buildAutomaton(String pattern) {
        String output = pattern;
        output = output.replaceAll("\\.", "\\\\.");
        output = output.replaceAll("\\*", ".*");
        return new RegExp(output).toAutomaton();
    }

    /**
     * Retrieves a list of all indices which match this descriptor's pattern.
     *
     * This cannot be done via {@link org.elasticsearch.cluster.metadata.IndexNameExpressionResolver} because that class can only handle
     * simple wildcard expressions, but system index name patterns may use full Lucene regular expression syntax,
     *
     * @param metadata The current metadata to get the list of matching indices from
     * @return A list of index names that match this descriptor
     */
    @Override
    public List<String> getMatchingIndices(Metadata metadata) {
        return metadata.indices().keySet().stream().filter(this::matchesIndexPattern).toList();
    }

    /**
     * Checks whether an index name matches the system index name pattern for this descriptor.
     * @param index The index name to be checked against the index pattern given at construction time.
     * @return True if the name matches the pattern, false otherwise.
     */
    private boolean matchesIndexPattern(String index) {
        return indexPatternAutomaton.run(index);
    }

    @Override
    public String toString() {
        return "AssociatedIndexDescriptor{"
            + "indexPattern='"
            + indexPattern
            + '\''
            + ", description='"
            + description
            + '\''
            + ", indexPatternAutomaton="
            + indexPatternAutomaton
            + '}';
    }
}

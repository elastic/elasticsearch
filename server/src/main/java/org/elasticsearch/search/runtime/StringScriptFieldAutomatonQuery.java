/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.runtime;

import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.StringFieldScript;

import java.util.Objects;

/**
 * Matches the values of a runtime {@code keyword} against an automaton built by the caller, as an
 * {@link org.elasticsearch.index.query.AutomatonQueryWithDescription} does on an indexed one, for callers that build
 * their automaton from a syntax no other query parses, such as ES|QL's RE2 label matchers.
 */
public class StringScriptFieldAutomatonQuery extends AbstractStringScriptFieldAutomatonQuery {
    private final ByteRunAutomaton automaton;
    private final String description;

    /**
     * @param automaton a determinized automaton over the UTF-32 code points of the values
     * @param description what {@link #toString(String)} prints for the automaton
     */
    public StringScriptFieldAutomatonQuery(
        Script script,
        StringFieldScript.LeafFactory leafFactory,
        String fieldName,
        Automaton automaton,
        String description
    ) {
        this(script, leafFactory, fieldName, new ByteRunAutomaton(automaton), description);
    }

    private StringScriptFieldAutomatonQuery(
        Script script,
        StringFieldScript.LeafFactory leafFactory,
        String fieldName,
        ByteRunAutomaton automaton,
        String description
    ) {
        super(script, leafFactory, fieldName, automaton);
        this.automaton = automaton;
        this.description = Objects.requireNonNull(description);
    }

    @Override
    protected long ownMemoryUsage() {
        return RamUsageEstimator.sizeOf(description);
    }

    @Override
    public final String toString(String field) {
        if (fieldName().contentEquals(field)) {
            return description;
        }
        return fieldName() + ":" + description;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), automaton, description);
    }

    @Override
    public boolean equals(Object obj) {
        if (false == super.equals(obj)) {
            return false;
        }
        StringScriptFieldAutomatonQuery other = (StringScriptFieldAutomatonQuery) obj;
        return automaton.equals(other.automaton) && description.equals(other.description);
    }

    String description() {
        return description;
    }
}

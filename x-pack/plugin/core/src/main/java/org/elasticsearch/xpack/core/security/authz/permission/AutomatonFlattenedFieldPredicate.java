/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.permission;

import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.elasticsearch.plugins.FieldPredicate;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.FieldSubsetReader;

import java.util.Set;

/**
 * An {@link AutomatonFieldPredicate} applied to one index's flattened fields. A flattened field is listed when the role sees its name or
 * any key below it, and a mapped sub-field below it when the role sees that key. Every other path is answered as the role predicate does.
 */
class AutomatonFlattenedFieldPredicate implements FieldPredicate {
    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(AutomatonFlattenedFieldPredicate.class);

    private final Set<String> flattenedFields;
    private final CharacterRunAutomaton automaton;
    private final String flattenedHash;

    AutomatonFlattenedFieldPredicate(Set<String> flattenedFields, Automaton automaton) {
        this.flattenedFields = flattenedFields;
        this.automaton = new CharacterRunAutomaton(automaton);
        this.flattenedHash = AutomatonFieldPredicate.sha256(automaton);
    }

    @Override
    public boolean test(String path) {
        // The automaton is minimized without dead states, so reaching a state through "field." means some path below the field is accepted.
        return automaton.run(path) || (flattenedFields.contains(path) && FieldSubsetReader.step(automaton, path + ".", 0) != -1);
    }

    @Override
    public String modifyHash(String hash) {
        // Two roles with the same merged automaton can still differ on a flattened field, so the role hash would let field_caps serve one
        // role's cached response to the other. What this predicate lists is decided by the flattened automaton alone, so hash that instead.
        return hash + ":" + flattenedHash;
    }

    @Override
    public long ramBytesUsed() {
        return SHALLOW_SIZE + automaton.ramBytesUsed(); // the field set is shared with the mapping so not counted here
    }
}

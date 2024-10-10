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
import org.apache.lucene.util.automaton.Transition;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.plugins.FieldPredicate;

import java.io.IOException;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.Base64;

/**
 * An implementation of {@link FieldPredicate} which matches fields
 * against an {@link Automaton}.
 */
class AutomatonFieldPredicate implements FieldPredicate {
    private final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(AutomatonFieldPredicate.class);

    private final String automatonHash;
    private final CharacterRunAutomaton automaton;

    AutomatonFieldPredicate(Automaton originalAutomaton, CharacterRunAutomaton automaton) {
        this.automatonHash = sha256(originalAutomaton);
        this.automaton = automaton;
    }

    @Override
    public boolean test(String field) {
        return automaton.run(field);
    }

    @Override
    public String modifyHash(String hash) {
        return hash + ":" + automatonHash;
    }

    @Override
    public long ramBytesUsed() {
        return SHALLOW_SIZE + RamUsageEstimator.sizeOf(automatonHash); // automaton itself is a shallow copy so not counted here
    }

    private static String sha256(Automaton automaton) {
        MessageDigest messageDigest = MessageDigests.sha256();
        try {
            StreamOutput out = new OutputStreamStreamOutput(new DigestOutputStream(Streams.NULL_OUTPUT_STREAM, messageDigest));
            Transition t = new Transition();
            for (int state = 0; state < automaton.getNumStates(); state++) {
                out.writeInt(state);
                out.writeBoolean(automaton.isAccept(state));

                int numTransitions = automaton.initTransition(state, t);
                for (int i = 0; i < numTransitions; ++i) {
                    automaton.getNextTransition(t);
                    out.writeInt(t.dest);
                    out.writeInt(t.min);
                    out.writeInt(t.max);
                }
            }
        } catch (IOException bogus) {
            // cannot happen
            throw new Error(bogus);
        }
        return Base64.getEncoder().encodeToString(messageDigest.digest());
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.eql.execution.assembler;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xpack.eql.execution.search.Limit;
import org.elasticsearch.xpack.eql.execution.search.Ordinal;
import org.elasticsearch.xpack.eql.execution.sequence.Sequence;
import org.elasticsearch.xpack.eql.execution.sequence.SequenceStateMachine;
import org.elasticsearch.xpack.eql.session.Payload;

import java.util.List;

/**
 * Executable tracking sequences at runtime.
 */
class Matcher {

    // NB: just like in a list, this represents the total number of stages yet counting starts at 0
    private final SequenceStateMachine stateMachine;
    private final int numberOfStages;

    Matcher(int numberOfStages, TimeValue maxSpan, Limit limit) {
        this.numberOfStages = numberOfStages;
        this.stateMachine = new SequenceStateMachine(numberOfStages, maxSpan, limit);
    }

    /**
     * Match hits for the given stage.
     * Returns false if the process needs to be stopped.
     */
    boolean match(int stage, Iterable<Tuple<KeyAndOrdinal, SearchHit>> hits) {
        for (Tuple<KeyAndOrdinal, SearchHit> tuple : hits) {
            KeyAndOrdinal ko = tuple.v1();
            SearchHit hit = tuple.v2();

            if (stage == 0) {
                Sequence seq = new Sequence(ko.key, numberOfStages, ko.ordinal, hit);
                stateMachine.trackSequence(seq);
            } else {
                stateMachine.match(stage, ko.key, ko.ordinal, hit);

                // early skip in case of reaching the limit
                // check the last stage to avoid calling the state machine in other stages
                if (stateMachine.reachedLimit()) {
                    return false;
                }
            }
        }
        return true;
    }

    boolean until(Iterable<Ordinal> markers) {
        // no-op so far

        return false;
    }


    public boolean hasCandidates(int stage) {
        return stateMachine.hasCandidates(stage);
    }

    Payload payload(long startTime) {
        List<Sequence> completed = stateMachine.completeSequences();
        TimeValue tookTime = new TimeValue(System.currentTimeMillis() - startTime);
        return new SequencePayload(completed, false, tookTime);
    }
}
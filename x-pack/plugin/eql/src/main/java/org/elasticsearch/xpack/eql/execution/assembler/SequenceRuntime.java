/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.execution.assembler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xpack.eql.execution.search.Limit;
import org.elasticsearch.xpack.eql.execution.search.QueryClient;
import org.elasticsearch.xpack.eql.execution.sequence.Ordinal;
import org.elasticsearch.xpack.eql.execution.sequence.Sequence;
import org.elasticsearch.xpack.eql.execution.sequence.SequenceKey;
import org.elasticsearch.xpack.eql.execution.sequence.SequenceStateMachine;
import org.elasticsearch.xpack.eql.session.Payload;
import org.elasticsearch.xpack.ql.execution.search.extractor.HitExtractor;

import java.util.List;

import static org.elasticsearch.action.ActionListener.wrap;

/**
 * Executable tracking sequences at runtime.
 */
class SequenceRuntime implements Executable {

    private final Logger log = LogManager.getLogger(SequenceRuntime.class);

    private final List<Criterion> criteria;
    // NB: just like in a list, this represents the total number of stages yet counting starts at 0
    private final int numberOfStages;
    private final SequenceStateMachine stateMachine;
    private final QueryClient queryClient;

    private long startTime;

    SequenceRuntime(List<Criterion> criteria, QueryClient queryClient, TimeValue maxSpan, Limit limit) {
        this.criteria = criteria;
        this.numberOfStages = criteria.size();
        this.queryClient = queryClient;
        this.stateMachine = new SequenceStateMachine(numberOfStages, maxSpan, limit);
    }

    @Override
    public void execute(ActionListener<Payload> listener) {
        startTime = System.currentTimeMillis();
        log.info("Starting sequencing");
        queryStage(0, listener);
    }

    private void queryStage(int stage, ActionListener<Payload> listener) {
        // sequencing is done, return results
        if (hasFinished(stage)) {
            listener.onResponse(sequencePayload());
            return;
        }

        // else continue finding matches
        Criterion currentCriterion = criteria.get(stage);
        if (stage > 0) {
            // FIXME: revisit this during pagination since the second criterion need to be limited to the range of the first one
            // narrow by the previous stage timestamp marker

            Criterion previous = criteria.get(stage - 1);
            // pass the next marker along
            currentCriterion.useMarker(previous.nextMarker());
        }
        
        log.info("Querying stage {}", stage);
        queryClient.query(currentCriterion, wrap(payload -> {
            List<SearchHit> hits = payload.values();

            // nothing matches the query -> bail out
            // FIXME: needs to be changed when doing pagination
            if (hits.isEmpty()) {
                listener.onResponse(sequencePayload());
                return;
            }

            findMatches(stage, hits);
            queryStage(stage + 1, listener);
        }, listener::onFailure));
    }

    // hits are guaranteed to be non-empty
    private void findMatches(int stage, List<SearchHit> hits) {
        // update criterion
        Criterion criterion = criteria.get(stage);

        // break the results per key
        // when dealing with descending order, queries outside the base are ASC (search_before)
        // so look at the data in reverse (that is DESC)
        Ordinal firstOrdinal = null, ordinal = null;
        for (SearchHit hit : criterion.iterable(hits)) {
            KeyAndOrdinal ko = key(hit, criterion);

            ordinal = ko.ordinal;

            if (firstOrdinal == null) {
                firstOrdinal = ordinal;
            }

            if (stage == 0) {
                Sequence seq = new Sequence(ko.key, numberOfStages, ordinal, hit);
                stateMachine.trackSequence(seq);
            } else {
                stateMachine.match(stage, ko.key, ordinal, hit);

                // early skip in case of reaching the limit
                // check the last stage to avoid calling the state machine in other stages
                if (stateMachine.reachedLimit()) {
                    break;
                }
            }
        }

        criterion.startMarker(firstOrdinal);
        criterion.stopMarker(ordinal);
    }

    private KeyAndOrdinal key(SearchHit hit, Criterion criterion) {
        List<HitExtractor> keyExtractors = criterion.keyExtractors();

        SequenceKey key;
        if (criterion.keyExtractors().isEmpty()) {
            key = SequenceKey.NONE;
        } else {
            Object[] docKeys = new Object[keyExtractors.size()];
            for (int i = 0; i < docKeys.length; i++) {
                docKeys[i] = keyExtractors.get(i).extract(hit);
            }
            key = new SequenceKey(docKeys);
        }

        return new KeyAndOrdinal(key, criterion.ordinal(hit));
    }

    private Payload sequencePayload() {
        List<Sequence> completed = stateMachine.completeSequences();
        TimeValue tookTime = new TimeValue(System.currentTimeMillis() - startTime);
        return new SequencePayload(completed, false, tookTime);
    }

    private boolean hasFinished(int stage) {
        return stage == numberOfStages;
    }
}
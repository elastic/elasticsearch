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
import org.elasticsearch.xpack.eql.execution.payload.ReversePayload;
import org.elasticsearch.xpack.eql.execution.search.Limit;
import org.elasticsearch.xpack.eql.execution.search.QueryClient;
import org.elasticsearch.xpack.eql.execution.sequence.Sequence;
import org.elasticsearch.xpack.eql.execution.sequence.SequenceKey;
import org.elasticsearch.xpack.eql.execution.sequence.SequenceStateMachine;
import org.elasticsearch.xpack.eql.session.Payload;
import org.elasticsearch.xpack.eql.util.ReversedIterator;
import org.elasticsearch.xpack.ql.execution.search.extractor.HitExtractor;

import java.util.Iterator;
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
    private final boolean descending;

    private long startTime;

    SequenceRuntime(List<Criterion> criteria, QueryClient queryClient, boolean descending, Limit limit) {
        this.criteria = criteria;
        this.numberOfStages = criteria.size();
        this.queryClient = queryClient;
        boolean hasTiebreaker = criteria.get(0).tiebreakerExtractor() != null;
        this.stateMachine = new SequenceStateMachine(numberOfStages, hasTiebreaker, limit);

        this.descending = descending;
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
            // if DESC, flip the markers (the stop becomes the start due to the reverse order), otherwise keep it accordingly
            Object[] marker = descending && stage == 1 ? previous.stopMarker() : previous.startMarker();
            currentCriterion.useMarker(marker);
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
    private void findMatches(int currentStage, List<SearchHit> hits) {
        // update criterion
        Criterion criterion = criteria.get(currentStage);
        criterion.startMarker(hits.get(0));
        criterion.stopMarker(hits.get(hits.size() - 1));

        // break the results per key
        // when dealing with descending order, queries outside the base are ASC (search_before)
        // so look at the data in reverse (that is DESC)
        for (Iterator<SearchHit> it = descending ? new ReversedIterator<>(hits) : hits.iterator(); it.hasNext();) {
            SearchHit hit = it.next();

            KeyAndOrdinal ko = key(hit, criterion);
            if (currentStage == 0) {
                Sequence seq = new Sequence(ko.key, numberOfStages, ko.timestamp, ko.tiebreaker, hit);
                long tStart = (long) criterion.startMarker()[0];
                long tStop = (long) criterion.stopMarker()[0];
                stateMachine.trackSequence(seq, tStart, tStop);
            } else {
                stateMachine.match(currentStage, ko.key, ko.timestamp, ko.tiebreaker, hit);

                // early skip in case of reaching the limit
                // check the last stage to avoid calling the state machine in other stages
                if (stateMachine.reachedLimit()) {
                    return;
                }
            }
        }
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

        return new KeyAndOrdinal(key, criterion.timestamp(hit), criterion.tiebreaker(hit));
    }

    private Payload sequencePayload() {
        List<Sequence> completed = stateMachine.completeSequences();
        TimeValue tookTime = new TimeValue(System.currentTimeMillis() - startTime);
        SequencePayload payload = new SequencePayload(completed, false, tookTime, null);
        return descending ? new ReversePayload(payload) : payload;
    }

    private boolean hasFinished(int stage) {
        return stage == numberOfStages;
    }
}
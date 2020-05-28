/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.execution.assembler;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xpack.eql.execution.payload.Payload;
import org.elasticsearch.xpack.eql.execution.sequence.Sequence;
import org.elasticsearch.xpack.eql.execution.sequence.SequenceKey;
import org.elasticsearch.xpack.eql.execution.sequence.SequenceStateMachine;
import org.elasticsearch.xpack.eql.session.Results;
import org.elasticsearch.xpack.ql.execution.search.extractor.HitExtractor;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.action.ActionListener.wrap;

/**
 * Executable tracking sequences at runtime.
 */
class SequenceRuntime implements Executable {

    private final List<Criterion> criteria;
    // NB: just like in a list, this represents the total number of stages yet counting starts at 0
    private final int numberOfStages;
    private final SequenceStateMachine stateMachine;
    private final QueryClient queryClient;
    private long startTime;

    SequenceRuntime(List<Criterion> criteria, QueryClient queryClient) {
        this.criteria = criteria;
        this.numberOfStages = criteria.size();
        this.stateMachine = new SequenceStateMachine(numberOfStages);
        this.queryClient = queryClient;
    }

    @Override
    public void execute(ActionListener<Results> resultsListener) {
        startTime = System.currentTimeMillis();
        startSequencing(resultsListener);
    }

    private void startSequencing(ActionListener<Results> resultsListener) {
        Criterion firstStage = criteria.get(0);
        queryClient.query(firstStage.searchSource(), wrap(payload -> {

            // 1. execute last stage (find keys)
            startTracking(payload);

            // 2. go descending through the rest of the stages, while adjusting the query
            inspectStage(1, resultsListener);

        }, resultsListener::onFailure));
    }

    private void startTracking(Payload<SearchHit> payload) {
        Criterion lastCriterion = criteria.get(0);
        List<SearchHit> hits = payload.values();

        long tMin = Long.MAX_VALUE;
        long tMax = Long.MIN_VALUE;
        // we could have extracted that in the hit loop but that if would have been evaluated
        // for every document
        if (hits.isEmpty() == false) {
            tMin = (Long) lastCriterion.timestampExtractor().extract(hits.get(0));
            tMax = (Long) lastCriterion.timestampExtractor().extract(hits.get(hits.size() - 1));
        }

        for (SearchHit hit : hits) {
            KeyWithTime keyAndTime = findKey(hit, lastCriterion);
            Sequence seq = new Sequence(keyAndTime.key, numberOfStages, keyAndTime.timestamp, hit);
            stateMachine.trackSequence(seq, tMin, tMax);
        }
        // TB: change
        stateMachine.setTimestampMarker(0, tMin);
    }

    private void inspectStage(int stage, ActionListener<Results> resultsListener) {
        // sequencing is done, return results
        if (stage == numberOfStages) {
            resultsListener.onResponse(assembleResults());
            return;
        }
        // else continue finding matches
        Criterion currentCriterion = criteria.get(stage);
        // narrow by the previous stage timestamp marker
        currentCriterion.fromTimestamp(stateMachine.getTimestampMarker(stage - 1));
        
        queryClient.query(currentCriterion.searchSource(), wrap(payload -> {
            findMatches(stage, payload);
            inspectStage(stage + 1, resultsListener);
        }, resultsListener::onFailure));
    }

    private void findMatches(int currentStage, Payload<SearchHit> payload) {
        Criterion currentCriterion = criteria.get(currentStage);
        List<SearchHit> hits = payload.values();
        
        // break the results per key
        for (SearchHit hit : hits) {
            KeyWithTime kt = findKey(hit, currentCriterion);
            stateMachine.match(currentStage, kt.key, kt.timestamp, hit);
        }
    }

    private KeyWithTime findKey(SearchHit hit, Criterion criterion) {
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

        return new KeyWithTime(key, (Long) criterion.timestampExtractor().extract(hit));
    }

    private Results assembleResults() {
        List<Sequence> done = stateMachine.completeSequences();
        List<org.elasticsearch.xpack.eql.action.EqlSearchResponse.Sequence> response = new ArrayList<>(done.size());
        for (Sequence s : done) {
            response.add(new org.elasticsearch.xpack.eql.action.EqlSearchResponse.Sequence(s.key().asStringList(), s.hits()));
        }
        
        TimeValue tookTime = new TimeValue(System.currentTimeMillis() - startTime);
        return Results.fromSequences(tookTime, response);
    }
}
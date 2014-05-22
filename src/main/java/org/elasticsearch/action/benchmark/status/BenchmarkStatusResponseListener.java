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

package org.elasticsearch.action.benchmark.status;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.benchmark.competition.CompetitionResult;
import org.elasticsearch.action.benchmark.start.BenchmarkStartResponse;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.util.concurrent.CountDown;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 *
 */
public class BenchmarkStatusResponseListener implements ActionListener<BenchmarkStartResponse> {

    private static final ESLogger logger = ESLoggerFactory.getLogger(BenchmarkStatusResponseListener.class.getName());

    private final CountDown                    countdown;
    private final CountDownLatch               complete = new CountDownLatch(1);
    private final List<BenchmarkStartResponse> responses = Collections.synchronizedList(new ArrayList<BenchmarkStartResponse>());
    private BenchmarkStartResponse             response;

    public BenchmarkStatusResponseListener(int count) {
        this.countdown = new CountDown(count);
    }

    @Override
    public void onResponse(BenchmarkStartResponse benchmarkStartResponse) {
        responses.add(benchmarkStartResponse);
        if (countdown.countDown()) {
            processResponses();
        }
    }

    @Override
    public void onFailure(Throwable e) {
        logger.debug(e.getMessage(), e);
        if (countdown.countDown()) {
            processResponses();
        }
    }

    private void processResponses() {
        try {
            synchronized (responses) {
                response = consolidate(responses);
            }
        } finally {
            complete.countDown();
        }
    }

    public boolean countdown() {
        return countdown.countDown();
    }

    public void awaitCompletion() throws InterruptedException {
        complete.await();
    }

    public List<BenchmarkStartResponse> responses() {
        return responses;
    }

    public BenchmarkStartResponse response() {
        return response;
    }

    /**
     * Merge node responses into a single consolidated response
     */
    private BenchmarkStartResponse consolidate(List<BenchmarkStartResponse> responses) {

        final BenchmarkStartResponse response = new BenchmarkStartResponse();
        final List<String> errors             = new ArrayList<>();

        for (BenchmarkStartResponse r : responses) {

            if (r.competitionResults() == null) {
                continue;
            }

            for (Map.Entry<String, CompetitionResult> entry : r.competitionResults().entrySet()) {
                if (!response.competitionResults().containsKey(entry.getKey())) {
                    response.competitionResults().put(entry.getKey(),
                            new CompetitionResult(
                                    entry.getKey(), entry.getValue().concurrency(), entry.getValue().multiplier(),
                                    false, entry.getValue().percentiles())
                    );
                }
                CompetitionResult cr = response.competitionResults().get(entry.getKey());
                cr.nodeResults().addAll(entry.getValue().nodeResults());
            }

            if (r.hasErrors()) {
                for (String error : r.errors()) {
                    errors.add(error);
                }
            }

            if (response.benchmarkId() == null) {
                response.benchmarkId(r.benchmarkId());
            }

            assert response.benchmarkId().equals(r.benchmarkId());
            if (!errors.isEmpty()) {
                response.errors(errors.toArray(new String[errors.size()]));
            }

            response.mergeState(r.state());
            assert errors.isEmpty() || response.state() != BenchmarkStartResponse.State.COMPLETED : "Response can't be complete since it has errors";
        }

        return response;
    }
}

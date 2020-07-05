/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.execution.assembler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xpack.eql.execution.search.Ordinal;
import org.elasticsearch.xpack.eql.execution.search.QueryClient;
import org.elasticsearch.xpack.eql.execution.sequence.SequenceKey;
import org.elasticsearch.xpack.eql.session.Payload;
import org.elasticsearch.xpack.eql.util.ReversedIterator;

import java.util.Iterator;
import java.util.List;

import static org.elasticsearch.action.ActionListener.wrap;

/**
 * Time-based window encapsulating query creation and advancement.
 * Since queries can return different number of results, to avoid creating incorrect sequences,
 * all searches are 'boxed' to a base query.
 * The base query is initially the first query - when no results are found, the next query gets promoted.
 * 
 * This allows the window to find any follow-up results even if they are found outside the initial window
 * of a base query.
 */
public class TumblingWindow implements Executable {

    private final Logger log = LogManager.getLogger(Matcher.class);

    private final QueryClient client;
    private final List<Criterion<BoxedQueryRequest>> criteria;
    private final Criterion<BoxedQueryRequest> until;
    private final Matcher matcher;
    // shortcut
    private final int maxStages;
    private final int windowSize;

    private long startTime;

    private static class WindowInfo {
        private final int baseStage;
        private final Ordinal begin, end;

        WindowInfo(int baseStage, Ordinal begin, Ordinal end) {
            this.baseStage = baseStage;
            this.begin = begin;
            this.end = end;
        }
    }

    public TumblingWindow(QueryClient client,
                          List<Criterion<BoxedQueryRequest>> criteria,
                          Criterion<BoxedQueryRequest> until,
                          Matcher matcher) {
        this.client = client;

        this.until = until;
        this.criteria = criteria;
        this.maxStages = criteria.size();
        this.windowSize = criteria.get(0).queryRequest().searchSource().size();

        this.matcher = matcher;
    }

    @Override
    public void execute(ActionListener<Payload> listener) {
        log.info("Starting sequence window...");
        startTime = System.currentTimeMillis();
        advance(0, listener);
    }

    private void advance(int baseStage, ActionListener<Payload> listener) {
        // initialize
        log.info("Querying base stage [{}]" + baseStage);
        Criterion<BoxedQueryRequest> base = criteria.get(baseStage);
        // remove any potential upper limit (if a criteria has been promoted)
        base.queryRequest().until(null);

        client.query(base.queryRequest(), wrap(p -> baseCriterion(baseStage, p, listener), listener::onFailure));
    }

    private void baseCriterion(int baseStage, Payload p, ActionListener<Payload> listener) {
        Criterion<BoxedQueryRequest> base = criteria.get(baseStage);
        List<SearchHit> hits = p.values();

        if (hits.isEmpty() == false) {
            if (matcher.match(baseStage, wrapValues(base, hits)) == false) {
                listener.onResponse(payload());
                return;
            }
        }
        // empty or only one result means there aren't going to be any matches
        // so move the window boxing to the next stage
        if (hits.size() < 2) {
            // if there are still candidates, advance the window base
            if (matcher.hasCandidates(baseStage) && baseStage + 1 < maxStages) {
                advance(baseStage + 1, listener);
            }
            // there aren't going to be any matches so cancel search
            else {
                listener.onResponse(payload());
            }
            return;
        }

        // get borders for the rest of the queries
        Ordinal begin = base.ordinal(hits.get(0));
        Ordinal end = base.ordinal(hits.get(hits.size() - 1));

        // update current query for the next request
        base.queryRequest().nextAfter(end);

        // find until ordinals
        //NB: not currently implemented

        // no more queries to run
        if (baseStage + 1 < maxStages) {
            secondaryCriterion(new WindowInfo(baseStage, begin, end), baseStage + 1, listener);
        } else {
            advance(baseStage, listener);
        }
    }

    private void secondaryCriterion(WindowInfo window, int currentStage, ActionListener<Payload> listener) {
        final Criterion<BoxedQueryRequest> criterion = criteria.get(currentStage);
        log.info("Querying (secondary) stage {}", criterion.stage());

        // first box the query
        final BoxedQueryRequest request = criterion.queryRequest();
        Criterion<BoxedQueryRequest> base = criteria.get(window.baseStage);

        // if the base has a different direction, swap begin/end
        // set upper limit of secondary query - the start was set on the previous run
        if (criterion.reverse() != base.reverse()) {
            request.until(window.begin);
        } else {
            request.until(window.end);
        }

        client.query(request, wrap(p -> {
            List<SearchHit> hits = p.values();

            // no more results for this query
            if (hits.isEmpty()) {
                // if there are no candidates, advance the window
                if (matcher.hasCandidates(criterion.stage()) == false) {
                    log.info("Advancing window...");
                    advance(window.baseStage, listener);
                    return;
                }
                // otherwise let the other queries run to allow potential matches with the existing candidates
            }
            else {
                // prepare the query for the next search
                request.nextAfter(criterion.ordinal(hits.get(hits.size() - 1)));

                // if the limit has been reached, return what's available
                if (matcher.match(criterion.stage(), wrapValues(criterion, hits)) == false) {
                    listener.onResponse(payload());
                    return;
                }
            }

            // keep running the query runs out of the results (essentially returns less than what we want)
            if (hits.size() == windowSize) {
                secondaryCriterion(window, currentStage, listener);
            }
            // looks like this stage is done, move on
            else {
                // to the next query
                if (currentStage + 1 < maxStages) {
                    secondaryCriterion(window, currentStage + 1, listener);
                }
                // or to the next window
                else {
                    advance(window.baseStage, listener);
                }
            }

        }, listener::onFailure));
    }

    Iterable<Tuple<KeyAndOrdinal, SearchHit>> wrapValues(Criterion<?> criterion, List<SearchHit> hits) {
        return () -> {
            final Iterator<SearchHit> iter = criterion.reverse() ? new ReversedIterator<>(hits) : hits.iterator();

            return new Iterator<>() {

                @Override
                public boolean hasNext() {
                    return iter.hasNext();
                }

                @Override
                public Tuple<KeyAndOrdinal, SearchHit> next() {
                    SearchHit hit = iter.next();
                    SequenceKey k = criterion.key(hit);
                    Ordinal o = criterion.ordinal(hit);
                    return new Tuple<>(new KeyAndOrdinal(k, o), hit);
                }
            };
        };
    }

    Payload payload() {
        return matcher.payload(startTime);
    }
}
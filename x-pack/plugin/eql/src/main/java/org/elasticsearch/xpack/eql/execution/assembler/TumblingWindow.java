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

    private long startTime;

    private int baseStage = 0;
    private Ordinal begin, end;

    public TumblingWindow(QueryClient client,
                          List<Criterion<BoxedQueryRequest>> criteria,
                          Criterion<BoxedQueryRequest> until,
                          Matcher matcher) {
        this.client = client;

        this.until = until;
        this.criteria = criteria;
        this.maxStages = criteria.size();

        this.matcher = matcher;
    }

    @Override
    public void execute(ActionListener<Payload> listener) {
        log.info("Starting sequence window...");
        startTime = System.currentTimeMillis();
        advance(listener);
    }


    private void advance(ActionListener<Payload> listener) {
        // initialize
        log.info("Querying base stage");
        Criterion<BoxedQueryRequest> base = criteria.get(baseStage);

        if (end != null) {
            // pick up where we left of
            base.queryRequest().next(end);
        }
        client.query(base.queryRequest(), wrap(p -> baseCriterion(p, listener), listener::onFailure));
    }

    private void baseCriterion(Payload p, ActionListener<Payload> listener) {
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
                // swap window begin/end when changing directions
                if (base.reverse() != criteria.get(baseStage + 1).reverse()) {
                    Ordinal temp = begin;
                    begin = end;
                    end = temp;
                }
                baseStage++;
                advance(listener);
            }
            // there aren't going to be any matches so cancel search
            else {
                listener.onResponse(payload());
            }
            return;
        }

        // get borders for the rest of the queries
        begin = base.ordinal(hits.get(0));
        end = base.ordinal(hits.get(hits.size() - 1));

        // find until ordinals
        //NB: not currently implemented

        // no more queries to run
        if (baseStage + 1 < maxStages) {
            secondaryCriterion(baseStage + 1, listener);
        } else {
            advance(listener);
        }
    }

    private void secondaryCriterion(int index, ActionListener<Payload> listener) {
        Criterion<BoxedQueryRequest> criterion = criteria.get(index);
        log.info("Querying (secondary) stage {}", criterion.stage());

        // first box the query
        BoxedQueryRequest request = criterion.queryRequest();
        Criterion<BoxedQueryRequest> base = criteria.get(baseStage);

        // if the base has a different direction, swap begin/end
        if (criterion.reverse() != base.reverse()) {
            request.between(end, begin);
        } else {
            request.between(begin, end);
        }

        client.query(request, wrap(p -> {
            List<SearchHit> hits = p.values();
            // no more results in this window so continue in another window
            if (hits.isEmpty()) {
                log.info("Advancing window...");
                advance(listener);
                return;
            }
            // if the limit has been reached, return what's available
            if (matcher.match(criterion.stage(), wrapValues(criterion, hits)) == false) {
                listener.onResponse(payload());
                return;
            }

            if (index + 1 < maxStages) {
                secondaryCriterion(index + 1, listener);
            } else {
                advance(listener);
            }

        }, listener::onFailure));
    }

    Iterable<Tuple<KeyAndOrdinal, SearchHit>> wrapValues(Criterion<?> criterion, List<SearchHit> hits) {
        return () -> {
            final Iterator<SearchHit> iter = criterion.reverse() ? new ReversedIterator<>(hits) : hits.iterator();

            return new Iterator<Tuple<KeyAndOrdinal, SearchHit>>() {

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
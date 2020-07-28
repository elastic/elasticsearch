/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.execution.sequence;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xpack.eql.execution.assembler.BoxedQueryRequest;
import org.elasticsearch.xpack.eql.execution.assembler.Criterion;
import org.elasticsearch.xpack.eql.execution.assembler.Executable;
import org.elasticsearch.xpack.eql.execution.search.HitReference;
import org.elasticsearch.xpack.eql.execution.search.Ordinal;
import org.elasticsearch.xpack.eql.execution.search.QueryClient;
import org.elasticsearch.xpack.eql.session.EmptyPayload;
import org.elasticsearch.xpack.eql.session.Payload;
import org.elasticsearch.xpack.eql.session.Results.Type;
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

    private final Logger log = LogManager.getLogger(TumblingWindow.class);

    private final QueryClient client;
    private final List<Criterion<BoxedQueryRequest>> criteria;
    private final Criterion<BoxedQueryRequest> until;
    private final SequenceMatcher matcher;
    // shortcut
    private final int maxStages;
    private final int windowSize;

    private long startTime;

    private static class WindowInfo {
        private final int baseStage;
        private final Ordinal end;

        WindowInfo(int baseStage, Ordinal end) {
            this.baseStage = baseStage;
            this.end = end;
        }
    }

    public TumblingWindow(QueryClient client,
                          List<Criterion<BoxedQueryRequest>> criteria,
                          Criterion<BoxedQueryRequest> until,
                          SequenceMatcher matcher) {
        this.client = client;

        this.until = until;
        this.criteria = criteria;
        this.maxStages = criteria.size();
        this.windowSize = criteria.get(0).queryRequest().searchSource().size();

        this.matcher = matcher;
    }

    @Override
    public void execute(ActionListener<Payload> listener) {
        log.trace("Starting sequence window w/ fetch size [{}]", windowSize);
        startTime = System.currentTimeMillis();
        advance(0, listener);
    }

    private void advance(int baseStage, ActionListener<Payload> listener) {
        // initialize
        Criterion<BoxedQueryRequest> base = criteria.get(baseStage);
        // remove any potential upper limit (if a criteria has been promoted)
        base.queryRequest().to(null);
        matcher.resetInsertPosition();

        log.trace("{}", matcher);
        log.trace("Querying base stage [{}] {}", base.stage(), base.queryRequest());

        client.query(base.queryRequest(), wrap(p -> baseCriterion(baseStage, p, listener), listener::onFailure));
    }

    private void baseCriterion(int baseStage, Payload p, ActionListener<Payload> listener) {
        Criterion<BoxedQueryRequest> base = criteria.get(baseStage);
        List<SearchHit> hits = p.values();

        log.trace("Found [{}] hits", hits.size());

        if (hits.isEmpty() == false) {
            if (matcher.match(baseStage, wrapValues(base, hits)) == false) {
                payload(listener);
                return;
            }
        }

        // only one result means there aren't going to be any matches
        // so move the window boxing to the next stage
        if (hits.size() < 2) {
            // if there are still candidates, advance the window base
            if (matcher.hasCandidates(baseStage) && baseStage + 1 < maxStages) {
                Runnable next = () -> advance(baseStage + 1, listener);

                if (until != null && hits.size() == 1) {
                    // find "until" ordinals - early on to discard data in-flight to avoid matching
                    // hits that can occur in other documents
                    untilCriterion(new WindowInfo(baseStage, base.ordinal(hits.get(0))), listener, next);
                } else {
                    next.run();
                }
            }
            // there aren't going to be any matches so cancel search
            else {
                payload(listener);
            }
            return;
        }

        // get borders for the rest of the queries
        Ordinal begin = base.ordinal(hits.get(0));
        Ordinal end = base.ordinal(hits.get(hits.size() - 1));

        // update current query for the next request
        base.queryRequest().nextAfter(end);

        log.trace("Found base [{}] window {}->{}", base.stage(), begin, end);

        WindowInfo info = new WindowInfo(baseStage, end);

        // no more queries to run
        if (baseStage + 1 < maxStages) {
            Runnable next = () -> secondaryCriterion(info, baseStage + 1, listener);
            if (until != null) {
                // find "until" ordinals - early on to discard data in-flight to avoid matching
                // hits that can occur in other documents
                untilCriterion(info, listener, next);
            } else {
                next.run();
            }
        } else {
            advance(baseStage, listener);
        }
    }

    private void untilCriterion(WindowInfo window, ActionListener<Payload> listener, Runnable next) {
        final BoxedQueryRequest request = until.queryRequest();

        // before doing a new query, clean all previous until hits
        // including dropping any in-flight sequences that were not dropped (because they did not match)
        matcher.dropUntil();

        final boolean reversed = boxQuery(window, until);

        log.trace("Querying until stage {}", request);

        client.query(request, wrap(p -> {
            List<SearchHit> hits = p.values();

            log.trace("Found [{}] hits", hits.size());
            // no more results for until - let the other queries run
            if (hits.isEmpty()) {
                // put the markers in place before the next call
                if (reversed) {
                    request.to(window.end);
                } else {
                    request.from(window.end);
                }
            } else {
                // prepare the query for the next search
                request.nextAfter(until.ordinal(hits.get(hits.size() - 1)));

                // if the limit has been reached, return what's available
                matcher.until(wrapUntilValues(wrapValues(until, hits)));
            }

            // keep running the query runs out of the results (essentially returns less than what we want)
            if (hits.size() == windowSize) {
                untilCriterion(window, listener, next);
            }
            // looks like this stage is done, move on
            else {
                // to the next query
                next.run();
            }

        }, listener::onFailure));
    }

    private void secondaryCriterion(WindowInfo window, int currentStage, ActionListener<Payload> listener) {
        final Criterion<BoxedQueryRequest> criterion = criteria.get(currentStage);
        final BoxedQueryRequest request = criterion.queryRequest();

        final boolean reversed = boxQuery(window, criterion);

        log.trace("Querying (secondary) stage [{}] {}", criterion.stage(), request);

        client.query(request, wrap(p -> {
            List<SearchHit> hits = p.values();

            log.trace("Found [{}] hits", hits.size());

            // no more results for this query
            if (hits.isEmpty()) {
                // put the markers in place before the next call
                if (reversed) {
                    request.to(window.end);
                } else {
                    request.from(window.end);
                }

                // if there are no candidates, advance the window
                if (matcher.hasCandidates(criterion.stage()) == false) {
                    log.trace("Advancing window...");
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
                    payload(listener);
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

    /**
     * Box the query for the given criterion based on the window information.
     * Returns a boolean indicating whether reversal has been applied or not.
     */
    private boolean boxQuery(WindowInfo window, Criterion<BoxedQueryRequest> criterion) {
        final BoxedQueryRequest request = criterion.queryRequest();
        Criterion<BoxedQueryRequest> base = criteria.get(window.baseStage);

        boolean reverse = criterion.reverse() != base.reverse();
        // first box the query
        // only the first base can be descending
        // all subsequence queries are ascending
        if (reverse) {
            if (window.end.equals(request.from()) == false) {
                // if that's the case, set the starting point
                request.from(window.end);
                // reposition the pointer
                request.nextAfter(window.end);
            }
        } else {
            // otherwise just the upper limit
            request.to(window.end);
        }

        return reverse;
    }

    private void payload(ActionListener<Payload> listener) {
        List<Sequence> completed = matcher.completed();

        log.trace("Sending payload for [{}] sequences", completed.size());

        if (completed.isEmpty()) {
            listener.onResponse(new EmptyPayload(Type.SEQUENCE, timeTook()));
            matcher.clear();
            return;
        }

        client.get(hits(completed), wrap(searchHits -> {
            listener.onResponse(new SequencePayload(completed, searchHits, false, timeTook()));
            matcher.clear();
        }, listener::onFailure));
    }

    private TimeValue timeTook() {
        return new TimeValue(System.currentTimeMillis() - startTime);
    }

    Iterable<List<HitReference>> hits(List<Sequence> sequences) {
        return () -> {
            final Iterator<Sequence> delegate = criteria.get(0).reverse() != criteria.get(1).reverse() ?
                    new ReversedIterator<>(sequences) :
                    sequences.iterator();
            
            return new Iterator<>() {

                @Override
                public boolean hasNext() {
                    return delegate.hasNext();
                }

                @Override
                public List<HitReference> next() {
                    return delegate.next().hits();
                }
            };
        };
    }

    Iterable<Tuple<KeyAndOrdinal, HitReference>> wrapValues(Criterion<?> criterion, List<SearchHit> hits) {
        return () -> {
            final Iterator<SearchHit> delegate = criterion.reverse() ? new ReversedIterator<>(hits) : hits.iterator();

            return new Iterator<>() {

                @Override
                public boolean hasNext() {
                    return delegate.hasNext();
                }

                @Override
                public Tuple<KeyAndOrdinal, HitReference> next() {
                    SearchHit hit = delegate.next();
                    SequenceKey k = criterion.key(hit);
                    Ordinal o = criterion.ordinal(hit);
                    return new Tuple<>(new KeyAndOrdinal(k, o), new HitReference(hit));
                }
            };
        };
    }

    <E> Iterable<KeyAndOrdinal> wrapUntilValues(Iterable<Tuple<KeyAndOrdinal, E>> iterable) {
        return () -> {
            final Iterator<Tuple<KeyAndOrdinal, E>> delegate = iterable.iterator();

            return new Iterator<>() {

                @Override
                public boolean hasNext() {
                    return delegate.hasNext();
                }

                @Override
                public KeyAndOrdinal next() {
                    return delegate.next().v1();
                }
            };
        };
    }
}
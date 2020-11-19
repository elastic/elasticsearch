/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.execution.sequence;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;
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
import org.elasticsearch.xpack.eql.session.Payload.Type;
import org.elasticsearch.xpack.eql.util.ReversedIterator;
import org.elasticsearch.xpack.ql.util.ActionListeners;

import java.util.Iterator;
import java.util.List;

import static org.elasticsearch.action.ActionListener.wrap;
import static org.elasticsearch.xpack.eql.execution.search.RuntimeUtils.searchHits;

/**
 * Time-based window encapsulating query creation and advancement.
 * Since queries can return different number of results, to avoid creating incorrect sequences,
 * all searches are 'boxed' to a base query.
 *
 * The window always moves ASC (sorted on timestamp/tiebreaker ordinal) since events in a sequence occur
 * one after the other. The window starts at the base (the first query) - when no results are found,
 * the next query gets promoted. This allows the window to find any follow-up results even if they are
 * found outside the initial window of a base query.
 *
 * TAIL/DESC sequences are handled somewhat differently. The first/base query moves DESC and the tumbling
 * window keeps moving ASC but using the second query as its base. When the tumbling window finishes instead
 * of bailing out, the DESC query keeps advancing.
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

    // flag used for DESC sequences to indicate whether
    // the window needs to restart (since the DESC query still has results)
    private boolean restartWindowFromTailQuery;
    private final boolean earlyUntil;

    private long startTime;

    private static class WindowInfo {
        private final int baseStage;
        private final Ordinal begin;
        private final Ordinal end;

        WindowInfo(int baseStage, Ordinal begin, Ordinal end) {
            this.baseStage = baseStage;
            this.begin = begin;
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
        this.matcher = matcher;

        Criterion<BoxedQueryRequest> baseRequest = criteria.get(0);
        this.windowSize = baseRequest.queryRequest().searchSource().size();
        this.restartWindowFromTailQuery = baseRequest.descending();
        this.earlyUntil = baseRequest.descending();
    }

    @Override
    public void execute(ActionListener<Payload> listener) {
        log.trace("Starting sequence window w/ fetch size [{}]", windowSize);
        startTime = System.currentTimeMillis();
        tumbleWindow(0, listener);
    }

    /**
     * Move the window while preserving the same base.
     */
    private void tumbleWindow(int currentStage, ActionListener<Payload> listener) {
        if (currentStage > 0 && matcher.hasCandidates() == false) {
            if (restartWindowFromTailQuery) {
                currentStage = 0;
            } else {
                // if there are no in-flight sequences (from previous stages)
                // no need to look for more results
                payload(listener);
                return;
            }
        }

        log.trace("Tumbling window...");
        // finished all queries in this window, run a trim
        // for descending queries clean everything
        if (restartWindowFromTailQuery) {
            if (currentStage == 0) {
                matcher.trim(true);
            }
        }
        // trim to last
        else {
           // check case when a rebase occurred and the current query
           // has a lot more results than the first once and hasn't
           // covered the whole window. Running a trim early data before
           // the whole window is matched
           boolean trimToLast = false;
           if (currentStage == 0) {
               trimToLast = true;
           }
           else {
               Ordinal current = criteria.get(currentStage).queryRequest().after();
               Ordinal previous = criteria.get(currentStage - 1).queryRequest().after();
               trimToLast = current.after(previous);
           }
           if (trimToLast) {
               matcher.trim(false);
           }
        }

        advance(currentStage, listener);
    }

    /**
     * Move the window while advancing the query base.
     */
    private void rebaseWindow(int nextStage, ActionListener<Payload> listener) {
        log.trace("Rebasing window...");
        advance(nextStage, listener);
    }

    private void advance(int stage, ActionListener<Payload> listener) {
        // initialize
        Criterion<BoxedQueryRequest> base = criteria.get(stage);
        // remove any potential upper limit (if a criteria has been promoted)
        base.queryRequest().to(null);

        log.trace("{}", matcher);
        log.trace("Querying base stage [{}] {}", stage, base.queryRequest());

        client.query(base.queryRequest(), wrap(p -> baseCriterion(stage, p, listener), listener::onFailure));
    }

    /**
     * Execute the base query.
     */
    private void baseCriterion(int baseStage, SearchResponse r, ActionListener<Payload> listener) {
        Criterion<BoxedQueryRequest> base = criteria.get(baseStage);
        List<SearchHit> hits = searchHits(r);

        log.trace("Found [{}] hits", hits.size());

        Ordinal begin = null, end = null;
        final WindowInfo info;

        // if there is at least one result, process it
        if (hits.isEmpty() == false) {
            // get borders for the rest of the queries - but only when at least one result is found
            begin = headOrdinal(hits, base);
            end = tailOrdinal(hits, base);
            boolean desc = base.descending();
            // always create an ASC window
            info = new WindowInfo(baseStage, begin, end);

            log.trace("Found {}base [{}] window {}->{}", base.descending() ? "tail " : "", base.stage(), begin, end);

            // update current query for the next request
            base.queryRequest().nextAfter(end);

            // execute UNTIL *before* matching the results
            // but after the window has been created
            //
            // this is needed for TAIL sequences since the base of the window
            // is called once with the DESC query, then with the ASC one
            // thus UNTIL needs to be executed before matching the second query
            // that is the ASC base of the window
            if (until != null && baseStage > 0) {
                // find "until" ordinals - early on to discard data in-flight to avoid matching
                // hits that can occur in other documents
                untilCriterion(info, listener, () -> completeBaseCriterion(baseStage, hits, info, listener));
                return;
            }
        } else {
            info = null;
        }
        completeBaseCriterion(baseStage, hits, info, listener);
    }

    private void completeBaseCriterion(int baseStage, List<SearchHit> hits, WindowInfo info, ActionListener<Payload> listener) {
        Criterion<BoxedQueryRequest> base = criteria.get(baseStage);

        // check for matches - if the limit has been reached, abort
        if (matcher.match(baseStage, wrapValues(base, hits)) == false) {
            payload(listener);
            return;
        }

        int nextStage = baseStage + 1;
        boolean windowCompleted = hits.size() < windowSize;

        // there are still queries
        if (nextStage < maxStages) {
            boolean descendingQuery = base.descending();
            Runnable next = null;

            // if there are results, setup the next stage
            if (info != null) {
                if (descendingQuery) {
                    // TAIL query
                    setupWindowFromTail(info.end);
                } else {
                    boxQuery(info, criteria.get(nextStage));
                }
            }

            // this is the last round of matches
            if (windowCompleted) {
                boolean shouldTerminate = false;

                // in case of DESC queries indicate there's no more window restarting
                if (descendingQuery) {
                    if (info != null) {
                        // DESC means starting the window
                        restartWindowFromTailQuery = false;
                        next = () -> advance(1, listener);
                    }
                    // if there are no new results, no need to check the window
                    else {
                        shouldTerminate = true;
                    }
                }
                // for ASC queries continue if there are still matches available
                else  {
                    if (matcher.hasFollowingCandidates(baseStage)) {
                        next = () -> rebaseWindow(nextStage, listener);
                    }
                    // otherwise bail-out, unless it's a DESC sequence that hasn't completed yet
                    // in which case restart
                    else {
                        if (restartWindowFromTailQuery == false) {
                            shouldTerminate = true;
                        } else {
                            next = () -> tumbleWindow(0, listener);
                        }
                    }
                }
                // otherwise bail-out
                if (shouldTerminate) {
                    payload(listener);
                    return;
                }
            }
            // go to the next stage
            else {
                // DESC means starting the window
                if (descendingQuery) {
                    next = () -> advance(1, listener);
                }
                // ASC to continue
                else {
                    next = () -> secondaryCriterion(info, nextStage, listener);
                }
            }

            // until check for HEAD queries
            if (until != null && info != null && info.baseStage == 0) {
                untilCriterion(info, listener, next);
            } else {
                next.run();
            }
        }
        // no more queries to run
        else {
            // no more results either
            if (windowCompleted) {
                if (restartWindowFromTailQuery) {
                    tumbleWindow(0, listener);
                } else {
                    payload(listener);
                }
            }
            // there are still results, keep going
            else {
                tumbleWindow(baseStage, listener);
            }
        }
    }

    private void untilCriterion(WindowInfo window, ActionListener<Payload> listener, Runnable next) {
        final BoxedQueryRequest request = until.queryRequest();
        boxQuery(window, until);

        // in case the base query returns less results than the fetch window
        // the rebase query might take a while to catch up to the until limit
        // the query can be executed but will return 0 results so avoid this case
        // by checking for it explicitly
        if (request.after().after(window.end)) {
            log.trace("Skipping until stage {}", request);
            next.run();
            return;
        }

        log.trace("Querying until stage {}", request);

        client.query(request, wrap(r -> {
            List<SearchHit> hits = searchHits(r);

            log.trace("Found [{}] hits", hits.size());
            // no more results for until - let the other queries run
            if (hits.isEmpty() == false) {
                // prepare the query for the next search
                request.nextAfter(tailOrdinal(hits, until));
                matcher.until(wrapUntilValues(wrapValues(until, hits)));
            }

            // keep running the query runs out of the results (essentially returns less than what we want)
            if (hits.size() == windowSize && request.after().before(window.end)) {
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

        //boxQuery(window, criterion);

        log.trace("Querying (secondary) stage [{}] {}", criterion.stage(), request);

        client.query(request, wrap(r -> {
            List<SearchHit> hits = searchHits(r);

            // filter hits that are escaping the window (same timestamp but different tiebreaker)
            // apply it only to ASC queries; DESC queries need it to find matches going the opposite direction

            hits = trim(hits, criterion, window.end);

            log.trace("Found [{}] hits", hits.size());

            int nextStage = currentStage + 1;

            // if there is at least one result, process it
            if (hits.isEmpty() == false) {
                // prepare the query for the next search
                // however when dealing with tiebreakers the same timestamp can contain different values that might
                // be within or outside the window
                // to make sure one is not lost, check the minimum ordinal between the one found (which might just outside
                // the window - same timestamp but a higher tiebreaker) and the actual window end
                Ordinal tailOrdinal = tailOrdinal(hits, criterion);
                Ordinal headOrdinal = headOrdinal(hits, criterion);

                log.trace("Found range [{}] -> [{}]", headOrdinal, tailOrdinal);

                // set search after
                // for ASC queries limit results to the search window
                // for DESC queries, do not otherwise the follow-up events won't match the headOrdinal result in DESC
                if (tailOrdinal.after(window.end)) {
                    tailOrdinal = window.end;
                }
                request.nextAfter(tailOrdinal);

                // if the limit has been reached, return what's available
                if (matcher.match(criterion.stage(), wrapValues(criterion, hits)) == false) {
                    payload(listener);
                    return;
                }

                // any subsequence query will be ASC - initialize its starting point if not set
                // this is the case during the headOrdinal run for HEAD queries or for each window for TAIL ones
                if (nextStage < maxStages) {
                    BoxedQueryRequest nextRequest = criteria.get(nextStage).queryRequest();
                    if (nextRequest.from() == null || nextRequest.after() == null) {
                        nextRequest.from(headOrdinal);
                        nextRequest.nextAfter(headOrdinal);
                    }
                }
            }

            // keep running the query runs out of the results (essentially returns less than what we want)
            // however check if the window has been fully consumed
            if (hits.size() == windowSize && request.after().before(window.end)) {
                secondaryCriterion(window, currentStage, listener);
            }
            // looks like this stage is done, move on
            else {
                // but first check is there are still candidates within the current window
                if (currentStage + 1 < maxStages && matcher.hasFollowingCandidates(criterion.stage())) {
                    secondaryCriterion(window, currentStage + 1, listener);
                } else {
                    // otherwise, advance it
                    tumbleWindow(window.baseStage, listener);
                }
            }
        }, listener::onFailure));
    }

    /**
     * Trim hits outside the (upper) limit.
     */
    private List<SearchHit> trim(List<SearchHit> searchHits, Criterion<BoxedQueryRequest> criterion, Ordinal boundary) {
        int offset = 0;

        for (int i = searchHits.size() - 1; i >= 0 ; i--) {
            Ordinal ordinal = criterion.ordinal(searchHits.get(i));
            if (ordinal.after(boundary)) {
                offset++;
            } else {
                break;
            }
        }
        return offset == 0 ? searchHits : searchHits.subList(0, searchHits.size() - offset);
    }

    /**
     * Box the query for the given (ASC) criterion based on the window information.
     */
    private void boxQuery(WindowInfo window, Criterion<BoxedQueryRequest> criterion) {
        final BoxedQueryRequest request = criterion.queryRequest();
        // for HEAD, it's the window upper limit that keeps changing
        // so check TO.
        if (window.end.equals(request.to()) == false) {
            request.to(window.end);
        }

        // initialize the start of the next query if needed (such as until)
        // in DESC queries, this is set before the window starts
        // in ASC queries, this is initialized based on the first result from the base query
        if (request.from() == null) {
            request.from(window.begin);
            request.nextAfter(window.begin);
        }
    }

    /**
     * Used by TAIL sequences. Sets the starting point of the (ASC) window.
     * It does that by initializing the from of the stage 1 (the window base)
     * and resets "from" from the other sub-queries so they can initialized accordingly
     * (based on the results of their predecessors).
     */
    private void setupWindowFromTail(Ordinal from) {
        // TAIL can only be at stage 0
        // the ASC window starts at stage 1
        BoxedQueryRequest request = criteria.get(1).queryRequest();

        // check if it hasn't been set before
        if (from.equals(request.from()) == false) {
            // initialize the next request
            request.from(from)
                .nextAfter(from);

            // initialize until (if available)
            if (until != null) {
                until.queryRequest()
                    .from(from)
                    .nextAfter(from);
            }
            // reset all sub queries
            for (int i = 2; i < maxStages; i++) {
                BoxedQueryRequest subRequest = criteria.get(i).queryRequest();
                subRequest.from(null);
            }
        }
    }

    private void payload(ActionListener<Payload> listener) {
        List<Sequence> completed = matcher.completed();

        log.trace("Sending payload for [{}] sequences", completed.size());

        if (completed.isEmpty()) {
            listener.onResponse(new EmptyPayload(Type.SEQUENCE, timeTook()));
            close(listener);
            return;
        }

        // get results through search (to keep using PIT)
        client.fetchHits(hits(completed), ActionListeners.map(listener, listOfHits -> {
            SequencePayload payload = new SequencePayload(completed, listOfHits, false, timeTook());
            close(listener);
            return payload;
        }));
    }

    private void close(ActionListener<Payload> listener) {
        matcher.clear();
        client.close(ActionListener.delegateFailure(listener, (l, r) -> {}));
    }

    private TimeValue timeTook() {
        return new TimeValue(System.currentTimeMillis() - startTime);
    }

    private static Ordinal headOrdinal(List<SearchHit> hits, Criterion<BoxedQueryRequest> criterion) {
        return criterion.ordinal(hits.get(0));
    }

    private static Ordinal tailOrdinal(List<SearchHit> hits, Criterion<BoxedQueryRequest> criterion) {
        return criterion.ordinal(hits.get(hits.size() - 1));
    }

    Iterable<List<HitReference>> hits(List<Sequence> sequences) {
        return () -> {
            final Iterator<Sequence> delegate = criteria.get(0).descending() != criteria.get(1).descending() ?
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
            final Iterator<SearchHit> delegate = criterion.descending() ? new ReversedIterator<>(hits) : hits.iterator();

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

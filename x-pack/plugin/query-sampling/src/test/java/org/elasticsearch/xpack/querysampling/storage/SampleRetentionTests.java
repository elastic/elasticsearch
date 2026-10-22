/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.storage;

import org.apache.logging.log4j.Level;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;

public class SampleRetentionTests extends ESTestCase {

    private final List<DeleteByQueryRequest> requests = new ArrayList<>();

    public void testItDeletesTheQueriesPickedBeforeTheRetentionAndLeavesVersionConflictsForTheNextRound() {
        AtomicLong clock = new AtomicLong(TimeValue.timeValueDays(30).millis());
        SampleRetention retention = new SampleRetention(
            (request, listener) -> requests.add(request),
            clock::get,
            TimeValue.timeValueDays(7)
        );

        retention.run();

        DeleteByQueryRequest request = requests.get(0);
        assertThat(request.indices(), arrayContaining(QuerySamplingIndex.NAME));
        long expiredBefore = clock.get() - TimeValue.timeValueDays(7).millis();
        assertThat(request.getSearchRequest().source().query(), equalTo(QueryBuilders.rangeQuery("picked_at").lt(expiredBefore)));
        assertFalse(request.isAbortOnVersionConflict());
    }

    public void testAMissingIndexIsNotAnError() {
        SampleRetention retention = new SampleRetention(
            (request, listener) -> listener.onFailure(new IndexNotFoundException(QuerySamplingIndex.NAME)),
            () -> 0L,
            TimeValue.timeValueDays(7)
        );
        SampleRetention failing = new SampleRetention(
            (request, listener) -> listener.onFailure(new IllegalStateException("boom")),
            () -> 0L,
            TimeValue.timeValueDays(7)
        );
        String logger = SampleRetention.class.getCanonicalName();

        MockLog.assertThatLogger(
            retention::run,
            SampleRetention.class,
            new MockLog.UnseenEventExpectation("no warning for a missing index", logger, Level.WARN, "*")
        );
        MockLog.assertThatLogger(
            failing::run,
            SampleRetention.class,
            new MockLog.SeenEventExpectation("a warning for another failure", logger, Level.WARN, "*")
        );
    }

    public void testTheIntervalIsAQuarterOfTheRetentionAtMostAnHour() {
        assertThat(SampleRetention.interval(TimeValue.timeValueHours(2)), equalTo(TimeValue.timeValueMinutes(30)));
        assertThat(SampleRetention.interval(TimeValue.timeValueHours(4)), equalTo(TimeValue.timeValueHours(1)));
        assertThat(SampleRetention.interval(TimeValue.timeValueDays(7)), equalTo(TimeValue.timeValueHours(1)));
    }
}

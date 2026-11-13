/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.grok;

import org.joni.Matcher;
import org.joni.Option;
import org.joni.Region;

/**
 * A joni {@link Matcher} registered with a {@link MatcherWatchdog} for its whole lifetime, for callers that need their own search loop
 * (for example to find every match rather than just the first). It exists so that such callers cannot forget the two parts of the
 * watchdog contract: every search that is interrupted fails with an exception rather than looking like "no more matches", and the
 * matcher is unregistered when finished with. Use it in a try-with-resources block.
 */
public final class WatchedMatcher implements AutoCloseable {

    private final Matcher matcher;
    private final MatcherWatchdog watchdog;

    WatchedMatcher(Matcher matcher, MatcherWatchdog watchdog) {
        this.matcher = matcher;
        this.watchdog = watchdog;
        watchdog.register(matcher);
    }

    /**
     * Searches forward for the next match, with the watchdog's time limit applying to this call alone.
     *
     * @return the match position, or {@link Matcher#FAILED} if there is no further match
     * @throws RuntimeException if the search was interrupted by the watchdog
     */
    public int search(int start, int range) {
        int result = matcher.search(start, range, Option.DEFAULT);
        Grok.handleInterrupted(result, watchdog);
        return result;
    }

    public int getEnd() {
        return matcher.getEnd();
    }

    public Region getEagerRegion() {
        return matcher.getEagerRegion();
    }

    @Override
    public void close() {
        watchdog.unregister(matcher);
    }
}

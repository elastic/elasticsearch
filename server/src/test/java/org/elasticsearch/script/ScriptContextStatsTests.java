/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.script.TimeSeriesTests.randomTimeseries;

public class ScriptContextStatsTests extends ESTestCase {
    public void testMerge() {
        {
            var first = randomScriptContextStats();
            var second = randomScriptContextStats();

            var e = expectThrows(AssertionError.class, () -> ScriptContextStats.merge(first, second));
            assertEquals(e.getMessage(), "To merge 2 ScriptContextStats both of them must have the same context.");
        }
        {
            var context = randomAlphaOfLength(30);
            var first = randomScriptContextStats(context);
            var second = randomScriptContextStats(context);

            assertEquals(
                ScriptContextStats.merge(first, second),
                new ScriptContextStats(
                    context,
                    first.compilations() + second.compilations(),
                    TimeSeries.merge(first.compilationsHistory(), second.compilationsHistory()),
                    first.cacheEvictions() + second.cacheEvictions(),
                    TimeSeries.merge(first.cacheEvictionsHistory(), second.cacheEvictionsHistory()),
                    first.compilationLimitTriggered() + second.compilationLimitTriggered()
                )
            );
        }
    }

    public static ScriptContextStats randomScriptContextStats() {
        return randomScriptContextStats(randomAlphaOfLength(30));
    }

    public static ScriptContextStats randomScriptContextStats(String contextName) {
        return new ScriptContextStats(
            contextName,
            randomLongBetween(0, 10000),
            randomTimeseries(),
            randomLongBetween(0, 10000),
            randomTimeseries(),
            randomLongBetween(0, 10000)
        );
    }

}

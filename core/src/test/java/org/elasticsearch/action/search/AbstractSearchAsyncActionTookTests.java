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

package org.elasticsearch.action.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class AbstractSearchAsyncActionTookTests extends ESTestCase {

    private AbstractSearchAsyncAction<SearchPhaseResult> createAction(
            final boolean controlled,
            final AtomicLong expected) {

        final Runnable runnable;
        final TransportSearchAction.SearchTimeProvider timeProvider;
        if (controlled) {
            runnable = () -> expected.set(randomNonNegativeLong());
            timeProvider = new TransportSearchAction.SearchTimeProvider(0, 0, expected::get);
        } else {
            runnable = () -> {
                long elapsed = spinForAtLeastNMilliseconds(randomIntBetween(1, 10));
                expected.set(elapsed);
            };
            timeProvider = new TransportSearchAction.SearchTimeProvider(
                    0,
                    System.nanoTime(),
                    System::nanoTime);
        }

        final ShardIterator it = new ShardIterator() {
            @Override
            public ShardId shardId() {
                return null;
            }

            @Override
            public void reset() {

            }

            @Override
            public int compareTo(ShardIterator o) {
                return 0;
            }

            @Override
            public int size() {
                return 0;
            }

            @Override
            public int sizeActive() {
                return 0;
            }

            @Override
            public ShardRouting nextOrNull() {
                return null;
            }

            @Override
            public int remaining() {
                return 0;
            }

            @Override
            public Iterable<ShardRouting> asUnordered() {
                return null;
            }
        };

        return new AbstractSearchAsyncAction<SearchPhaseResult>(
                "test",
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                new GroupShardsIterator(Collections.singletonList(it)),
                timeProvider,
                0,
                null,
                null
        ) {
            @Override
            protected SearchPhase getNextPhase(
                    final SearchPhaseResults<SearchPhaseResult> results,
                    final SearchPhaseContext context) {
                return null;
            }

            @Override
            protected void executePhaseOnShard(
                    final ShardIterator shardIt,
                    final ShardRouting shard,
                    final SearchActionListener<SearchPhaseResult> listener) {

            }

            @Override
            long buildTookInMillis() {
                runnable.run();
                return super.buildTookInMillis();
            }
        };
    }

    public void testTookWithControlledClock() {
        runTestTook(true);
    }

    public void testTookWithRealClock() {
        runTestTook(false);
    }

    private void runTestTook(final boolean controlled) {
        final AtomicLong expected = new AtomicLong();
        AbstractSearchAsyncAction<SearchPhaseResult> action = createAction(controlled, expected);
        final long actual = action.buildTookInMillis();
        if (controlled) {
            // with a controlled clock, we can assert the exact took time
            assertThat(actual, equalTo(TimeUnit.NANOSECONDS.toMillis(expected.get())));
        } else {
            // with a real clock, the best we can say is that it took as long as we spun for
            assertThat(actual, greaterThanOrEqualTo(TimeUnit.NANOSECONDS.toMillis(expected.get())));
        }
    }

}

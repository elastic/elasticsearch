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

package org.elasticsearch.common.breaker;

import org.elasticsearch.test.ESTestCase;

import java.util.LinkedList;
import java.util.Map;

public class DebuggingBreakerTests extends ESTestCase {

    private TestBreaker breaker = new TestBreaker();

    public void testThing() {
        CircuitBreaker.BreakerKey key = CircuitBreaker.getKey("arrays");
        breaker.addEstimateBytesAndMaybeBreak(key, 10);

        Map<CircuitBreaker.BreakerKey, LinkedList<DebuggingBreaker.CircuitNode>> state = breaker.getState();

        System.out.println(state);
    }

    private static class TestBreaker extends DebuggingBreaker {

        private long limit = 20;

        private TestBreaker() {
            super(true);
        }

        @Override
        public void circuitBreak(String fieldName, long bytesNeeded) {

        }

        @Override
        public double addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
            return 0;
        }

        @Override
        public long addWithoutBreaking(long bytes) {
            return 0;
        }

        @Override
        public long getUsed() {
            return 0;
        }

        @Override
        public long getLimit() {
            return 0;
        }

        @Override
        public double getOverhead() {
            return 0;
        }

        @Override
        public long getTrippedCount() {
            return 0;
        }

        @Override
        public String getName() {
            return null;
        }
    }
}

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

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

public class DebuggingBreakerTests extends ESTestCase {

    private TestBreaker breaker = new TestBreaker(true);

    public void testNodesAreAddedWhenBreakerCalled() {
        CircuitBreaker.BreakerKey key = CircuitBreaker.getKey("arrays");
        int first = randomInt(5);
        int second = randomInt(5);
        int third = randomInt(5);
        int[] numbers = {first, second, third};
        breaker.addEstimateBytesAndMaybeBreak(key, first);
        breaker.addEstimateBytesAndMaybeBreak(key, second);
        breaker.addEstimateBytesAndMaybeBreak(key, third);

        Map<CircuitBreaker.BreakerKey, LinkedList<DebuggingBreaker.CircuitNode>> state = breaker.getState();

        assertEquals(1, state.size());

        LinkedList<DebuggingBreaker.CircuitNode> circuitNodes = state.get(key);

        int i = 0;
        for (DebuggingBreaker.CircuitNode node : circuitNodes) {
            assertEquals(numbers[i], node.getDelta());
            Set<String> fileNames = new HashSet<>();
            Set<String> methodNames = new HashSet<>();
            for (StackTraceElement element : node.getStackTraceElements()) {
                fileNames.add(element.getFileName());
                methodNames.add(element.getMethodName());
            }
            assertTrue(fileNames.contains("DebuggingBreakerTests.java"));
            assertTrue(methodNames.contains("testNodesAreAddedWhenBreakerCalled"));
            assertTrue(fileNames.contains("DebuggingBreaker.java"));
            assertTrue(methodNames.contains("addEstimateBytesAndMaybeBreak"));
            ++i;
        }
    }

    public void testNodeNotAddedWhenBreakerBreaks() {
        CircuitBreaker.BreakerKey key = CircuitBreaker.getKey("arrays");
        breaker.addEstimateBytesAndMaybeBreak(key, 1);
        expectThrows(CircuitBreakingException.class, () -> breaker.addEstimateBytesAndMaybeBreak(key, 21));

        Map<CircuitBreaker.BreakerKey, LinkedList<DebuggingBreaker.CircuitNode>> state = breaker.getState();

        assertEquals(1, state.size());

        LinkedList<DebuggingBreaker.CircuitNode> circuitNodes = state.get(key);

        assertEquals(1, circuitNodes.size());
        assertEquals(1, circuitNodes.getFirst().getDelta());
    }

    public void testNodesAreDroppedWhenReleaseCalled() {
        CircuitBreaker.BreakerKey key = CircuitBreaker.getKey("arrays");
        int first = randomInt(5);
        int second = randomInt(5);
        int third = randomInt(5);
        breaker.addEstimateBytesAndMaybeBreak(key, first);
        breaker.addEstimateBytesAndMaybeBreak(key, second);
        breaker.addEstimateBytesAndMaybeBreak(key, third);

        Map<CircuitBreaker.BreakerKey, LinkedList<DebuggingBreaker.CircuitNode>> state = breaker.getState();

        assertEquals(1, state.size());

        LinkedList<DebuggingBreaker.CircuitNode> circuitNodes = state.get(key);

        assertEquals(3, circuitNodes.size());

        breaker.release(key);

        assertFalse(breaker.getState().containsKey(key));
    }

    public void testBytesAreProperlyAccountedFor() {
        CircuitBreaker.BreakerKey key = CircuitBreaker.getKey("arrays");
        int first = randomInt(5);
        int second = randomInt(5);
        int third = randomInt(5);
        breaker.addEstimateBytesAndMaybeBreak(key, first);
        breaker.addEstimateBytesAndMaybeBreak(key, second);
        breaker.addEstimateBytesAndMaybeBreak(key, third);

        int total = first + second + third;

        assertEquals(total, key.getBytes());
        assertEquals(total, breaker.getUsed());

        breaker.release(key);

        assertEquals(0, key.getBytes());
        assertEquals(0, breaker.getUsed());
    }

    public void testBytesAreProperlyAccountedForEvenIfNotDebug() {
        breaker = new TestBreaker(false);
        CircuitBreaker.BreakerKey key = CircuitBreaker.getKey("arrays");
        int first = randomInt(5);
        int second = randomInt(5);
        int third = randomInt(5);
        breaker.addEstimateBytesAndMaybeBreak(key, first);
        breaker.addEstimateBytesAndMaybeBreak(key, second);
        breaker.addEstimateBytesAndMaybeBreak(key, third);

        int total = first + second + third;

        assertEquals(total, key.getBytes());
        assertEquals(total, breaker.getUsed());

        breaker.release(key);

        assertEquals(0, key.getBytes());
        assertEquals(0, breaker.getUsed());
    }

    public void testCannotReleaseNonExistentKey() {
        CircuitBreaker.BreakerKey key = CircuitBreaker.getKey("arrays");

        IllegalStateException ex = expectThrows(IllegalStateException.class, () -> breaker.release(key));
        assertEquals("Key does not exist in breaker", ex.getMessage());
    }

    public void testCanReleaseNonExistentKeyIfNotDebug() {
        breaker = new TestBreaker(false);
        CircuitBreaker.BreakerKey key = CircuitBreaker.getKey("arrays");

        breaker.release(key);
    }

    private static class TestBreaker extends DebuggingBreaker {

        private long limit = 20;
        private long current = 0;

        private TestBreaker(boolean debug) {
            super(debug, true);
        }

        @Override
        public void circuitBreak(String fieldName, long bytesNeeded) {
            throw new CircuitBreakingException("Broken");
        }

        @Override
        public double addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
            if (bytes > limit) {
                throw new CircuitBreakingException("Broken");
            }
            current += bytes;
            return 0;
        }

        @Override
        public long addWithoutBreaking(long bytes) {
            current += bytes;
            return 0;
        }

        @Override
        public long getUsed() {
            return current;
        }

        @Override
        public long getLimit() {
            return limit;
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

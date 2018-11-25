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
package org.elasticsearch.cluster.coordination;

import org.elasticsearch.cluster.coordination.LinearizabilityChecker.History;
import org.elasticsearch.cluster.coordination.LinearizabilityChecker.SequentialSpec;
import org.elasticsearch.test.ESTestCase;

import java.util.Optional;

import static org.hamcrest.Matchers.equalTo;

public class LinearizabilityCheckerTests extends ESTestCase {

    final LinearizabilityChecker checker = new LinearizabilityChecker();

    /**
     * Simple specification of a lock that can be exactly locked once. There is no unlocking.
     * Input is always null (and represents lock acquisition), output is a boolean whether lock was acquired.
     */
    final SequentialSpec lockSpec = new SequentialSpec() {

        @Override
        public Object initialState() {
            return false;
        }

        @Override
        public Optional<Object> nextState(Object currentState, Object input, Object output) {
            if (input != null) {
                throw new AssertionError("invalid history: input must be null");
            }
            if (output instanceof Boolean == false) {
                throw new AssertionError("invalid history: output must be boolean");
            }
            if (false == (boolean) currentState) {
                if (false == (boolean) output) {
                    return Optional.empty();
                }
                return Optional.of(true);
            } else if (false == (boolean) output) {
                return Optional.of(currentState);
            }
            return Optional.empty();
        }
    };

    public void testLockConsistent() {
        assertThat(lockSpec.initialState(), equalTo(false));
        assertThat(lockSpec.nextState(false, null, true), equalTo(Optional.of(true)));
        assertThat(lockSpec.nextState(false, null, false), equalTo(Optional.empty()));
        assertThat(lockSpec.nextState(true, null, false), equalTo(Optional.of(true)));
        assertThat(lockSpec.nextState(true, null, true), equalTo(Optional.empty()));
    }

    public void testLockWithLinearizableHistory1() {
        final History history = new History();
        int call0 = history.invoke(null); // 0: acquire lock
        history.respond(call0, true); // 0: lock acquisition succeeded
        int call1 = history.invoke(null); // 1: acquire lock
        history.respond(call1, false); // 0: lock acquisition failed
        assertTrue(checker.isLinearizable(lockSpec, history));
    }

    public void testLockWithLinearizableHistory2() {
        final History history = new History();
        int call0 = history.invoke(null); // 0: acquire lock
        int call1 = history.invoke(null); // 1: acquire lock
        history.respond(call0, false); // 0: lock acquisition failed
        history.respond(call1, true); // 0: lock acquisition succeeded
        assertTrue(checker.isLinearizable(lockSpec, history));
    }

    public void testLockWithLinearizableHistory3() {
        final History history = new History();
        int call0 = history.invoke(null); // 0: acquire lock
        int call1 = history.invoke(null); // 1: acquire lock
        history.respond(call0, true); // 0: lock acquisition succeeded
        history.respond(call1, false); // 0: lock acquisition failed
        assertTrue(checker.isLinearizable(lockSpec, history));
    }

    public void testLockWithNonLinearizableHistory() {
        final History history = new History();
        int call0 = history.invoke(null); // 0: acquire lock
        history.respond(call0, false); // 0: lock acquisition failed
        int call1 = history.invoke(null); // 1: acquire lock
        history.respond(call1, true); // 0: lock acquisition succeeded
        assertFalse(checker.isLinearizable(lockSpec, history));
    }

    /**
     * Simple specification of a read/write register.
     * Writes are modeled as integer inputs (with corresponding null responses) and
     * reads are modeled as null inputs with integer outputs.
     */
    final SequentialSpec registerSpec = new SequentialSpec() {

        @Override
        public Object initialState() {
            return 0;
        }

        @Override
        public Optional<Object> nextState(Object currentState, Object input, Object output) {
            if ((input == null) == (output == null)) {
                throw new AssertionError("invalid history: exactly one of input or output must be null");
            }
            if (input != null) {
                return Optional.of(input);
            } else if (output.equals(currentState)) {
                return Optional.of(currentState);
            }
            return Optional.empty();
        }
    };

    public void testRegisterConsistent() {
        assertThat(registerSpec.initialState(), equalTo(0));
        assertThat(registerSpec.nextState(7, 42, null), equalTo(Optional.of(42)));
        assertThat(registerSpec.nextState(7, null, 7), equalTo(Optional.of(7)));
        assertThat(registerSpec.nextState(7, null, 42), equalTo(Optional.empty()));
    }

    public void testRegisterWithLinearizableHistory() {
        final History history = new History();
        int call0 = history.invoke(42); // 0: invoke write 42
        int call1 = history.invoke(null); // 1: invoke read
        int call2 = history.invoke(null); // 2: invoke read
        history.respond(call2, 0); // 2: read returns 0
        history.respond(call1, 42); // 1: read returns 42

        expectThrows(IllegalArgumentException.class, () -> checker.isLinearizable(registerSpec, history));
        assertTrue(checker.isLinearizable(registerSpec, history, i -> null));

        history.respond(call0, null); // 0: write returns
        assertTrue(checker.isLinearizable(registerSpec, history));
    }

    public void testRegisterWithNonLinearizableHistory() {
        final History history = new History();
        int call0 = history.invoke(42); // 0: invoke write 42
        int call1 = history.invoke(null); // 1: invoke read
        history.respond(call1, 42); // 1: read returns 42
        int call2 = history.invoke(null); // 2: invoke read
        history.respond(call2, 0); // 2: read returns 0

        expectThrows(IllegalArgumentException.class, () -> checker.isLinearizable(registerSpec, history));
        assertFalse(checker.isLinearizable(registerSpec, history, i -> null));

        history.respond(call0, null); // 0: write returns
        assertFalse(checker.isLinearizable(registerSpec, history));
    }
}

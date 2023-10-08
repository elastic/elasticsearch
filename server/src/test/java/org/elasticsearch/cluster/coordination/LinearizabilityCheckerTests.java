/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.coordination;

import org.elasticsearch.cluster.coordination.LinearizabilityChecker.History;
import org.elasticsearch.cluster.coordination.LinearizabilityChecker.KeyedSpec;
import org.elasticsearch.cluster.coordination.LinearizabilityChecker.SequentialSpec;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;

import java.util.Optional;

import static org.hamcrest.Matchers.equalTo;

public class LinearizabilityCheckerTests extends ESTestCase {

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
        assertTrue(LinearizabilityChecker.isLinearizable(lockSpec, history));
    }

    public void testLockWithLinearizableHistory2() {
        final History history = new History();
        int call0 = history.invoke(null); // 0: acquire lock
        int call1 = history.invoke(null); // 1: acquire lock
        history.respond(call0, false); // 0: lock acquisition failed
        history.respond(call1, true); // 0: lock acquisition succeeded
        assertTrue(LinearizabilityChecker.isLinearizable(lockSpec, history));
    }

    public void testLockWithLinearizableHistory3() {
        final History history = new History();
        int call0 = history.invoke(null); // 0: acquire lock
        int call1 = history.invoke(null); // 1: acquire lock
        history.respond(call0, true); // 0: lock acquisition succeeded
        history.respond(call1, false); // 0: lock acquisition failed
        assertTrue(LinearizabilityChecker.isLinearizable(lockSpec, history));
    }

    public void testLockWithNonLinearizableHistory() {
        final History history = new History();
        int call0 = history.invoke(null); // 0: acquire lock
        history.respond(call0, false); // 0: lock acquisition failed
        int call1 = history.invoke(null); // 1: acquire lock
        history.respond(call1, true); // 0: lock acquisition succeeded
        assertFalse(LinearizabilityChecker.isLinearizable(lockSpec, history));
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

        expectThrows(IllegalArgumentException.class, () -> LinearizabilityChecker.isLinearizable(registerSpec, history));
        assertTrue(LinearizabilityChecker.isLinearizable(registerSpec, history, i -> null));

        history.respond(call0, null); // 0: write returns
        assertTrue(LinearizabilityChecker.isLinearizable(registerSpec, history));
    }

    public void testRegisterWithNonLinearizableHistory() {
        final History history = new History();
        int call0 = history.invoke(42); // 0: invoke write 42
        int call1 = history.invoke(null); // 1: invoke read
        history.respond(call1, 42); // 1: read returns 42
        int call2 = history.invoke(null); // 2: invoke read
        history.respond(call2, 0); // 2: read returns 0, not allowed

        expectThrows(IllegalArgumentException.class, () -> LinearizabilityChecker.isLinearizable(registerSpec, history));
        assertFalse(LinearizabilityChecker.isLinearizable(registerSpec, history, i -> null));

        history.respond(call0, null); // 0: write returns
        assertFalse(LinearizabilityChecker.isLinearizable(registerSpec, history));
    }

    public void testRegisterObservedSequenceOfUpdatesWitLinearizableHistory() {
        final History history = new History();
        int call0 = history.invoke(42); // 0: invoke write 42
        int call1 = history.invoke(43); // 1: invoke write 43
        int call2 = history.invoke(null); // 2: invoke read
        history.respond(call2, 42); // 1: read returns 42
        int call3 = history.invoke(null); // 3: invoke read
        history.respond(call3, 43); // 3: read returns 43
        int call4 = history.invoke(null); // 4: invoke read
        history.respond(call4, 43); // 4: read returns 43

        history.respond(call0, null); // 0: write returns
        history.respond(call1, null); // 1: write returns

        assertTrue(LinearizabilityChecker.isLinearizable(registerSpec, history));
    }

    public void testRegisterObservedSequenceOfUpdatesWithNonLinearizableHistory() {
        final History history = new History();
        int call0 = history.invoke(42); // 0: invoke write 42
        int call1 = history.invoke(43); // 1: invoke write 43
        int call2 = history.invoke(null); // 2: invoke read
        history.respond(call2, 42); // 1: read returns 42
        int call3 = history.invoke(null); // 3: invoke read
        history.respond(call3, 43); // 3: read returns 43
        int call4 = history.invoke(null); // 4: invoke read
        history.respond(call4, 42); // 4: read returns 42, not allowed

        history.respond(call0, null); // 0: write returns
        history.respond(call1, null); // 1: write returns

        assertFalse(LinearizabilityChecker.isLinearizable(registerSpec, history));
    }

    final SequentialSpec multiRegisterSpec = new KeyedSpec() {

        @Override
        public Object getKey(Object value) {
            return ((Tuple) value).v1();
        }

        @Override
        public Object getValue(Object value) {
            return ((Tuple) value).v2();
        }

        @Override
        public Object initialState() {
            return registerSpec.initialState();
        }

        @Override
        public Optional<Object> nextState(Object currentState, Object input, Object output) {
            return registerSpec.nextState(currentState, input, output);
        }
    };

    public void testMultiRegisterWithLinearizableHistory() {
        final History history = new History();
        int callX0 = history.invoke(new Tuple<>("x", 42)); // 0: invoke write 42 on key x
        int callX1 = history.invoke(new Tuple<>("x", null)); // 1: invoke read on key x
        int callY0 = history.invoke(new Tuple<>("y", 42)); // 0: invoke write 42 on key y
        int callY1 = history.invoke(new Tuple<>("y", null)); // 1: invoke read on key y
        int callX2 = history.invoke(new Tuple<>("x", null)); // 2: invoke read on key x
        int callY2 = history.invoke(new Tuple<>("y", null)); // 2: invoke read on key y
        history.respond(callX2, 0); // 2: read returns 0 on key x
        history.respond(callY2, 0); // 2: read returns 0 on key y
        history.respond(callY1, 42); // 1: read returns 42 on key y
        history.respond(callX1, 42); // 1: read returns 42 on key x

        expectThrows(IllegalArgumentException.class, () -> LinearizabilityChecker.isLinearizable(multiRegisterSpec, history));
        assertTrue(LinearizabilityChecker.isLinearizable(multiRegisterSpec, history, i -> null));

        history.respond(callX0, null); // 0: write returns on key x
        history.respond(callY0, null); // 0: write returns on key y
        assertTrue(LinearizabilityChecker.isLinearizable(multiRegisterSpec, history));
    }

    public void testMultiRegisterWithNonLinearizableHistory() {
        final History history = new History();
        int callX0 = history.invoke(new Tuple<>("x", 42)); // 0: invoke write 42 on key x
        int callX1 = history.invoke(new Tuple<>("x", null)); // 1: invoke read on key x
        int callY0 = history.invoke(new Tuple<>("y", 42)); // 0: invoke write 42 on key y
        int callY1 = history.invoke(new Tuple<>("y", null)); // 1: invoke read on key y
        int callX2 = history.invoke(new Tuple<>("x", null)); // 2: invoke read on key x
        history.respond(callY1, 42); // 1: read returns 42 on key y
        int callY2 = history.invoke(new Tuple<>("y", null)); // 2: invoke read on key y
        history.respond(callX2, 0); // 2: read returns 0 on key x
        history.respond(callY2, 0); // 2: read returns 0 on key y, not allowed
        history.respond(callX1, 42); // 1: read returns 42 on key x

        expectThrows(IllegalArgumentException.class, () -> LinearizabilityChecker.isLinearizable(multiRegisterSpec, history));
        assertFalse(LinearizabilityChecker.isLinearizable(multiRegisterSpec, history, i -> null));

        history.respond(callX0, null); // 0: write returns on key x
        history.respond(callY0, null); // 0: write returns on key y
        assertFalse(LinearizabilityChecker.isLinearizable(multiRegisterSpec, history));
    }
}

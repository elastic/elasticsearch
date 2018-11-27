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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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

    private static final Pattern invokedRead = Pattern.compile("^INFO\\s+jepsen\\.util\\s+-\\s+(\\d+)\\s+:invoke\\s+:read\\s+nil$");
    private static final Pattern invokedWrite = Pattern.compile("^INFO\\s+jepsen\\.util\\s+-\\s+(\\d+)\\s+:invoke\\s+:write\\s+(\\d+)$");
    private static final Pattern invokedCas =
        Pattern.compile("^INFO\\s+jepsen\\.util\\s+-\\s+(\\d+)\\s+:invoke\\s+:cas\\s+\\[(\\d+)\\s+(\\d+)\\]$");
    private static final Pattern returnRead = Pattern.compile("^INFO\\s+jepsen\\.util\\s+-\\s+(\\d+)\\s+:ok\\s+:read\\s+(nil|\\d+)$");
    private static final Pattern returnWrite = Pattern.compile("^INFO\\s+jepsen\\.util\\s+-\\s+(\\d+)\\s+:ok\\s+:write\\s+(\\d+)$");
    private static final Pattern returnCas =
        Pattern.compile("^INFO\\s+jepsen\\.util\\s+-\\s+(\\d+)\\s+:(ok|fail)\\s+:cas\\s+\\[(\\d+)\\s+(\\d+)\\]$");
    private static final Pattern timeoutRead = Pattern.compile("^INFO\\s+jepsen\\.util\\s+-\\s+(\\d+)\\s+:fail\\s+:read\\s+:timed-out$");
    private static final Pattern timeoutWrite = Pattern.compile("^INFO\\s+jepsen\\.util\\s+-\\s+(\\d+)\\s+:info\\s+:write\\s+:timed-out$");
    private static final Pattern timeoutCas = Pattern.compile("^INFO\\s+jepsen\\.util\\s+-\\s+(\\d+)\\s+:info\\s+:cas\\s+:timed-out$");

    History readEtcdHistory(String name) {
        final History history = new History();
        Map<Integer, Integer> map = new HashMap<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream(name)))) {
            for (String line : reader.lines().collect(Collectors.toList())) {
                Matcher invokedReadMatcher = invokedRead.matcher(line);
                Matcher invokedWriteMatcher = invokedWrite.matcher(line);
                Matcher invokedCasMatcher = invokedCas.matcher(line);
                Matcher returnReadMatcher = returnRead.matcher(line);
                Matcher returnWriteMatcher = returnWrite.matcher(line);
                Matcher returnCasMatcher = returnCas.matcher(line);
                Matcher timeoutReadMatcher = timeoutRead.matcher(line);
                Matcher timeoutWriteMatcher = timeoutWrite.matcher(line);
                Matcher timeoutCasMatcher = timeoutCas.matcher(line);
                if (invokedReadMatcher.matches()) {
                    MatchResult result = invokedReadMatcher.toMatchResult();
                    int proc = Integer.valueOf(result.group(1));
                    map.put(proc, history.invoke(new EtcdInput(0, 0, 0)));
                } else if (invokedWriteMatcher.matches()) {
                    MatchResult result = invokedWriteMatcher.toMatchResult();
                    int proc = Integer.valueOf(result.group(1));
                    int value = Integer.valueOf(result.group(2));
                    map.put(proc, history.invoke(new EtcdInput(1, value, 0)));
                } else if (invokedCasMatcher.matches()) {
                    MatchResult result = invokedCasMatcher.toMatchResult();
                    int proc = Integer.valueOf(result.group(1));
                    int from = Integer.valueOf(result.group(2));
                    int to = Integer.valueOf(result.group(3));
                    map.put(proc, history.invoke(new EtcdInput(2, from, to)));
                } else if (returnReadMatcher.matches()) {
                    MatchResult result = returnReadMatcher.toMatchResult();
                    int proc = Integer.valueOf(result.group(1));
                    boolean exists = false;
                    int value = 0;
                    if ("nil".equals(result.group(2)) == false) {
                        exists = true;
                        value = Integer.valueOf(result.group(2));
                    }
                    history.respond(map.remove(proc), new EtcdOutput(false, exists, value, false));
                } else if (returnWriteMatcher.matches()) {
                    MatchResult result = returnWriteMatcher.toMatchResult();
                    int proc = Integer.valueOf(result.group(1));
                    history.respond(map.remove(proc), new EtcdOutput(false, false, 0, false));
                } else if (returnCasMatcher.matches()) {
                    MatchResult result = returnCasMatcher.toMatchResult();
                    int proc = Integer.valueOf(result.group(1));
                    history.respond(map.remove(proc), new EtcdOutput("ok".equals(result.group(2)), false, 0, false));
                } else if (timeoutReadMatcher.matches()) {
                    MatchResult result = timeoutReadMatcher.toMatchResult();
                    int proc = Integer.valueOf(result.group(1));
                    history.respond(map.remove(proc), new EtcdOutput(false, false, 0, true));
                } else if (timeoutWriteMatcher.matches() || timeoutCasMatcher.matches()) {
                    // handled by history completion below
                } else {
                    throw new IllegalArgumentException(line);
                }
            }
            history.complete(i -> new EtcdOutput(false, false, 0, true));
            return history;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void checkJepsen(int id, boolean result) {
        History history = readEtcdHistory(String.format("etcd_%03d.log", id));
        assertThat(checker.isLinearizable(etcdSpec, history), equalTo(result));
    }

    public void testAll() {
        for (int i = 0; i < 103; i++) {
            logger.info("trying {}", i);
            History history = readEtcdHistory(String.format("etcd_%03d.log", i));
            logger.info("linearizable {}", checker.isLinearizable(etcdSpec, history));
        }
    }

    public void testEtcd000() {
        checkJepsen(0, false);
    }

    public void testEtcd001() {
        checkJepsen(1, false);
    }

    public void testEtcd002() {
        checkJepsen(2, true);
    }

    public void testEtcd022() {
        checkJepsen(22, false);
    }

    public void testEtcd007() {
        checkJepsen(7, true);
    }

    public void testEtcd099() {
        checkJepsen(99, false);
    }

    public void testEtcd040() {
        checkJepsen(40, false);
    }

    public void testEtcd097() {
        checkJepsen(97, false);
    }

    private final SequentialSpec etcdSpec = new SequentialSpec() {

        @Override
        public Object initialState() {
            return -1000000;
        }

        @Override
        public Optional<Object> nextState(Object currentState, Object input, Object output) {
            EtcdInput in = (EtcdInput) input;
            EtcdOutput out = (EtcdOutput) output;
            int st = (int) currentState;
            if (in.op == 0) {
                if ((out.exists == false && st == -1000000) || (out.exists && st == out.value) || out.unknown) {
                    return Optional.of(currentState);
                }
                return Optional.empty();
            } else if (in.op == 1) {
                return Optional.of(in.arg1);
            } else {
                if ((in.arg1 == st && out.ok) || (in.arg1 != st && !out.ok) || out.unknown) {
                    return Optional.of(in.arg1 == st ? in.arg2 : st);
                }
                return Optional.empty();
            }
        }
    };

    public static class EtcdInput {
        final int op;
        final int arg1;
        final int arg2;

        public EtcdInput(int op, int arg1, int arg2) {
            this.op = op;
            this.arg1 = arg1;
            this.arg2 = arg2;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof EtcdInput)) return false;

            EtcdInput etcdInput = (EtcdInput) o;

            if (op != etcdInput.op) return false;
            if (arg1 != etcdInput.arg1) return false;
            return arg2 == etcdInput.arg2;
        }

        @Override
        public int hashCode() {
            int result = op;
            result = 31 * result + arg1;
            result = 31 * result + arg2;
            return result;
        }
    }

    public static class EtcdOutput {
        final boolean ok;
        final boolean exists;
        final int value;
        final boolean unknown;

        public EtcdOutput(boolean ok, boolean exists, int value, boolean unknown) {
            this.ok = ok;
            this.exists = exists;
            this.value = value;
            this.unknown = unknown;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof EtcdOutput)) return false;

            EtcdOutput that = (EtcdOutput) o;

            if (ok != that.ok) return false;
            if (exists != that.exists) return false;
            if (value != that.value) return false;
            return unknown == that.unknown;
        }

        @Override
        public int hashCode() {
            int result = (ok ? 1 : 0);
            result = 31 * result + (exists ? 1 : 0);
            result = 31 * result + value;
            result = 31 * result + (unknown ? 1 : 0);
            return result;
        }
    }

}

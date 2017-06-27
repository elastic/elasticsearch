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

package org.elasticsearch.monitor.jvm;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;

public class DeadlockAnalyzer {

    private static final Deadlock NULL_RESULT[] = new Deadlock[0];
    private final ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();

    private static DeadlockAnalyzer INSTANCE = new DeadlockAnalyzer();

    public static DeadlockAnalyzer deadlockAnalyzer() {
        return INSTANCE;
    }

    private DeadlockAnalyzer() {

    }

    public Deadlock[] findDeadlocks() {
        long deadlockedThreads[] = threadBean.findMonitorDeadlockedThreads();
        if (deadlockedThreads == null || deadlockedThreads.length == 0) {
            return NULL_RESULT;
        }
        Map<Long, ThreadInfo> threadInfoMap = createThreadInfoMap(deadlockedThreads);
        Set<LinkedHashSet<ThreadInfo>> cycles = calculateCycles(threadInfoMap);
        Set<LinkedHashSet<ThreadInfo>> chains = calculateCycleDeadlockChains(threadInfoMap, cycles);
        cycles.addAll(chains);
        return createDeadlockDescriptions(cycles);
    }


    private Deadlock[] createDeadlockDescriptions(Set<LinkedHashSet<ThreadInfo>> cycles) {
        Deadlock result[] = new Deadlock[cycles.size()];
        int count = 0;
        for (LinkedHashSet<ThreadInfo> cycle : cycles) {
            ThreadInfo asArray[] = cycle.toArray(new ThreadInfo[cycle.size()]);
            Deadlock d = new Deadlock(asArray);
            result[count++] = d;
        }
        return result;
    }


    private Set<LinkedHashSet<ThreadInfo>> calculateCycles(Map<Long, ThreadInfo> threadInfoMap) {
        Set<LinkedHashSet<ThreadInfo>> cycles = new HashSet<>();
        for (Map.Entry<Long, ThreadInfo> entry : threadInfoMap.entrySet()) {
            LinkedHashSet<ThreadInfo> cycle = new LinkedHashSet<>();
            for (ThreadInfo t = entry.getValue(); !cycle.contains(t); t = threadInfoMap.get(Long.valueOf(t.getLockOwnerId()))) {
                cycle.add(t);
            }

            if (!cycles.contains(cycle)) {
                cycles.add(cycle);
            }
        }
        return cycles;
    }


    private Set<LinkedHashSet<ThreadInfo>> calculateCycleDeadlockChains(Map<Long, ThreadInfo> threadInfoMap,
            Set<LinkedHashSet<ThreadInfo>> cycles) {
        ThreadInfo allThreads[] = threadBean.getThreadInfo(threadBean.getAllThreadIds());
        Set<LinkedHashSet<ThreadInfo>> deadlockChain = new HashSet<>();
        Set<Long> knownDeadlockedThreads = threadInfoMap.keySet();
        for (ThreadInfo threadInfo : allThreads) {
            Thread.State state = threadInfo.getThreadState();
            if (state == Thread.State.BLOCKED && !knownDeadlockedThreads.contains(threadInfo.getThreadId())) {
                for (LinkedHashSet<ThreadInfo> cycle : cycles) {
                    if (cycle.contains(threadInfoMap.get(Long.valueOf(threadInfo.getLockOwnerId())))) {
                        LinkedHashSet<ThreadInfo> chain = new LinkedHashSet<>();
                        ThreadInfo node = threadInfo;
                        while (!chain.contains(node)) {
                            chain.add(node);
                            node = threadInfoMap.get(Long.valueOf(node.getLockOwnerId()));
                        }
                        deadlockChain.add(chain);
                    }
                }

            }
        }

        return deadlockChain;
    }


    private Map<Long, ThreadInfo> createThreadInfoMap(long threadIds[]) {
        ThreadInfo threadInfos[] = threadBean.getThreadInfo(threadIds);
        Map<Long, ThreadInfo> threadInfoMap = new HashMap<>();
        for (ThreadInfo threadInfo : threadInfos) {
            threadInfoMap.put(threadInfo.getThreadId(), threadInfo);
        }
        return unmodifiableMap(threadInfoMap);
    }

    public static class Deadlock {
        private final ThreadInfo members[];
        private final String description;
        private final Set<Long> memberIds;

        public Deadlock(ThreadInfo[] members) {
            this.members = members;

            Set<Long> builder = new HashSet<>();
            StringBuilder sb = new StringBuilder();
            for (int x = 0; x < members.length; x++) {
                ThreadInfo ti = members[x];
                sb.append(ti.getThreadName());
                if (x < members.length) {
                    sb.append(" > ");
                }
                if (x == members.length - 1) {
                    sb.append(ti.getLockOwnerName());
                }
                builder.add(ti.getThreadId());
            }
            this.description = sb.toString();
            this.memberIds = unmodifiableSet(builder);
        }

        public ThreadInfo[] members() {
            return members;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Deadlock deadlock = (Deadlock) o;

            if (memberIds != null ? !memberIds.equals(deadlock.memberIds) : deadlock.memberIds != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = members != null ? Arrays.hashCode(members) : 0;
            result = 31 * result + (description != null ? description.hashCode() : 0);
            result = 31 * result + (memberIds != null ? memberIds.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return description;
        }
    }
}

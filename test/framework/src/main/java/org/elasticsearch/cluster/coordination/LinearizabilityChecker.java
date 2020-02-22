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

import com.carrotsearch.hppc.LongObjectHashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Basic implementation of the Wing and Gong Graph Search Algorithm, following the descriptions in
 * Gavin Lowe: Testing for linearizability
 *   Concurrency and Computation: Practice and Experience 29, 4 (2017). http://dx.doi.org/10.1002/cpe.3928
 * Alex Horn and Daniel Kroening: Faster linearizability checking via P-compositionality
 *   FORTE (2015). http://dx.doi.org/10.1007/978-3-319-19195-9_4
 */
public class LinearizabilityChecker {

    private static final Logger logger = LogManager.getLogger(LinearizabilityChecker.class);

    /**
     * Sequential specification of a datatype. Used as input for the linearizability checker.
     * All parameter and return values should be immutable and have proper equals / hashCode implementations
     */
    public interface SequentialSpec {
        /**
         * Returns the initial state of the datatype
         */
        Object initialState();

        /**
         * Next-state function, checking whether transitioning the datatype in the given state under the provided input and output is valid.
         *
         * @param currentState the current state of the datatype
         * @param input the input, associated with the given invocation event
         * @param output the output, associated with the corresponding response event
         * @return the next state, if the given current state, input and output are a valid transition, or Optional.empty() otherwise
         */
        Optional<Object> nextState(Object currentState, Object input, Object output);

        /**
         * For compositional checking, the history can be partitioned into sub-histories
         *
         * @param events the history of events to partition
         * @return the partitioned history
         */
        default Collection<List<Event>> partition(List<Event> events) {
            return Collections.singleton(events);
        }
    }

    /**
     * Sequential specification of a datatype that allows for keyed access,
     * providing compositional checking (see {@link SequentialSpec#partition(List)}).
     */
    public interface KeyedSpec extends SequentialSpec {
        /**
         * extracts the key from the given keyed invocation input value
         */
        Object getKey(Object value);

        /**
         * extracts the key-less value from the given keyed invocation input value
         */
        Object getValue(Object value);

        @Override
        default Collection<List<Event>> partition(List<Event> events) {
            final Map<Object, List<Event>> keyedPartitions = new HashMap<>();
            final Map<Integer, Object> matches = new HashMap<>();
            for (Event event : events) {
                if (event.type == EventType.INVOCATION) {
                    final Object key = getKey(event.value);
                    final Object val = getValue(event.value);
                    final Event unfoldedEvent = new Event(EventType.INVOCATION, val, event.id);
                    keyedPartitions.computeIfAbsent(key, k -> new ArrayList<>()).add(unfoldedEvent);
                    matches.put(event.id, key);
                } else {
                    final Object key = matches.get(event.id);
                    keyedPartitions.get(key).add(event);
                }
            }
            return keyedPartitions.values();
        }
    }

    /**
     * Sequence of invocations and responses, recording the run of a concurrent system.
     */
    public static class History {
        private final Queue<Event> events;
        private AtomicInteger nextId = new AtomicInteger();

        public History() {
            events = new ConcurrentLinkedQueue<>();
        }

        public History(Collection<Event> events) {
            this();
            this.events.addAll(events);
            this.nextId.set(events.stream().mapToInt(e -> e.id).max().orElse(-1) + 1);
        }

        /**
         * Appends a new invocation event to the history
         *
         * @param input the input value associated with the invocation event
         * @return an id that can be used to record the corresponding response event
         */
        public int invoke(Object input) {
            final int id = nextId.getAndIncrement();
            events.add(new Event(EventType.INVOCATION, input, id));
            return id;
        }

        /**
         * Appends a new response event to the history
         *
         * @param id the id of the corresponding invocation event
         * @param output the output value associated with the response event
         */
        public void respond(int id, Object output) {
            events.add(new Event(EventType.RESPONSE, output, id));
        }

        /**
         * Removes the events with the corresponding id from the history
         *
         * @param id the value of the id to remove
         */
        public void remove(int id) {
            events.removeIf(e -> e.id == id);
        }

        /**
         * Copy the list of events for external use.
         * @return list of events in the order recorded.
         */
        public List<Event> copyEvents() {
            return new ArrayList<>(events);
        }
        /**
         * Completes the history with response events for invocations that are missing corresponding responses
         *
         * @param missingResponseGenerator a function from invocation input to response output, used to generate the corresponding response
         */
        public void complete(Function<Object, Object> missingResponseGenerator) {
            final Map<Integer, Event> uncompletedInvocations = new HashMap<>();
            for (Event event : events) {
                if (event.type == EventType.INVOCATION) {
                    uncompletedInvocations.put(event.id, event);
                } else {
                    final Event removed = uncompletedInvocations.remove(event.id);
                    if (removed == null) {
                        throw new IllegalArgumentException("history not well-formed: " + events);
                    }
                }
            }
            for (Map.Entry<Integer, Event> entry : uncompletedInvocations.entrySet()) {
                events.add(new Event(EventType.RESPONSE, missingResponseGenerator.apply(entry.getValue().value), entry.getKey()));
            }
        }

        @Override
        public History clone() {
            return new History(events);
        }

        /**
         * Returns the number of recorded events
         */
        public int size() {
            return events.size();
        }

        @Override
        public String toString() {
            return "History{" +
                "events=" + events +
                ", nextId=" + nextId +
                '}';
        }

    }

    /**
     * Checks whether the provided history is linearizable with respect to the given sequential specification
     *
     * @param spec the sequential specification of the datatype
     * @param history the history of events to check for linearizability
     * @param missingResponseGenerator used to complete the history with missing responses
     * @return true iff the history is linearizable w.r.t. the given spec
     */
    public boolean isLinearizable(SequentialSpec spec, History history, Function<Object, Object> missingResponseGenerator) {
        return isLinearizable(spec, history, missingResponseGenerator, () -> false);
    }

    /**
     * Checks whether the provided history is linearizable with respect to the given sequential specification
     *
     * @param spec the sequential specification of the datatype
     * @param history the history of events to check for linearizability
     * @param missingResponseGenerator used to complete the history with missing responses
     * @param terminateEarly a condition upon which to terminate early
     * @return true iff the history is linearizable w.r.t. the given spec
     */
    public boolean isLinearizable(SequentialSpec spec, History history, Function<Object, Object> missingResponseGenerator,
                                  BooleanSupplier terminateEarly) {
        history = history.clone(); // clone history before completing it
        history.complete(missingResponseGenerator); // complete history
        final Collection<List<Event>> partitions = spec.partition(history.copyEvents());
        return partitions.stream().allMatch(h -> isLinearizable(spec, h, terminateEarly));
    }

    private boolean isLinearizable(SequentialSpec spec, List<Event> history, BooleanSupplier terminateEarly) {
        logger.debug("Checking history of size: {}: {}", history.size(), history);
        Object state = spec.initialState(); // the current state of the datatype
        final FixedBitSet linearized = new FixedBitSet(history.size() / 2); // the linearized prefix of the history

        final Cache cache = new Cache(); // cache of explored <state, linearized prefix> pairs
        final Deque<Tuple<Entry, Object>> calls = new LinkedList<>(); // path we're currently exploring

        final Entry headEntry = createLinkedEntries(history);
        Entry entry = headEntry.next; // current entry

        while (headEntry.next != null) {
            if (terminateEarly.getAsBoolean()) {
                return false;
            }
            if (entry.match != null) {
                final Optional<Object> maybeNextState = spec.nextState(state, entry.event.value, entry.match.event.value);
                boolean shouldExploreNextState = false;
                if (maybeNextState.isPresent()) {
                    // check if we have already explored this linearization
                    final FixedBitSet updatedLinearized = linearized.clone();
                    updatedLinearized.set(entry.id);
                    shouldExploreNextState = cache.add(maybeNextState.get(), updatedLinearized);
                }
                if (shouldExploreNextState) {
                    calls.push(new Tuple<>(entry, state));
                    state = maybeNextState.get();
                    linearized.set(entry.id);
                    entry.lift();
                    entry = headEntry.next;
                } else {
                    entry = entry.next;
                }
            } else {
                if (calls.isEmpty()) {
                    return false;
                }
                final Tuple<Entry, Object> top = calls.pop();
                entry = top.v1();
                state = top.v2();
                linearized.clear(entry.id);
                entry.unlift();
                entry = entry.next;
            }
        }
        return true;
    }

    /**
     * Convenience method for {@link #isLinearizable(SequentialSpec, History, Function)} that requires the history to be complete
     */
    public boolean isLinearizable(SequentialSpec spec, History history) {
        return isLinearizable(spec, history, o -> {
            throw new IllegalArgumentException("history is not complete");
        });
    }

    /**
     * Return a visual representation of the history
     */
    public static String visualize(SequentialSpec spec, History history, Function<Object, Object> missingResponseGenerator) {
        history = history.clone();
        history.complete(missingResponseGenerator);
        final Collection<List<Event>> partitions = spec.partition(history.copyEvents());
        StringBuilder builder = new StringBuilder();
        partitions.forEach(new Consumer<List<Event>>() {
            int index = 0;
            @Override
            public void accept(List<Event> events) {
                builder.append("Partition " ).append(index++).append("\n");
                builder.append(visualizePartition(events));
            }
        });

        return builder.toString();
    }

    private static String visualizePartition(List<Event> events) {
        StringBuilder builder = new StringBuilder();
        Entry entry = createLinkedEntries(events).next;
        Map<Tuple<EventType, Integer>, Integer> eventToPosition = new HashMap<>();
        for (Event event : events) {
            eventToPosition.put(Tuple.tuple(event.type, event.id), eventToPosition.size());
        }
        while (entry != null) {
            if (entry.match != null) {
                builder.append(visualizeEntry(entry, eventToPosition)).append("\n");
            }
            entry = entry.next;
        }
        return builder.toString();
    }

    private static String visualizeEntry(Entry entry, Map<Tuple<EventType, Integer>, Integer> eventToPosition) {
        String input = String.valueOf(entry.event.value);
        String output = String.valueOf(entry.match.event.value);
        int id = entry.event.id;
        int beginIndex = eventToPosition.get(Tuple.tuple(EventType.INVOCATION, id));
        int endIndex = eventToPosition.get(Tuple.tuple(EventType.RESPONSE, id));
        input = input.substring(0, Math.min(beginIndex + 25, input.length()));
        return Strings.padStart(input, beginIndex + 25, ' ') +
            "   "  + Strings.padStart("", endIndex-beginIndex, 'X') + "   "
            + output + "  (" + entry.event.id + ")";
    }

    /**
     * Creates the internal linked data structure used by the linearizability checker.
     * Generates contiguous internal ids for the events so that they can be efficiently recorded in bit sets.
     */
    private static Entry createLinkedEntries(List<Event> history) {
        if (history.size() % 2 != 0) {
            throw new IllegalArgumentException("mismatch between number of invocations and responses");
        }

        // first, create entries and link response events to invocation events
        final Map<Integer, Entry> matches = new HashMap<>(); // map from event id to matching response entry
        final Entry[] entries = new Entry[history.size()];
        int nextInternalId = (history.size() / 2) - 1;
        for (int i = history.size() - 1; i >= 0; i--) {
            final Event elem = history.get(i);
            if (elem.type == EventType.RESPONSE) {
                final Entry entry = entries[i] = new Entry(elem, null, nextInternalId--);
                final Entry prev = matches.put(elem.id, entry);
                if (prev != null) {
                    throw new IllegalArgumentException("duplicate response with id " + elem.id);
                }
            } else {
                final Entry matchingResponse = matches.get(elem.id);
                if (matchingResponse == null) {
                    throw new IllegalArgumentException("no matching response found for " + elem);
                }
                entries[i] = new Entry(elem, matchingResponse, matchingResponse.id);
            }
        }

        // sanity check
        if (nextInternalId != -1) {
            throw new IllegalArgumentException("id mismatch");
        }

        // now link entries together in history order, and add a sentinel node at the beginning
        Entry first = new Entry(null, null, -1);
        Entry lastEntry = first;
        for (Entry entry : entries) {
            lastEntry.next = entry;
            entry.prev = lastEntry;
            lastEntry = entry;
        }

        return first;
    }

    public enum EventType {
        INVOCATION,
        RESPONSE
    }

    public static class Event {
        public final EventType type;
        public final Object value;
        public final int id;

        public Event(EventType type, Object value, int id) {
            this.type = type;
            this.value = value;
            this.id = id;
        }

        @Override
        public String toString() {
            return "Event{" +
                "type=" + type +
                ", value=" + value +
                ", id=" + id +
                '}';
        }
    }

    static class Entry {
        final Event event;
        final Entry match; // null if current entry is a response, non-null if it's an invocation
        final int id; // internal id, distinct from Event.id
        Entry prev;
        Entry next;

        Entry(Event event, Entry match, int id) {
            this.event = event;
            this.match = match;
            this.id = id;
        }

        // removes this entry from the surrounding structures
        void lift() {
            prev.next = next;
            next.prev = prev;
            match.prev.next = match.next;
            if (match.next != null) {
                match.next.prev = match.prev;
            }
        }

        // reinserts this entry into the surrounding structures
        void unlift() {
            match.prev.next = match;
            if (match.next != null) {
                match.next.prev = match;
            }
            prev.next = this;
            next.prev = this;
        }
    }


    /**
     * A cache optimized for small bit-counts (less than 64) and small number of unique permutations of state objects.
     *
     * Each combination of states is kept once only, building on the
     * assumption that the number of permutations is small compared to the
     * number of bits permutations. For those histories that are difficult to check
     * we will have many bits combinations that use the same state permutations.
     *
     * The smallMap optimization allows us to avoid object overheads for bit-sets up to 64 bit large.
     *
     * Comparing set of (bits, state) to smallMap:
     * (bits, state) : 24 (tuple) + 24 (FixedBitSet) + 24 (bits) + 5 (hash buckets) + 24 (hashmap node).
     * smallMap bits to {state} : 10 (bits) + 5 (hash buckets) + avg-size of unique permutations.
     *
     * The avg-size of the unique permutations part is very small compared to the
     * sometimes large number of bits combinations (which are the cases where
     * we run into trouble).
     *
     * set of (bits, state) totals 101 bytes compared to smallMap bits to { state }
     * which totals 15 bytes, ie. a 6x improvement in memory usage.
     */
    private static class Cache {
        private final Map<Object, Set<FixedBitSet>> largeMap = new HashMap<>();
        private final LongObjectHashMap<Set<Object>> smallMap = new LongObjectHashMap<>();
        private final Map<Object, Object> internalizeStateMap = new HashMap<>();
        private final Map<Set<Object>, Set<Object>> statePermutations = new HashMap<>();

        /**
         * Add state, bits combination
         * @return true if added, false if already registered.
         */
        public boolean add(Object state, FixedBitSet bitSet) {
            return addInternal(internalizeStateMap.computeIfAbsent(state, k -> state), bitSet);
        }

        private boolean addInternal(Object state, FixedBitSet bitSet) {
            long[] bits = bitSet.getBits();
            if (bits.length == 1)
                return addSmall(state, bits[0]);
            else
                return addLarge(state, bitSet);
        }

        private boolean addSmall(Object state, long bits) {
            int index = smallMap.indexOf(bits);
            Set<Object> states;
            if (index < 0) {
                states = Collections.singleton(state);
            } else {
                Set<Object> oldStates = smallMap.indexGet(index);
                if (oldStates.contains(state))
                    return false;
                states = new HashSet<>(oldStates.size() + 1);
                states.addAll(oldStates);
                states.add(state);
            }

            // Get a unique set object per state permutation. We assume that the number of permutations of states are small.
            // We thus avoid the overhead of the set data structure.
            states = statePermutations.computeIfAbsent(states, k -> k);

            if (index < 0) {
                smallMap.indexInsert(index, bits, states);
            } else {
                smallMap.indexReplace(index, states);
            }

            return true;
        }

        private boolean addLarge(Object state, FixedBitSet bitSet) {
            return largeMap.computeIfAbsent(state, k -> new HashSet<>()).add(bitSet);
        }
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.serialization;

import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.cluster.DiffableUtils.MapDiff;
import org.elasticsearch.common.collect.ImmutableOpenIntMap;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;

public class DiffableTests extends ESTestCase {

    public void testJKDMapDiff() throws IOException {
        new JdkMapDriver<TestDiffable>() {
            @Override
            protected boolean diffableValues() {
                return true;
            }

            @Override
            protected TestDiffable createValue(Integer key, boolean before) {
                return new TestDiffable(String.valueOf(before ? key : key + 1));
            }

            @Override
            protected MapDiff diff(Map<Integer, TestDiffable> before, Map<Integer, TestDiffable> after) {
                return DiffableUtils.diff(before, after, keySerializer);
            }

            @Override
            protected MapDiff readDiff(StreamInput in) throws IOException {
                return useProtoForDiffableSerialization
                        ? DiffableUtils.readJdkMapDiff(in, keySerializer, TestDiffable::readFrom, TestDiffable::readDiffFrom)
                        : DiffableUtils.readJdkMapDiff(in, keySerializer, diffableValueSerializer());
            }
        }.execute();

        new JdkMapDriver<String>() {
            @Override
            protected boolean diffableValues() {
                return false;
            }

            @Override
            protected String createValue(Integer key, boolean before) {
                return String.valueOf(before ? key : key + 1);
            }

            @Override
            protected MapDiff diff(Map<Integer, String> before, Map<Integer, String> after) {
                return DiffableUtils.diff(before, after, keySerializer, nonDiffableValueSerializer());
            }

            @Override
            protected MapDiff readDiff(StreamInput in) throws IOException {
                return DiffableUtils.readJdkMapDiff(in, keySerializer, nonDiffableValueSerializer());
            }
        }.execute();
    }

    public void testImmutableOpenMapDiff() throws IOException {
        new ImmutableOpenMapDriver<TestDiffable>() {
            @Override
            protected boolean diffableValues() {
                return true;
            }

            @Override
            protected TestDiffable createValue(Integer key, boolean before) {
                return new TestDiffable(String.valueOf(before ? key : key + 1));
            }

            @Override
            protected MapDiff diff(ImmutableOpenMap<Integer, TestDiffable> before, ImmutableOpenMap<Integer, TestDiffable> after) {
                return DiffableUtils.diff(before, after, keySerializer);
            }

            @Override
            protected MapDiff readDiff(StreamInput in) throws IOException {
                return useProtoForDiffableSerialization
                        ? DiffableUtils.readImmutableOpenMapDiff(in, keySerializer,
                        new DiffableUtils.DiffableValueReader<>(TestDiffable::readFrom, TestDiffable::readDiffFrom))
                        : DiffableUtils.readImmutableOpenMapDiff(in, keySerializer, diffableValueSerializer());
            }
        }.execute();

        new ImmutableOpenMapDriver<String>() {
            @Override
            protected boolean diffableValues() {
                return false;
            }

            @Override
            protected String createValue(Integer key, boolean before) {
                return String.valueOf(before ? key : key + 1);
            }

            @Override
            protected MapDiff diff(ImmutableOpenMap<Integer, String> before, ImmutableOpenMap<Integer, String> after) {
                return DiffableUtils.diff(before, after, keySerializer, nonDiffableValueSerializer());
            }

            @Override
            protected MapDiff readDiff(StreamInput in) throws IOException {
                return DiffableUtils.readImmutableOpenMapDiff(in, keySerializer, nonDiffableValueSerializer());
            }
        }.execute();
    }

    public void testImmutableOpenIntMapDiff() throws IOException {
        new ImmutableOpenIntMapDriver<TestDiffable>() {
            @Override
            protected boolean diffableValues() {
                return true;
            }

            @Override
            protected TestDiffable createValue(Integer key, boolean before) {
                return new TestDiffable(String.valueOf(before ? key : key + 1));
            }

            @Override
            protected MapDiff diff(ImmutableOpenIntMap<TestDiffable> before, ImmutableOpenIntMap<TestDiffable> after) {
                return DiffableUtils.diff(before, after, keySerializer);
            }

            @Override
            protected MapDiff readDiff(StreamInput in) throws IOException {
                return useProtoForDiffableSerialization
                        ? DiffableUtils.readImmutableOpenIntMapDiff(in, keySerializer, TestDiffable::readFrom, TestDiffable::readDiffFrom)
                        : DiffableUtils.readImmutableOpenIntMapDiff(in, keySerializer, diffableValueSerializer());
            }
        }.execute();

        new ImmutableOpenIntMapDriver<String>() {
            @Override
            protected boolean diffableValues() {
                return false;
            }

            @Override
            protected String createValue(Integer key, boolean before) {
                return String.valueOf(before ? key : key + 1);
            }

            @Override
            protected MapDiff diff(ImmutableOpenIntMap<String> before, ImmutableOpenIntMap<String> after) {
                return DiffableUtils.diff(before, after, keySerializer, nonDiffableValueSerializer());
            }

            @Override
            protected MapDiff readDiff(StreamInput in) throws IOException {
                return DiffableUtils.readImmutableOpenIntMapDiff(in, keySerializer, nonDiffableValueSerializer());
            }
        }.execute();
    }

    /**
     * Class that abstracts over specific map implementation type and value kind (Diffable or not)
     * @param <T> map type
     * @param <V> value type
     */
    public abstract class MapDriver<T, V> {
        protected final Set<Integer> keys = randomPositiveIntSet();
        protected final Set<Integer> keysToRemove = new HashSet<>(randomSubsetOf(randomInt(keys.size()), keys.toArray(new Integer[0])));
        protected final Set<Integer> keysThatAreNotRemoved = Sets.difference(keys, keysToRemove);
        protected final Set<Integer> keysToOverride = new HashSet<>(randomSubsetOf(randomInt(keysThatAreNotRemoved.size()),
                keysThatAreNotRemoved.toArray(new Integer[keysThatAreNotRemoved.size()])));
        // make sure keysToAdd does not contain elements in keys
        protected final Set<Integer> keysToAdd = Sets.difference(randomPositiveIntSet(), keys);
        protected final Set<Integer> keysUnchanged = Sets.difference(keysThatAreNotRemoved, keysToOverride);

        protected final DiffableUtils.KeySerializer<Integer> keySerializer = randomBoolean()
                ? DiffableUtils.getIntKeySerializer()
                : DiffableUtils.getVIntKeySerializer();

        protected final boolean useProtoForDiffableSerialization = randomBoolean();

        private Set<Integer> randomPositiveIntSet() {
            int maxSetSize = randomInt(6);
            Set<Integer> result = new HashSet<>();
            for (int i = 0; i < maxSetSize; i++) {
                // due to duplicates, set size can be smaller than maxSetSize
                result.add(randomIntBetween(0, 100));
            }
            return result;
        }

        /**
         * whether we operate on {@link org.elasticsearch.cluster.Diffable} values
         */
        protected abstract boolean diffableValues();

        /**
         * functions that determines value in "before" or "after" map based on key
         */
        protected abstract V createValue(Integer key, boolean before);

        /**
         * creates map based on JDK-based map
         */
        protected abstract T createMap(Map<Integer, V> values);

        /**
         * calculates diff between two maps
         */
        protected abstract MapDiff<Integer, V, T> diff(T before, T after);

        /**
         * reads diff of maps from stream
         */
        protected abstract MapDiff<Integer, V, T> readDiff(StreamInput in) throws IOException;

        /**
         * gets element at key "key" in map "map"
         */
        protected abstract V get(T map, Integer key);

        /**
         * returns size of given map
         */
        protected abstract int size(T map);

        /**
         * executes the actual test
         */
        public void execute() throws IOException {
            logger.debug("Keys in 'before' map: {}", keys);
            logger.debug("Keys to remove: {}", keysToRemove);
            logger.debug("Keys to override: {}", keysToOverride);
            logger.debug("Keys to add: {}", keysToAdd);

            logger.debug("--> creating 'before' map");
            Map<Integer, V> before = new HashMap<>();
            for (Integer key : keys) {
                before.put(key, createValue(key, true));
            }
            T beforeMap = createMap(before);

            logger.debug("--> creating 'after' map");
            Map<Integer, V> after = new HashMap<>();
            after.putAll(before);
            for (Integer key : keysToRemove) {
                after.remove(key);
            }
            for (Integer key : keysToOverride) {
                after.put(key, createValue(key, false));
            }
            for (Integer key : keysToAdd) {
                after.put(key, createValue(key, false));
            }
            T afterMap = createMap(unmodifiableMap(after));

            MapDiff<Integer, V, T> diffMap = diff(beforeMap, afterMap);

            // check properties of diffMap
            assertThat(new HashSet(diffMap.getDeletes()), equalTo(keysToRemove));
            if (diffableValues()) {
                assertThat(diffMap.getDiffs().keySet(), equalTo(keysToOverride));
                for (Integer key : keysToOverride) {
                    assertThat(diffMap.getDiffs().get(key).apply(get(beforeMap, key)), equalTo(get(afterMap, key)));
                }
                assertThat(diffMap.getUpserts().keySet(), equalTo(keysToAdd));
                for (Integer key : keysToAdd) {
                    assertThat(diffMap.getUpserts().get(key), equalTo(get(afterMap, key)));
                }
            } else {
                assertThat(diffMap.getDiffs(), equalTo(emptyMap()));
                Set<Integer> keysToAddAndOverride = Sets.union(keysToAdd, keysToOverride);
                assertThat(diffMap.getUpserts().keySet(), equalTo(keysToAddAndOverride));
                for (Integer key : keysToAddAndOverride) {
                    assertThat(diffMap.getUpserts().get(key), equalTo(get(afterMap, key)));
                }
            }

            if (randomBoolean()) {
                logger.debug("--> serializing diff");
                BytesStreamOutput out = new BytesStreamOutput();
                diffMap.writeTo(out);
                StreamInput in = out.bytes().streamInput();
                logger.debug("--> reading diff back");
                diffMap = readDiff(in);
            }
            T appliedDiffMap = diffMap.apply(beforeMap);

            // check properties of appliedDiffMap
            assertThat(size(appliedDiffMap), equalTo(keys.size() - keysToRemove.size() + keysToAdd.size()));
            for (Integer key : keysToRemove) {
                assertThat(get(appliedDiffMap, key), nullValue());
            }
            for (Integer key : keysUnchanged) {
                assertThat(get(appliedDiffMap, key), equalTo(get(beforeMap, key)));
            }
            for (Integer key : keysToOverride) {
                assertThat(get(appliedDiffMap, key), not(equalTo(get(beforeMap, key))));
                assertThat(get(appliedDiffMap, key), equalTo(get(afterMap, key)));
            }
            for (Integer key : keysToAdd) {
                assertThat(get(appliedDiffMap, key), equalTo(get(afterMap, key)));
            }
        }
    }

    abstract class JdkMapDriver<V> extends MapDriver<Map<Integer, V>, V> {

        @Override
        protected Map<Integer, V> createMap(Map values) {
            return values;
        }

        @Override
        protected V get(Map<Integer, V> map, Integer key) {
            return map.get(key);
        }

        @Override
        protected int size(Map<Integer, V> map) {
            return map.size();
        }
    }

    abstract class ImmutableOpenMapDriver<V> extends MapDriver<ImmutableOpenMap<Integer, V>, V> {

        @Override
        protected ImmutableOpenMap<Integer, V> createMap(Map values) {
            return ImmutableOpenMap.<Integer, V>builder().putAll(values).build();
        }

        @Override
        protected V get(ImmutableOpenMap<Integer, V> map, Integer key) {
            return map.get(key);
        }

        @Override
        protected int size(ImmutableOpenMap<Integer, V> map) {
            return map.size();
        }
    }


    abstract class ImmutableOpenIntMapDriver<V> extends MapDriver<ImmutableOpenIntMap<V>, V> {

        @Override
        protected ImmutableOpenIntMap<V> createMap(Map values) {
            return ImmutableOpenIntMap.<V>builder().putAll(values).build();
        }

        @Override
        protected V get(ImmutableOpenIntMap<V> map, Integer key) {
            return map.get(key);
        }

        @Override
        protected int size(ImmutableOpenIntMap<V> map) {
            return map.size();
        }
    }

    private static <K> DiffableUtils.DiffableValueSerializer<K, TestDiffable> diffableValueSerializer() {
        return new DiffableUtils.DiffableValueSerializer<K, TestDiffable>() {
            @Override
            public TestDiffable read(StreamInput in, K key) throws IOException {
                return new TestDiffable(in.readString());
            }

            @Override
            public Diff<TestDiffable> readDiff(StreamInput in, K key) throws IOException {
                return AbstractDiffable.readDiffFrom(TestDiffable::readFrom, in);
            }
        };
    }

    private static <K> DiffableUtils.NonDiffableValueSerializer<K, String> nonDiffableValueSerializer() {
        return new DiffableUtils.NonDiffableValueSerializer<K, String>() {
            @Override
            public void write(String value, StreamOutput out) throws IOException {
                out.writeString(value);
            }

            @Override
            public String read(StreamInput in, K key) throws IOException {
                return in.readString();
            }
        };
    }

    public static class TestDiffable extends AbstractDiffable<TestDiffable> {

        private final String value;

        public TestDiffable(String value) {
            this.value = value;
        }

        public String value() {
            return value;
        }

        public static TestDiffable readFrom(StreamInput in) throws IOException {
            return new TestDiffable(in.readString());
        }

        public static Diff<TestDiffable> readDiffFrom(StreamInput in) throws IOException {
            return readDiffFrom(TestDiffable::readFrom, in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(value);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TestDiffable that = (TestDiffable) o;

            return Objects.equals(value, that.value);
        }

        @Override
        public int hashCode() {
            return value != null ? value.hashCode() : 0;
        }
    }

}

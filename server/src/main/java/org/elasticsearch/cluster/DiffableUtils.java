/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.Tuple;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

public final class DiffableUtils {
    private DiffableUtils() {}

    private static final MapDiff<?, ?, ?> EMPTY = new MapDiff<>(null, null, List.of(), List.of(), List.of(), null) {
        @Override
        public Map<Object, Object> apply(Map<Object, Object> part) {
            return part;
        }
    };

    /**
     * Returns a map key serializer for String keys
     */
    public static KeySerializer<String> getStringKeySerializer() {
        return StringKeySerializer.INSTANCE;
    }

    /**
     * Returns a map key serializer for Integer keys. Encodes as Int.
     */
    public static KeySerializer<Integer> getIntKeySerializer() {
        return IntKeySerializer.INSTANCE;
    }

    /**
     * Returns a map key serializer for Integer keys. Encodes as VInt.
     */
    public static KeySerializer<Integer> getVIntKeySerializer() {
        return VIntKeySerializer.INSTANCE;
    }

    /**
     * Returns a map key serializer for {@link Writeable} keys.
     */
    public static <W extends Writeable> KeySerializer<W> getWriteableKeySerializer(Writeable.Reader<W> reader) {
        return new WriteableKeySerializer<>(reader);
    }

    /**
     * Calculates diff between two Maps of Diffable objects.
     */
    public static <K, T extends Diffable<T>, M extends Map<K, T>> MapDiff<K, T, M> diff(M before, M after, KeySerializer<K> keySerializer) {
        assert after != null && before != null;
        return before.equals(after)
            ? emptyDiff()
            : createDiff(before, after, keySerializer, DiffableValueSerializer.getWriteOnlyInstance());
    }

    /**
     * Calculates diff between two Maps of non-diffable objects
     */
    public static <K, T, M extends Map<K, T>> MapDiff<K, T, M> diff(
        M before,
        M after,
        KeySerializer<K> keySerializer,
        ValueSerializer<K, T> valueSerializer
    ) {
        assert after != null && before != null;
        return before.equals(after) ? emptyDiff() : createDiff(before, after, keySerializer, valueSerializer);
    }

    @SuppressWarnings("unchecked")
    public static <K, T, M extends Map<K, T>> MapDiff<K, T, M> emptyDiff() {
        return (MapDiff<K, T, M>) EMPTY;
    }

    /**
     * Merges two map diffs into one unified diff with write-only value serializer.
     */
    @SuppressWarnings("unchecked")
    public static <K, T extends Diffable<T>, T1 extends T, T2 extends T, M extends Map<K, T>> MapDiff<K, T, M> merge(
        MapDiff<K, T1, ? extends ImmutableOpenMap<K, T1>> diff1,
        MapDiff<K, T2, ? extends ImmutableOpenMap<K, T2>> diff2,
        KeySerializer<K> keySerializer
    ) {
        return merge(diff1, diff2, keySerializer, DiffableValueSerializer.getWriteOnlyInstance());
    }

    /**
     * Merges two map diffs into one unified diff.
     */
    @SuppressWarnings("unchecked")
    public static <K, T, T1 extends T, T2 extends T, M extends Map<K, T>> MapDiff<K, T, M> merge(
        MapDiff<K, T1, ? extends ImmutableOpenMap<K, T1>> diff1,
        MapDiff<K, T2, ? extends ImmutableOpenMap<K, T2>> diff2,
        KeySerializer<K> keySerializer,
        ValueSerializer<K, T> valueSerializer
    ) {
        final List<K> deletes = CollectionUtils.concatLists(diff1.getDeletes(), diff2.getDeletes());
        final List<Map.Entry<K, Diff<T>>> diffs = Stream.concat(
            mapEntries(diff1.getDiffs(), diff -> (Diff<T>) diff),
            mapEntries(diff2.getDiffs(), diff -> (Diff<T>) diff)
        ).toList();
        List<Map.Entry<K, T>> upserts = Stream.concat(
            mapEntries(diff1.getUpserts(), val -> (T) val),
            mapEntries(diff2.getUpserts(), val -> (T) val)
        ).toList();
        return new MapDiff<K, T, M>(keySerializer, valueSerializer, deletes, diffs, upserts, DiffableUtils::createImmutableMapBuilder);
    }

    /**
     * Create a new MapDiff by removing the keys from any of its deletes, diffs and upserts
     */
    public static <K, T, M extends Map<K, T>> MapDiff<K, T, M> removeKeys(MapDiff<K, T, M> diff, Set<K> keys) {
        final List<K> deletes = diff.getDeletes().stream().filter(k -> keys.contains(k) == false).toList();
        final List<Map.Entry<K, Diff<T>>> diffs = diff.getDiffs().stream().filter(entry -> keys.contains(entry.getKey()) == false).toList();
        final List<Map.Entry<K, T>> upserts = diff.getUpserts().stream().filter(entry -> keys.contains(entry.getKey()) == false).toList();
        return new MapDiff<>(diff.keySerializer, diff.valueSerializer, deletes, diffs, upserts, diff.builderCtor);
    }

    /**
     * Check whether the specified MapDiff has any changes associated with the specified key
     */
    public static <K, T, M extends Map<K, T>> boolean hasKey(MapDiff<K, T, M> diff, K key) {
        if (diff.getDeletes().contains(key)) {
            return true;
        }
        if (diff.getDiffs().stream().map(Map.Entry::getKey).anyMatch(k -> Objects.equals(k, key))) {
            return true;
        }
        if (diff.getUpserts().stream().map(Map.Entry::getKey).anyMatch(k -> Objects.equals(k, key))) {
            return true;
        }
        return false;
    }

    /**
     * Create a new MapDiff from the specified MapDiff by transforming its diffs with the provided diffUpdateFunction as well as
     * transforming its upserts with the provided upsertUpdateFunction. Whether an entry should be transformed is determined by
     * the specified keyPredicate.
     * @param diff The original MapDiff
     * @param keyPredicate Determines whether an entry should be transformed
     * @param diffUpdateFunction A function to transform a Diff entry
     * @param upsertUpdateFunction A function to transform an upsert entry
     * @return A new MapDiff as a result of the transformation
     */
    public static <K, T, M extends Map<K, T>> MapDiff<K, T, M> updateDiffsAndUpserts(
        MapDiff<K, T, M> diff,
        Predicate<K> keyPredicate,
        BiFunction<K, Diff<T>, Diff<T>> diffUpdateFunction,
        BiFunction<K, T, T> upsertUpdateFunction
    ) {
        final var newDiffs = diff.getDiffs().stream().map(entry -> {
            if (keyPredicate.test(entry.getKey()) == false) {
                return entry;
            }
            return Map.entry(entry.getKey(), diffUpdateFunction.apply(entry.getKey(), entry.getValue()));
        }).toList();

        final var newUpserts = diff.getUpserts().stream().map(entry -> {
            if (keyPredicate.test(entry.getKey()) == false) {
                return entry;
            }
            return Map.entry(entry.getKey(), upsertUpdateFunction.apply(entry.getKey(), entry.getValue()));
        }).toList();

        return new MapDiff<>(diff.keySerializer, diff.valueSerializer, diff.deletes, newDiffs, newUpserts, diff.builderCtor);
    }

    /**
     * Create a new JDK map backed MapDiff by transforming the keys with the provided keyFunction.
     * @param diff Original MapDiff to transform
     * @param keyFunction Function to transform the key
     * @param keySerializer Serializer for the new key
     */
    public static <K1, K2, T extends Diffable<T>, M1 extends Map<K1, T>> MapDiff<K2, T, Map<K2, T>> jdkMapDiffWithUpdatedKeys(
        MapDiff<K1, T, M1> diff,
        Function<K1, K2> keyFunction,
        KeySerializer<K2> keySerializer
    ) {
        final List<K2> deletes = diff.getDeletes().stream().map(keyFunction).toList();
        final List<Map.Entry<K2, Diff<T>>> diffs = diff.getDiffs()
            .stream()
            .map(entry -> Map.entry(keyFunction.apply(entry.getKey()), entry.getValue()))
            .toList();
        final List<Map.Entry<K2, T>> upserts = diff.getUpserts()
            .stream()
            .map(entry -> Map.entry(keyFunction.apply(entry.getKey()), entry.getValue()))
            .toList();
        return new MapDiff<>(keySerializer, DiffableValueSerializer.getWriteOnlyInstance(), deletes, diffs, upserts, JdkMapBuilder::new);
    }

    /**
     * Creates a MapDiff that applies a single entry diff to a map
     */
    public static <K, T extends Diffable<T>, M extends Map<K, T>> MapDiff<K, T, M> singleEntryDiff(
        K key,
        Diff<T> diff,
        KeySerializer<K> keySerializer
    ) {
        return new MapDiff<>(
            keySerializer,
            DiffableValueSerializer.getWriteOnlyInstance(),
            List.of(),
            List.of(Map.entry(key, diff)),
            List.of(),
            DiffableUtils::createJdkMapBuilder
        );
    }

    /**
     * Creates a MapDiff that applies a single entry deletion to a map
     */
    public static <K, T extends Diffable<T>, M extends Map<K, T>> MapDiff<K, T, M> singleDeleteDiff(K key, KeySerializer<K> keySerializer) {
        return new MapDiff<K, T, M>(
            keySerializer,
            DiffableValueSerializer.getWriteOnlyInstance(),
            List.of(key),
            List.of(),
            List.of(),
            DiffableUtils::createJdkMapBuilder
        );
    }

    /**
     * Creates a MapDiff that applies a single entry upsert to a map
     */
    public static <K, T extends Diffable<T>, M extends Map<K, T>> MapDiff<K, T, M> singleUpsertDiff(
        K key,
        T entry,
        KeySerializer<K> keySerializer
    ) {
        return new MapDiff<>(
            keySerializer,
            DiffableValueSerializer.getWriteOnlyInstance(),
            List.of(),
            List.of(),
            List.of(Map.entry(key, entry)),
            DiffableUtils::createJdkMapBuilder
        );
    }

    private static <K, F, T> Stream<Map.Entry<K, T>> mapEntries(List<Map.Entry<K, F>> source, Function<F, T> fn) {
        return source.stream().map(e -> Map.entry(e.getKey(), fn.apply(e.getValue())));
    }

    /**
     * Split one map diff into two distinct map diffs, based on which serializer accepts a key. If both serializers accept a key,
     * the first serializer will be used.
     */
    @SuppressWarnings("unchecked")
    public static <
        K,
        T,
        T1 extends T,
        T2 extends T> Tuple<MapDiff<K, T1, ImmutableOpenMap<K, T1>>, MapDiff<K, T2, ImmutableOpenMap<K, T2>>> split(
            MapDiff<K, T, ? extends Map<K, T>> diff,
            Predicate<K> keysT1,
            ValueSerializer<K, T1> serializer1,
            Predicate<K> keysT2,
            ValueSerializer<K, T2> serializer2
        ) {
        final List<K> deletes1 = new ArrayList<>();
        final List<K> deletes2 = new ArrayList<>();
        split(diff.getDeletes(), Function.identity(), keysT1, deletes1::add, keysT2, deletes2::add);

        final List<Map.Entry<K, Diff<T1>>> diffs1 = new ArrayList<>();
        final List<Map.Entry<K, Diff<T2>>> diffs2 = new ArrayList<>();
        DiffableUtils.split(
            diff.getDiffs(),
            e -> e.getKey(),
            keysT1,
            e -> diffs1.add(Map.entry(e.getKey(), (Diff<T1>) e.getValue())),
            keysT2,
            e -> diffs2.add(Map.entry(e.getKey(), (Diff<T2>) e.getValue()))
        );

        final List<Map.Entry<K, T1>> upserts1 = new ArrayList<>();
        final List<Map.Entry<K, T2>> upserts2 = new ArrayList<>();
        DiffableUtils.split(
            diff.getUpserts(),
            e -> e.getKey(),
            keysT1,
            e -> upserts1.add(Map.entry(e.getKey(), (T1) e.getValue())),
            keysT2,
            e -> upserts2.add(Map.entry(e.getKey(), (T2) e.getValue()))
        );

        return Tuple.tuple(
            new MapDiff<>(diff.keySerializer, serializer1, deletes1, diffs1, upserts1, DiffableUtils::createImmutableMapBuilder),
            new MapDiff<>(diff.keySerializer, serializer2, deletes2, diffs2, upserts2, DiffableUtils::createImmutableMapBuilder)
        );
    }

    private static <K, E> void split(
        List<E> source,
        Function<E, K> getKey,
        Predicate<K> keys1,
        Consumer<E> dest1,
        Predicate<K> keys2,
        Consumer<E> dest2
    ) {
        for (E e : source) {
            K k = getKey.apply(e);
            if (keys1.test(k)) {
                dest1.accept(e);
            } else if (keys2.test(k)) {
                dest2.accept(e);
            } else {
                throw new IllegalStateException("Found diff key [" + k + "] which does not match [" + keys1 + "] nor [" + keys2 + "]");
            }
        }
    }

    /**
     * Loads an object that represents difference between two ImmutableOpenMaps
     */
    public static <K, T> MapDiff<K, T, ImmutableOpenMap<K, T>> readImmutableOpenMapDiff(
        StreamInput in,
        KeySerializer<K> keySerializer,
        ValueSerializer<K, T> valueSerializer
    ) throws IOException {
        return diffOrEmpty(new MapDiff<>(in, keySerializer, valueSerializer, ImmutableOpenMapBuilder::new));
    }

    /**
     * Loads an object that represents difference between two Maps of Diffable objects
     */
    public static <K, T> MapDiff<K, T, Map<K, T>> readJdkMapDiff(
        StreamInput in,
        KeySerializer<K> keySerializer,
        ValueSerializer<K, T> valueSerializer
    ) throws IOException {
        return diffOrEmpty(new MapDiff<>(in, keySerializer, valueSerializer, JdkMapBuilder::new));
    }

    /**
     * Loads an object that represents difference between two ImmutableOpenMaps of Diffable objects using Diffable proto object
     */
    public static <K, T extends Diffable<T>> MapDiff<K, T, ImmutableOpenMap<K, T>> readImmutableOpenMapDiff(
        StreamInput in,
        KeySerializer<K> keySerializer,
        DiffableValueReader<K, T> diffableValueReader
    ) throws IOException {
        return diffOrEmpty(new MapDiff<>(in, keySerializer, diffableValueReader, ImmutableOpenMapBuilder::new));
    }

    /**
     * Loads an object that represents difference between two Maps of Diffable objects using Diffable proto object
     */
    public static <K, T extends Diffable<T>> MapDiff<K, T, Map<K, T>> readJdkMapDiff(
        StreamInput in,
        KeySerializer<K> keySerializer,
        Reader<T> reader,
        Reader<Diff<T>> diffReader
    ) throws IOException {
        return diffOrEmpty(new MapDiff<>(in, keySerializer, new DiffableValueReader<>(reader, diffReader), JdkMapBuilder::new));
    }

    private static <K, T, M extends Map<K, T>> MapDiff<K, T, M> diffOrEmpty(MapDiff<K, T, M> diff) {
        // TODO: refactor map diff reading to avoid having to construct empty diffs before throwing them away here
        if (diff.getUpserts().isEmpty() && diff.getDiffs().isEmpty() && diff.getDeletes().isEmpty()) {
            return emptyDiff();
        }
        return diff;
    }

    private static <K, T, M extends Map<K, T>> MapDiff<K, T, M> createDiff(
        M before,
        M after,
        KeySerializer<K> keySerializer,
        ValueSerializer<K, T> valueSerializer
    ) {
        assert after != null && before != null;

        int inserts = 0;
        var upserts = new ArrayList<Map.Entry<K, T>>();
        var diffs = new ArrayList<Map.Entry<K, Diff<T>>>();
        for (Map.Entry<K, T> entry : after.entrySet()) {
            T previousValue = before.get(entry.getKey());
            if (previousValue == null) {
                upserts.add(entry);
                inserts++;
            } else if (entry.getValue().equals(previousValue) == false) {
                if (valueSerializer.supportsDiffableValues()) {
                    diffs.add(
                        new AbstractMap.SimpleImmutableEntry<>(entry.getKey(), valueSerializer.diff(entry.getValue(), previousValue))
                    );
                } else {
                    upserts.add(entry);
                }
            }
        }

        int expectedDeletes = before.size() + inserts - after.size();
        var deletes = new ArrayList<K>(expectedDeletes);
        if (expectedDeletes > 0) {
            for (K key : before.keySet()) {
                if (after.containsKey(key) == false) {
                    deletes.add(key);
                    if (--expectedDeletes == 0) {
                        break;
                    }
                }
            }
        }

        Function<M, MapBuilder<K, T, M>> builderCtor;
        if (before instanceof ImmutableOpenMap) {
            builderCtor = DiffableUtils::createImmutableMapBuilder;
        } else {
            builderCtor = DiffableUtils::createJdkMapBuilder;
        }

        return new MapDiff<>(keySerializer, valueSerializer, deletes, diffs, upserts, builderCtor);
    }

    @SuppressWarnings("unchecked")
    private static <K, T, M extends Map<K, T>> MapBuilder<K, T, M> createImmutableMapBuilder(Map<K, T> m) {
        assert m instanceof ImmutableOpenMap<K, T>;
        return (MapBuilder<K, T, M>) new ImmutableOpenMapBuilder<>((ImmutableOpenMap<K, T>) m);
    }

    @SuppressWarnings("unchecked")
    private static <K, T, M extends Map<K, T>> MapBuilder<K, T, M> createJdkMapBuilder(Map<K, T> m) {
        return (MapBuilder<K, T, M>) new JdkMapBuilder<>(m);
    }

    private interface MapBuilder<K, T, M extends Map<K, T>> {
        void remove(K key);

        T get(K key);

        void put(K key, T value);

        M build();
    }

    private static class JdkMapBuilder<K, T> implements MapBuilder<K, T, Map<K, T>> {
        private final Map<K, T> map;

        JdkMapBuilder(Map<K, T> map) {
            this.map = new HashMap<>(map);
        }

        @Override
        public void remove(K key) {
            map.remove(key);
        }

        @Override
        public T get(K key) {
            return map.get(key);
        }

        @Override
        public void put(K key, T value) {
            map.put(key, value);
        }

        @Override
        public Map<K, T> build() {
            return Collections.unmodifiableMap(map);
        }
    }

    private static class ImmutableOpenMapBuilder<K, T> implements MapBuilder<K, T, ImmutableOpenMap<K, T>> {
        private final ImmutableOpenMap.Builder<K, T> builder;

        ImmutableOpenMapBuilder(ImmutableOpenMap<K, T> map) {
            this.builder = ImmutableOpenMap.builder(map);
        }

        @Override
        public void remove(K key) {
            builder.remove(key);
        }

        @Override
        public T get(K key) {
            return builder.get(key);
        }

        @Override
        public void put(K key, T value) {
            builder.put(key, value);
        }

        @Override
        public ImmutableOpenMap<K, T> build() {
            return builder.build();
        }
    }

    /**
     * Represents differences between two maps of objects and is used as base class for different map implementations.
     *
     * Implements serialization. How differences are applied is left to subclasses.
     *
     * @param <K> the type of map keys
     * @param <T> the type of map values
     */
    public static class MapDiff<K, T, M extends Map<K, T>> implements Diff<M> {

        protected final List<K> deletes;
        protected final List<Map.Entry<K, Diff<T>>> diffs; // incremental updates
        protected final List<Map.Entry<K, T>> upserts; // additions or full updates
        protected final KeySerializer<K> keySerializer;
        protected final ValueSerializer<K, T> valueSerializer;
        private final Function<M, MapBuilder<K, T, M>> builderCtor;

        private MapDiff(
            KeySerializer<K> keySerializer,
            ValueSerializer<K, T> valueSerializer,
            List<K> deletes,
            List<Map.Entry<K, Diff<T>>> diffs,
            List<Map.Entry<K, T>> upserts,
            Function<M, MapBuilder<K, T, M>> builderCtor
        ) {
            this.keySerializer = keySerializer;
            this.valueSerializer = valueSerializer;
            this.deletes = deletes;
            this.diffs = diffs;
            this.upserts = upserts;
            this.builderCtor = builderCtor;
        }

        private MapDiff(
            StreamInput in,
            KeySerializer<K> keySerializer,
            ValueSerializer<K, T> valueSerializer,
            Function<M, MapBuilder<K, T, M>> builderCtor
        ) throws IOException {
            this.keySerializer = keySerializer;
            this.valueSerializer = valueSerializer;
            deletes = in.readCollectionAsList(keySerializer::readKey);
            int diffsCount = in.readVInt();
            diffs = diffsCount == 0 ? List.of() : new ArrayList<>(diffsCount);
            for (int i = 0; i < diffsCount; i++) {
                K key = keySerializer.readKey(in);
                Diff<T> diff = valueSerializer.readDiff(in, key);
                diffs.add(new AbstractMap.SimpleImmutableEntry<>(key, diff));
            }
            int upsertsCount = in.readVInt();
            upserts = upsertsCount == 0 ? List.of() : new ArrayList<>(upsertsCount);
            for (int i = 0; i < upsertsCount; i++) {
                K key = keySerializer.readKey(in);
                T newValue = valueSerializer.read(in, key);
                upserts.add(new AbstractMap.SimpleImmutableEntry<>(key, newValue));
            }
            this.builderCtor = builderCtor;
        }

        @Override
        public M apply(M map) {
            MapBuilder<K, T, M> builder = builderCtor.apply(map);

            for (K part : deletes) {
                builder.remove(part);
            }

            for (Map.Entry<K, Diff<T>> diff : diffs) {
                builder.put(diff.getKey(), diff.getValue().apply(builder.get(diff.getKey())));
            }

            for (Map.Entry<K, T> upsert : upserts) {
                builder.put(upsert.getKey(), upsert.getValue());
            }
            return builder.build();
        }

        /**
         * {@code true} if this diff results in no changes to the map
         */
        public boolean isEmpty() {
            return deletes.isEmpty() && diffs.isEmpty() && upserts.isEmpty();
        }

        /**
         * The keys that, when this diff is applied to a map, should be removed from the map.
         *
         * @return the list of keys that are deleted
         */
        public List<K> getDeletes() {
            return deletes;
        }

        /**
         * Map entries that, when this diff is applied to a map, should be
         * incrementally updated. The incremental update is represented using
         * the {@link Diff} interface.
         *
         * @return the map entries that are incrementally updated
         */
        public List<Map.Entry<K, Diff<T>>> getDiffs() {
            return diffs;
        }

        /**
         * Map entries that, when this diff is applied to a map, should be
         * added to the map or fully replace the previous value.
         *
         * @return the map entries that are additions or full updates
         */
        public List<Map.Entry<K, T>> getUpserts() {
            return upserts;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeCollection(deletes, (o, v) -> keySerializer.writeKey(v, o));
            TransportVersion version = out.getTransportVersion();
            // filter out custom states not supported by the other node
            int diffCount = 0;
            for (Map.Entry<K, Diff<T>> diff : diffs) {
                if (valueSerializer.supportsVersion(diff.getValue(), version)) {
                    diffCount++;
                }
            }
            out.writeVInt(diffCount);
            for (Map.Entry<K, Diff<T>> entry : diffs) {
                if (valueSerializer.supportsVersion(entry.getValue(), version)) {
                    keySerializer.writeKey(entry.getKey(), out);
                    valueSerializer.writeDiff(entry.getValue(), out);
                }
            }
            // filter out custom states not supported by the other node
            int upsertsCount = 0;
            for (Map.Entry<K, T> upsert : upserts) {
                if (valueSerializer.supportsVersion(upsert.getValue(), version)) {
                    upsertsCount++;
                }
            }
            out.writeVInt(upsertsCount);
            for (Map.Entry<K, T> entry : upserts) {
                if (valueSerializer.supportsVersion(entry.getValue(), version)) {
                    keySerializer.writeKey(entry.getKey(), out);
                    valueSerializer.write(entry.getValue(), out);
                }
            }
        }
    }

    /**
     * Provides read and write operations to serialize keys of map
     * @param <K> type of key
     */
    public interface KeySerializer<K> {
        void writeKey(K key, StreamOutput out) throws IOException;

        K readKey(StreamInput in) throws IOException;
    }

    /**
     * Serializes String keys of a map
     */
    private static final class StringKeySerializer implements KeySerializer<String> {
        private static final StringKeySerializer INSTANCE = new StringKeySerializer();

        @Override
        public void writeKey(String key, StreamOutput out) throws IOException {
            out.writeString(key);
        }

        @Override
        public String readKey(StreamInput in) throws IOException {
            return in.readString();
        }
    }

    /**
     * Serializes Integer keys of a map as an Int
     */
    private static final class IntKeySerializer implements KeySerializer<Integer> {
        public static final IntKeySerializer INSTANCE = new IntKeySerializer();

        @Override
        public void writeKey(Integer key, StreamOutput out) throws IOException {
            out.writeInt(key);
        }

        @Override
        public Integer readKey(StreamInput in) throws IOException {
            return in.readInt();
        }
    }

    /**
     * Serializes Integer keys of a map as a VInt. Requires keys to be positive.
     */
    private static final class VIntKeySerializer implements KeySerializer<Integer> {
        public static final IntKeySerializer INSTANCE = new IntKeySerializer();

        @Override
        public void writeKey(Integer key, StreamOutput out) throws IOException {
            if (key < 0) {
                throw new IllegalArgumentException("Map key [" + key + "] must be positive");
            }
            out.writeVInt(key);
        }

        @Override
        public Integer readKey(StreamInput in) throws IOException {
            return in.readVInt();
        }
    }

    /**
     * Serializes Writeable keys of a map. Requires keys to be non-null.
     */
    private static class WriteableKeySerializer<W extends Writeable> implements KeySerializer<W> {
        private final Reader<W> reader;

        WriteableKeySerializer(Reader<W> reader) {
            this.reader = reader;
        }

        @Override
        public void writeKey(W key, StreamOutput out) throws IOException {
            out.writeWriteable(key);
        }

        @Override
        public W readKey(StreamInput in) throws IOException {
            return reader.read(in);
        }
    }

    /**
     * Provides read and write operations to serialize map values.
     * Reading of values can be made dependent on map key.
     *
     * Also provides operations to distinguish whether map values are diffable.
     *
     * Should not be directly implemented, instead implement either
     * {@link DiffableValueSerializer} or {@link NonDiffableValueSerializer}.
     *
     * @param <K> key type of map
     * @param <V> value type of map
     */
    public interface ValueSerializer<K, V> {

        /**
         * Writes value to stream
         */
        void write(V value, StreamOutput out) throws IOException;

        /**
         * Reads value from stream. Reading operation can be made dependent on map key.
         */
        V read(StreamInput in, K key) throws IOException;

        /**
         * Whether this serializer supports diffable values
         */
        boolean supportsDiffableValues();

        /**
         * Whether this serializer supports the version of the output stream
         */
        default boolean supportsVersion(Diff<V> value, TransportVersion version) {
            return true;
        }

        /**
         * Whether this serializer supports the version of the output stream
         */
        default boolean supportsVersion(V value, TransportVersion version) {
            return true;
        }

        /**
         * Computes diff if this serializer supports diffable values
         */
        Diff<V> diff(V value, V beforePart);

        /**
         * Writes value as diff to stream if this serializer supports diffable values
         */
        void writeDiff(Diff<V> value, StreamOutput out) throws IOException;

        /**
         * Reads value as diff from stream if this serializer supports diffable values.
         * Reading operation can be made dependent on map key.
         */
        Diff<V> readDiff(StreamInput in, K key) throws IOException;
    }

    /**
     * Serializer for Diffable map values. Needs to implement read and readDiff methods.
     *
     * @param <K> type of map keys
     * @param <V> type of map values
     */
    public abstract static class DiffableValueSerializer<K, V extends Diffable<V>> implements ValueSerializer<K, V> {
        @SuppressWarnings("rawtypes")
        private static final DiffableValueSerializer WRITE_ONLY_INSTANCE = new DiffableValueSerializer() {
            @Override
            public Object read(StreamInput in, Object key) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Diff<Object> readDiff(StreamInput in, Object key) {
                throw new UnsupportedOperationException();
            }
        };

        @SuppressWarnings("unchecked")
        private static <K, V extends Diffable<V>> DiffableValueSerializer<K, V> getWriteOnlyInstance() {
            return WRITE_ONLY_INSTANCE;
        }

        @Override
        public boolean supportsDiffableValues() {
            return true;
        }

        @Override
        public Diff<V> diff(V value, V beforePart) {
            return value.diff(beforePart);
        }

        @Override
        public void write(V value, StreamOutput out) throws IOException {
            value.writeTo(out);
        }

        @Override
        public void writeDiff(Diff<V> value, StreamOutput out) throws IOException {
            value.writeTo(out);
        }
    }

    /**
     * Serializer for non-diffable map values
     *
     * @param <K> type of map keys
     * @param <V> type of map values
     */
    public abstract static class NonDiffableValueSerializer<K, V> implements ValueSerializer<K, V> {
        @Override
        public boolean supportsDiffableValues() {
            return false;
        }

        @Override
        public Diff<V> diff(V value, V beforePart) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeDiff(Diff<V> value, StreamOutput out) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Diff<V> readDiff(StreamInput in, K key) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Implementation of the ValueSerializer that wraps value and diff readers.
     *
     * Note: this implementation is ignoring the key.
     */
    public static class DiffableValueReader<K, V extends Diffable<V>> extends DiffableValueSerializer<K, V> {
        private final Reader<V> reader;
        private final Reader<Diff<V>> diffReader;

        public DiffableValueReader(Reader<V> reader, Reader<Diff<V>> diffReader) {
            this.reader = reader;
            this.diffReader = diffReader;
        }

        @Override
        public V read(StreamInput in, K key) throws IOException {
            return reader.read(in);
        }

        @Override
        public Diff<V> readDiff(StreamInput in, K key) throws IOException {
            return diffReader.read(in);
        }
    }

    /**
     * Implementation of ValueSerializer that serializes immutable sets
     *
     * @param <K> type of map key
     */
    @SuppressWarnings("rawtypes")
    public static class StringSetValueSerializer<K> extends NonDiffableValueSerializer<K, Set<String>> {
        private static final StringSetValueSerializer INSTANCE = new StringSetValueSerializer();

        @SuppressWarnings("unchecked")
        public static <K> StringSetValueSerializer<K> getInstance() {
            return INSTANCE;
        }

        @Override
        public void write(Set<String> value, StreamOutput out) throws IOException {
            out.writeStringCollection(value);
        }

        @Override
        public Set<String> read(StreamInput in, K key) throws IOException {
            return Set.of(in.readStringArray());
        }
    }
}

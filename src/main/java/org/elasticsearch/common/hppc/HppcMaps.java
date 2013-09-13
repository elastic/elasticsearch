package org.elasticsearch.common.hppc;

import com.carrotsearch.hppc.ObjectIntOpenHashMap;
import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import org.elasticsearch.ElasticSearchIllegalArgumentException;

/**
 */
public final class HppcMaps {

    private HppcMaps() {
    }

    /**
     * Returns a new map with the given initial capacity
     */
    public static <K, V> ObjectObjectOpenHashMap<K, V> newMap(int capacity) {
        return new ObjectObjectOpenHashMap<K, V>(capacity);
    }

    /**
     * Returns a new map with a default initial capacity of
     * {@value com.carrotsearch.hppc.HashContainerUtils#DEFAULT_CAPACITY}
     */
    public static <K, V> ObjectObjectOpenHashMap<K, V> newMap() {
        return newMap(16);
    }

    /**
     * Returns a map like {@link #newMap()} that does not accept <code>null</code> keys
     */
    public static <K, V> ObjectObjectOpenHashMap<K, V> newNoNullKeysMap() {
        return ensureNoNullKeys(16);
    }

    /**
     * Returns a map like {@link #newMap(int)} that does not accept <code>null</code> keys
     */
    public static <K, V> ObjectObjectOpenHashMap<K, V> newNoNullKeysMap(int capacity) {
        return ensureNoNullKeys(capacity);
    }

    /**
     * Wraps the given map and prevent adding of <code>null</code> keys.
     */
    public static <K, V> ObjectObjectOpenHashMap<K, V> ensureNoNullKeys(int capacity) {
        return new ObjectObjectOpenHashMap<K, V>(capacity) {

            @Override
            public V put(K key, V value) {
                if (key == null) {
                    throw new ElasticSearchIllegalArgumentException("Map key must not be null");
                }
                return super.put(key, value);
            }

        };
    }

    public final static class Object {

        public final static class Integer {

            public static <V> ObjectIntOpenHashMap<V> ensureNoNullKeys(int capacity, float loadFactor) {
                return new ObjectIntOpenHashMap<V>(capacity, loadFactor) {

                    @Override
                    public int put(V key, int value) {
                        if (key == null) {
                            throw new ElasticSearchIllegalArgumentException("Map key must not be null");
                        }
                        return super.put(key, value);
                    }
                };
            }

        }

    }

}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.ingest.IngestDocument;

import java.time.ZonedDateTime;
import java.util.AbstractCollection;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * Map containing ingest source and metadata.
 *
 * The Metadata values in {@link IngestDocument.Metadata} are validated when put in the map.
 * _index, _id and _routing must be a String or null
 * _version_type must be a lower case VersionType or null
 * _version must be representable as a long without loss of precision and may not be null
 *
 * The map is expected to be used by processors where-as the typed getter and setters should
 * be used by server code where possible.
 * ...
 * IngestSourceAndMetadata is an implementation of {@code Map<String, Object>} that has a validating functions for a subset of keys.
 *
 * The validators are BiFunctions that should either return the identity or throw an {@link IllegalArgumentException}.  The
 * arguments to the validator are the key and the proposed value or {@code null} for key removal.
 *
 * Validation occurs everywhere updates can happen, either directly via the {@link #put(String, Object)}, {@link #replace(Object, Object)},
 * {@link #merge(Object, Object, BiFunction)} family of methods, via {@link Map.Entry#setValue(Object)},
 * via the linked Set from {@link #entrySet()} or {@link Collection#remove(Object)} from the linked collection via {@link #values()}
 */
public class IngestSourceAndMetadata extends AbstractMap<String, Object> {
    protected final ZonedDateTime timestamp;
    // protected static final BiFunction<String, Object, Object> IDENTITY = (k, v) -> v;

    // map of key to validating function. Should throw {@link IllegalArgumentException} on invalid value, otherwise return identity
    static final Map<String, BiFunction<String, Object, Object>> VALIDATORS = Map.of(
        IngestDocument.Metadata.INDEX.getFieldName(),
        IngestSourceAndMetadata::stringValidator,
        IngestDocument.Metadata.ID.getFieldName(),
        IngestSourceAndMetadata::stringValidator,
        IngestDocument.Metadata.ROUTING.getFieldName(),
        IngestSourceAndMetadata::stringValidator,
        IngestDocument.Metadata.VERSION.getFieldName(),
        IngestSourceAndMetadata::longValidator,
        IngestDocument.Metadata.VERSION_TYPE.getFieldName(),
        IngestSourceAndMetadata::versionTypeValidator,
        IngestDocument.Metadata.DYNAMIC_TEMPLATES.getFieldName(),
        IngestSourceAndMetadata::mapValidator,
        IngestDocument.Metadata.IF_SEQ_NO.getFieldName(),
        IngestSourceAndMetadata::longValidator,
        IngestDocument.Metadata.IF_PRIMARY_TERM.getFieldName(),
        IngestSourceAndMetadata::longValidator
    );

    protected final Map<String, Object> source;
    protected final Map<String, Object> metadata;
    protected final Map<String, BiFunction<String, Object, Object>> validators;
    private EntrySet entrySet;

    public IngestSourceAndMetadata(
        String index,
        String id,
        long version,
        String routing,
        VersionType versionType,
        ZonedDateTime timestamp,
        Map<String, Object> source
    ) {
        this(new HashMap<>(source), metadataMap(index, id, version, routing, versionType), timestamp, VALIDATORS);
    }

    /**
     * Creates an {@code IngestSourceAndMetadata} from the given source, metadata and timestamp
     */
    public IngestSourceAndMetadata(Map<String, Object> source, Map<String, Object> metadata, ZonedDateTime timestamp) {
        this(source, metadata, timestamp, VALIDATORS);
    }

    IngestSourceAndMetadata(
        Map<String, Object> source,
        Map<String, Object> metadata,
        ZonedDateTime timestamp,
        Map<String, BiFunction<String, Object, Object>> validators
    ) {
        this.source = source != null ? source : new HashMap<>();
        this.metadata = metadata != null ? metadata : new HashMap<>();
        this.timestamp = timestamp;
        this.validators = validators;
        validateMetadata();
    }

    /**
     * Create a IngestSourceAndMetadata using the underlying map and set of validators.  The validators are applied to the map to ensure
     * the incoming map matches the invariants enforced by the validators.
     * @param sourceAndMetadata the wrapped map.  Should not be externally modified after creation.  This map is modified to remove
     *                          metadata values and will become source
     * @param timestamp the timestamp of ingestion
     * @throws IllegalArgumentException if a validator fails for a given key
     */
    public static IngestSourceAndMetadata ofMixedSourceAndMetadata(Map<String, Object> sourceAndMetadata, ZonedDateTime timestamp) {
        Tuple<Map<String, Object>, Map<String, Object>> split = splitSourceAndMetadata(sourceAndMetadata);
        return new IngestSourceAndMetadata(split.v1(), split.v2(), timestamp, VALIDATORS);
    }

    /**
     * Copy constructor
     */
    public static IngestSourceAndMetadata copy(IngestSourceAndMetadata ingestSourceAndMetadata) {
        return new IngestSourceAndMetadata(
            IngestDocument.deepCopyMap(ingestSourceAndMetadata.source),
            IngestDocument.deepCopyMap(ingestSourceAndMetadata.metadata),
            ingestSourceAndMetadata.timestamp,
            ingestSourceAndMetadata.validators
        );
    }

    protected static Map<String, Object> metadataMap(String index, String id, long version, String routing, VersionType versionType) {
        Map<String, Object> metadata = Maps.newHashMapWithExpectedSize(IngestDocument.Metadata.values().length);
        metadata.put(IngestDocument.Metadata.INDEX.getFieldName(), index);
        metadata.put(IngestDocument.Metadata.ID.getFieldName(), id);
        metadata.put(IngestDocument.Metadata.VERSION.getFieldName(), version);
        if (routing != null) {
            metadata.put(IngestDocument.Metadata.ROUTING.getFieldName(), routing);
        }
        if (versionType != null) {
            metadata.put(IngestDocument.Metadata.VERSION_TYPE.getFieldName(), VersionType.toString(versionType));
        }
        return metadata;
    }

    /**
     * Returns a new metadata map and the existing source map with metadata removed.
     */
    public static Tuple<Map<String, Object>, Map<String, Object>> splitSourceAndMetadata(Map<String, Object> sourceAndMetadata) {
        if (sourceAndMetadata instanceof IngestSourceAndMetadata ingestSourceAndMetadata) {
            return new Tuple<>(new HashMap<>(ingestSourceAndMetadata.source), new HashMap<>(ingestSourceAndMetadata.metadata));
        }
        Map<String, Object> metadata = Maps.newHashMapWithExpectedSize(IngestDocument.Metadata.values().length);
        Map<String, Object> source = new HashMap<>(sourceAndMetadata);
        for (String metadataName : VALIDATORS.keySet()) {
            if (sourceAndMetadata.containsKey(metadataName)) {
                metadata.put(metadataName, source.remove(metadataName));
            }
        }
        return new Tuple<>(source, metadata);
    }

    public Map<String, Object> getSource() {
        return source;
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    // These are available to scripts

    public String getIndex() {
        return getString(IngestDocument.Metadata.INDEX.getFieldName());
    }

    public void setIndex(String index) {
        put(IngestDocument.Metadata.INDEX.getFieldName(), index);
    }

    public String getId() {
        return getString(IngestDocument.Metadata.ID.getFieldName());
    }

    public void setId(String id) {
        put(IngestDocument.Metadata.ID.getFieldName(), id);
    }

    public String getRouting() {
        return getString(IngestDocument.Metadata.ROUTING.getFieldName());
    }

    public void setRouting(String routing) {
        put(IngestDocument.Metadata.ROUTING.getFieldName(), routing);
    }

    public String getVersionType() {
        return getString(IngestDocument.Metadata.VERSION_TYPE.getFieldName());
    }

    public void setVersionType(String versionType) {
        put(IngestDocument.Metadata.VERSION_TYPE.getFieldName(), versionType);
    }

    public long getVersion() {
        Number version = getNumber(IngestDocument.Metadata.VERSION.getFieldName());
        assert version != null : IngestDocument.Metadata.VERSION.getFieldName() + " validation allowed null version";
        return version.longValue();
    }

    public void setVersion(long version) {
        put(IngestDocument.Metadata.VERSION.getFieldName(), version);
    }

    // timestamp isn't backed by the map
    public ZonedDateTime getTimestamp() {
        return timestamp;
    }

    // These are not available to scripts
    public Number getIfSeqNo() {
        return getNumber(IngestDocument.Metadata.IF_SEQ_NO.getFieldName());
    }

    public Number getIfPrimaryTerm() {
        return getNumber(IngestDocument.Metadata.IF_PRIMARY_TERM.getFieldName());
    }

    @SuppressWarnings("unchecked")
    public Map<String, String> getDynamicTemplates() {
        return (Map<String, String>) mapValidator(
            IngestDocument.Metadata.DYNAMIC_TEMPLATES.getFieldName(),
            get(IngestDocument.Metadata.DYNAMIC_TEMPLATES)
        );
    }

    protected void validateMetadata() {
        validators.forEach((k, v) -> {
            v.apply(k, metadata.get(k));
            if (source.containsKey(k)) {
                throw new IllegalArgumentException("Unexpected metadata key [" + k + "] in source with value [" + source.get(k) + "]");
            }
        });
    }

    /**
     * Returns an entrySet that respects the validators of the map.
     */
    @Override
    public Set<Map.Entry<String, Object>> entrySet() {
        if (entrySet == null) {
            entrySet = new EntrySet(source.entrySet(), metadata.entrySet());
        }
        return entrySet;
    }

    /**
     * Associate a key with a value.  If the key has a validator, it is applied before association.
     * @throws IllegalArgumentException if value does not pass validation for the given key
     */
    @Override
    public Object put(String key, Object value) {
        BiFunction<String, Object, Object> validator = validators.get(key);
        if (validator != null) {
            return metadata.put(key, validator.apply(key, value));
        }
        return source.put(key, value);
    }

    /**
     * Remove the mapping of key.  If the key has a validator, it is checked before key removal.
     * @throws IllegalArgumentException if the validator does not allow the key to be removed
     */
    @Override
    public Object remove(Object key) {
        // uses map directly to avoid AbstractMaps linear time implementation using entrySet()
        if (key instanceof String strKey) {
            BiFunction<String, Object, Object> validator = validators.get(key);
            if (validator != null) {
                validator.apply(strKey, null);
                return metadata.remove(key);
            }
        }
        return source.remove(key);
    }

    /**
     * Clear entire map.  For each key in the map with a validator, that validator is checked as in {@link #remove(Object)}.
     * @throws IllegalArgumentException if any validator does not allow the key to be removed, in this case the map is unmodified
     */
    @Override
    public void clear() {
        // AbstractMap uses entrySet().clear(), it should be quicker to run through the validators, then call the wrapped map's clear
        validators.forEach((k, v) -> {
            if (metadata.containsKey(k)) {
                v.apply(k, null);
            }
        });
        metadata.clear();
        source.clear();
    }

    @Override
    public int size() {
        // uses map directly to avoid creating an EntrySet via AbstractMaps implementation, which returns entrySet().size()
        return source.size() + metadata.size();
    }

    @Override
    public boolean containsValue(Object value) {
        // uses map directly to avoid AbstractMaps linear time implementation using entrySet()
        return metadata.containsValue(value) || source.containsValue(value);
    }

    @Override
    public boolean containsKey(Object key) {
        // uses map directly to avoid AbstractMaps linear time implementation using entrySet()
        return metadata.containsKey(key) || source.containsKey(key);
    }

    @Override
    public Object get(Object key) {
        // uses map directly to avoid AbstractMaps linear time implementation using entrySet()
        if (validators.get(key) != null) {
            return metadata.get(key);
        }
        return source.get(key);
    }

    /**
     * Get the String version of the value associated with {@code key}, or null
     */
    public String getString(Object key) {
        return Objects.toString(get(key), null);
    }

    /**
     * Get the {@link Number} associated with key, or null
     * @throws IllegalArgumentException if the value is not a {@link Number}
     */
    public Number getNumber(Object key) {
        Object value = get(key);
        if (value == null) {
            return null;
        }
        if (value instanceof Number number) {
            return number;
        }
        throw new IllegalArgumentException(
            "unexpected type for [" + key + "] with value [" + value + "], expected Number, got [" + value.getClass().getName() + "]"
        );
    }

    /**
     * Set of entries of the wrapped map that calls the appropriate validator before changing an entries value or removing an entry.
     *
     * Inherits {@link AbstractSet#removeAll(Collection)}, which calls the overridden {@link #remove(Object)} which performs validation.
     *
     * Inherits {@link AbstractCollection#retainAll(Collection)} and {@link AbstractCollection#clear()}, which both use
     * {@link EntrySetIterator#remove()} for removal.
     */
    class EntrySet extends AbstractSet<Map.Entry<String, Object>> {
        Set<Map.Entry<String, Object>> sourceSet;
        Set<Map.Entry<String, Object>> metadataSet;

        EntrySet(Set<Map.Entry<String, Object>> sourceSet, Set<Map.Entry<String, Object>> metadataSet) {
            this.sourceSet = sourceSet;
            this.metadataSet = metadataSet;
        }

        @Override
        public Iterator<Map.Entry<String, Object>> iterator() {
            return new EntrySetIterator(sourceSet.iterator(), metadataSet.iterator());
        }

        @Override
        public int size() {
            return sourceSet.size() + metadataSet.size();
        }

        @Override
        public boolean remove(Object o) {
            if (o instanceof Map.Entry<?, ?> entry) {
                if (entry.getKey()instanceof String key) {
                    BiFunction<String, Object, Object> validator = validators.get(key);
                    if (validator != null) {
                        validator.apply(key, entry.getValue());
                        return metadataSet.remove(o);
                    }
                }
            }
            return sourceSet.remove(o);
        }
    }

    /**
     * Iterator over the wrapped map that returns a validating {@link Entry} on {@link #next()} and validates on {@link #remove()}.
     *
     * {@link #remove()} is called by remove in {@link AbstractMap#values()}, {@link AbstractMap#keySet()}, {@link AbstractMap#clear()} via
     * {@link AbstractSet#clear()}
     */
    class EntrySetIterator implements Iterator<Map.Entry<String, Object>> {
        final Iterator<Map.Entry<String, Object>> metadataIter;
        final Iterator<Map.Entry<String, Object>> sourceIter;

        boolean sourceCur = true;
        Entry cur;

        EntrySetIterator(Iterator<Map.Entry<String, Object>> metadataIter, Iterator<Map.Entry<String, Object>> sourceIter) {
            this.metadataIter = metadataIter;
            this.sourceIter = sourceIter;
        }

        @Override
        public boolean hasNext() {
            return sourceIter.hasNext() || metadataIter.hasNext();
        }

        @Override
        public Map.Entry<String, Object> next() {
            sourceCur = sourceIter.hasNext();
            return cur = new Entry(sourceCur ? sourceIter.next() : metadataIter.next(), sourceCur);
        }

        /**
         * Remove current entry from the backing Map.  Checks the Entry's key's validator, if one exists, before removal.
         * @throws IllegalArgumentException if the validator does not allow the Entry to be removed
         */
        @Override
        public void remove() {
            if (cur == null) {
                return;
            }
            if (sourceCur) {
                sourceIter.remove();
            } else {
                BiFunction<String, Object, Object> validator = validators.get(cur.getKey());
                if (validator != null) {
                    validator.apply(cur.getKey(), null);
                }
                metadataIter.remove();
            }
        }
    }

    /**
     * Wrapped Map.Entry that calls the key's validator on {@link #setValue(Object)}
     */
    class Entry implements Map.Entry<String, Object> {
        final Map.Entry<String, Object> entry;
        final boolean isSource;

        Entry(Map.Entry<String, Object> entry, boolean isSource) {
            this.entry = entry;
            this.isSource = isSource;
        }

        @Override
        public String getKey() {
            return entry.getKey();
        }

        @Override
        public Object getValue() {
            return entry.getValue();
        }

        /**
         * Associate the value with the Entry's key in the linked Map.  If the Entry's key has a validator, it is applied before association
         * @throws IllegalArgumentException if value does not pass validation for the Entry's key
         */
        @Override
        public Object setValue(Object value) {
            if (isSource == false) {
                BiFunction<String, Object, Object> validator = validators.get(entry.getKey());
                if (validator != null) {
                    validator.apply(entry.getKey(), value);
                }
            }
            return entry.setValue(value);
        }
    }

    /**
     * Allow a String or null
     */
    protected static String stringValidator(String key, Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof String str) {
            return str;
        }
        throw new IllegalArgumentException(
            key + " must be null or a String but was [" + value + "] with type [" + value.getClass().getName() + "]"
        );
    }

    /**
     * Allow Numbers that can be represented as longs without loss of precision
     */
    protected static Long longValidator(String key, Object value) {
        if (value == null) {
            return null; // Allow null version for now
        }
        if (value instanceof Number number) {
            long version = number.longValue();
            if (number.doubleValue() != version) {
                // did we round?
                throw new IllegalArgumentException(
                    key + " may only be set to an int or a long but was [" + number + "] with type [" + value.getClass().getName() + "]"
                );
            }
            return version;
        }
        throw new IllegalArgumentException(
            "_version may only be set to an int or a long but was [" + value + "] with type [" + value.getClass().getName() + "]"
        );
    }

    /**
     * Allow lower case Strings that map to VersionType values, or null
     */
    protected static String versionTypeValidator(String key, Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof String versionType) {
            VersionType.fromString(versionType);
            return versionType;
        }
        throw new IllegalArgumentException(
            key
                + " must be a null or one of ["
                + Arrays.stream(VersionType.values()).map(vt -> VersionType.toString(vt)).collect(Collectors.joining(","))
                + "]"
        );
    }

    /**
     * Allow maps
     */
    protected static Map<?, ?> mapValidator(String key, Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Map<?, ?> map) {
            return map;
        }
        throw new IllegalArgumentException(
            key + " must be a null or a Map but was [" + value + "] with type [" + value.getClass().getName() + "]"
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if ((o instanceof IngestSourceAndMetadata) == false) return false;
        if (super.equals(o) == false) return false;
        IngestSourceAndMetadata that = (IngestSourceAndMetadata) o;
        return Objects.equals(timestamp, that.timestamp)
            && source.equals(that.source)
            && metadata.equals(that.metadata)
            && validators.equals(that.validators);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, source, metadata, validators);
    }
}

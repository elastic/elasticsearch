/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest;

import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.script.Metadata;

import java.time.ZonedDateTime;
import java.util.AbstractCollection;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * Map containing ingest source and metadata.
 *
 * The Metadata values in {@link IngestDocument.Metadata} are validated when put in the map.
 * _index, _id and _routing must be a String or null
 * _version_type must be a lower case VersionType or null
 * _version must be representable as a long without loss of precision or null
 * _dyanmic_templates must be a map
 * _if_seq_no must be a long or null
 * _if_primary_term must be a long or null
 *
 * The map is expected to be used by processors, server code should the typed getter and setters where possible.
 */
class IngestSourceAndMetadata extends AbstractMap<String, Object> implements Metadata {
    protected final ZonedDateTime timestamp;

    /**
     * map of key to validating function. Should throw {@link IllegalArgumentException} on invalid value
     */
    static final Map<String, BiConsumer<String, Object>> VALIDATORS = Map.of(
        IngestDocument.Metadata.INDEX.getFieldName(),
        IngestSourceAndMetadata::stringValidator,
        IngestDocument.Metadata.ID.getFieldName(),
        IngestSourceAndMetadata::stringValidator,
        IngestDocument.Metadata.ROUTING.getFieldName(),
        IngestSourceAndMetadata::stringValidator,
        IngestDocument.Metadata.VERSION.getFieldName(),
        IngestSourceAndMetadata::versionValidator,
        IngestDocument.Metadata.VERSION_TYPE.getFieldName(),
        IngestSourceAndMetadata::versionTypeValidator,
        IngestDocument.Metadata.DYNAMIC_TEMPLATES.getFieldName(),
        IngestSourceAndMetadata::mapValidator,
        IngestDocument.Metadata.IF_SEQ_NO.getFieldName(),
        IngestSourceAndMetadata::longValidator,
        IngestDocument.Metadata.IF_PRIMARY_TERM.getFieldName(),
        IngestSourceAndMetadata::longValidator,
        IngestDocument.Metadata.TYPE.getFieldName(),
        IngestSourceAndMetadata::stringValidator
    );

    protected final Map<String, Object> source;
    protected final Map<String, Object> metadata;
    protected final Map<String, BiConsumer<String, Object>> validators;
    private EntrySet entrySet; // cache to avoid recreation

    /**
     * Create an IngestSourceAndMetadata with the given metadata, source and default validators
     */
    IngestSourceAndMetadata(
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
     * Create IngestSourceAndMetadata with custom validators.
     *
     * @param source the source document map
     * @param metadata the metadata map
     * @param timestamp the time of ingestion
     * @param validators validators to run on metadata map, if a key is in this map, the value is stored in metadata.
     *                   if null, use the default validators from {@link #VALIDATORS}
     */
    IngestSourceAndMetadata(
        Map<String, Object> source,
        Map<String, Object> metadata,
        ZonedDateTime timestamp,
        Map<String, BiConsumer<String, Object>> validators
    ) {
        this.source = source != null ? source : new HashMap<>();
        this.metadata = metadata != null ? metadata : new HashMap<>();
        this.timestamp = timestamp;
        this.validators = validators != null ? validators : VALIDATORS;
        validateMetadata();
    }

    /**
     * Create the backing metadata map with the standard contents assuming default validators.
     */
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

    /**
     * Fetch the timestamp from the ingestMetadata, if it exists
     * @return the timestamp for the document or null
     */
    public static ZonedDateTime getTimestamp(Map<String, Object> ingestMetadata) {
        if (ingestMetadata == null) {
            return null;
        }
        Object ts = ingestMetadata.get(IngestDocument.TIMESTAMP);
        if (ts instanceof ZonedDateTime timestamp) {
            return timestamp;
        } else if (ts instanceof String str) {
            return ZonedDateTime.parse(str);
        }
        return null;
    }

    /**
     * get the source map, if externally modified then the guarantees of this class are not enforced
     */
    public Map<String, Object> getSource() {
        return source;
    }

    /**
     * get the metadata map, if externally modified then the guarantees of this class are not enforced
     */
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
        return (Map<String, String>) metadata.get(IngestDocument.Metadata.DYNAMIC_TEMPLATES.getFieldName());
    }

    /**
     * Check that all metadata map contains only valid metadata and no extraneous keys and source map contains no metadata
     */
    protected void validateMetadata() {
        int numMetadata = 0;
        for (Map.Entry<String, BiConsumer<String, Object>> entry : validators.entrySet()) {
            String key = entry.getKey();
            if (metadata.containsKey(key)) {
                numMetadata++;
            }
            entry.getValue().accept(key, metadata.get(key));
            if (source.containsKey(key)) {
                throw new IllegalArgumentException("Unexpected metadata key [" + key + "] in source with value [" + source.get(key) + "]");
            }
        }
        if (numMetadata < metadata.size()) {
            Set<String> keys = new HashSet<>(metadata.keySet());
            keys.removeAll(validators.keySet());
            throw new IllegalArgumentException(
                "Unexpected metadata keys ["
                    + keys.stream().sorted().map(k -> k + ":" + metadata.get(k)).collect(Collectors.joining(", "))
                    + "]"
            );
        }
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
        BiConsumer<String, Object> validator = validators.get(key);
        if (validator != null) {
            validator.accept(key, value);
            return metadata.put(key, value);
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
            BiConsumer<String, Object> validator = validators.get(key);
            if (validator != null) {
                validator.accept(strKey, null);
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
        // AbstractMap uses entrySet().clear(), it should be quicker to run through the validators, then call the wrapped maps clear
        validators.forEach((k, v) -> {
            if (metadata.containsKey(k)) {
                v.accept(k, null);
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
            if (metadataSet.contains(o)) {
                if (o instanceof Map.Entry<?, ?> entry) {
                    if (entry.getKey()instanceof String key) {
                        BiConsumer<String, Object> validator = validators.get(key);
                        if (validator != null) {
                            validator.accept(key, null);
                            return metadataSet.remove(o);
                        }
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
        final Iterator<Map.Entry<String, Object>> sourceIter;
        final Iterator<Map.Entry<String, Object>> metadataIter;

        boolean sourceCur = true;
        Entry cur;

        EntrySetIterator(Iterator<Map.Entry<String, Object>> sourceIter, Iterator<Map.Entry<String, Object>> metadataIter) {
            this.sourceIter = sourceIter;
            this.metadataIter = metadataIter;
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
         * @throws IllegalStateException if remove is called before {@link #next()}
         */
        @Override
        public void remove() {
            if (cur == null) {
                throw new IllegalStateException();
            }
            if (sourceCur) {
                sourceIter.remove();
            } else {
                BiConsumer<String, Object> validator = validators.get(cur.getKey());
                if (validator != null) {
                    validator.accept(cur.getKey(), null);
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
                BiConsumer<String, Object> validator = validators.get(entry.getKey());
                if (validator != null) {
                    validator.accept(entry.getKey(), value);
                }
            }
            return entry.setValue(value);
        }
    }

    /**
     * Allow a String or null
     */
    protected static void stringValidator(String key, Object value) {
        if (value == null || value instanceof String) {
            return;
        }
        throw new IllegalArgumentException(
            key + " must be null or a String but was [" + value + "] with type [" + value.getClass().getName() + "]"
        );
    }

    /**
     * Allow Numbers that can be represented as longs without loss of precision
     */
    protected static void longValidator(String key, Object value) {
        if (value == null) {
            return; // Allow null version for now
        }
        if (value instanceof Number number) {
            long version = number.longValue();
            // did we round?
            if (number.doubleValue() == version) {
                return;
            }
        }
        throw new IllegalArgumentException(
            key + " may only be set to an int or a long but was [" + value + "] with type [" + value.getClass().getName() + "]"
        );
    }

    /**
     * Version must be non-null and representable as a long without loss of precision
     */
    protected static void versionValidator(String key, Object value) {
        if (value == null) {
            throw new IllegalArgumentException(key + " cannot be null");
        }
        longValidator(key, value);
    }

    /**
     * Allow lower case Strings that map to VersionType values, or null
     */
    protected static void versionTypeValidator(String key, Object value) {
        if (value == null) {
            return;
        }
        if (value instanceof String versionType) {
            try {
                VersionType.fromString(versionType);
                return;
            } catch (IllegalArgumentException ignored) {}
        }
        throw new IllegalArgumentException(
            key
                + " must be a null or one of ["
                + Arrays.stream(VersionType.values()).map(vt -> VersionType.toString(vt)).collect(Collectors.joining(", "))
                + "] but was ["
                + value
                + "] with type ["
                + value.getClass().getName()
                + "]"
        );
    }

    /**
     * Allow maps
     */
    protected static void mapValidator(String key, Object value) {
        if (value == null || value instanceof Map<?, ?>) {
            return;
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

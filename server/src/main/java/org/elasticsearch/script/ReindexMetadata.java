/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.script;

import org.elasticsearch.common.util.Maps;
import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.ingest.IngestDocument;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Metadata for the {@link ReindexScript} context.
 * _index, _id, _version, _routing, _slice are all read-write. _id, _version, _routing and _slice are also nullable.
 * _now is millis since epoch and read-only
 * op is read-write one of 'index', 'noop', 'delete'
 *
 * If _version is set to null in the ctx map, that is interpreted as using internal versioning.
 *
 * The {@link #setVersionToInternal(Metadata)} and {@link #isVersionInternal(Metadata)} augmentations
 * are provided for users of this class.  These augmentations allow the class to appear as {@link Metadata}
 * but handle the internal versioning scheme without scripts accessing the ctx map.
 */
public class ReindexMetadata extends Metadata {
    private static final String SLICE = SliceIndexing.PARAM_NAME;
    private static final Map<String, FieldProperty<?>> PROPERTIES_WITHOUT_SLICE = Map.of(
        INDEX,
        ObjectField.withWritable(),
        ID,
        ObjectField.withWritable().withNullable(),
        VERSION,
        LongField.withWritable().withNullable(),
        ROUTING,
        StringField.withWritable().withNullable(),
        OP,
        StringField.withWritable().withValidation(stringSetValidator(Set.of("noop", "index", "delete"))),
        NOW,
        LongField
    );
    private static final Map<String, FieldProperty<?>> PROPERTIES_WITH_SLICE = Map.of(
        INDEX,
        ObjectField.withWritable(),
        ID,
        ObjectField.withWritable().withNullable(),
        VERSION,
        LongField.withWritable().withNullable(),
        ROUTING,
        StringField.withWritable().withNullable(),
        SLICE,
        StringField.withWritable().withNullable(),
        OP,
        StringField.withWritable().withValidation(stringSetValidator(Set.of("noop", "index", "delete"))),
        NOW,
        LongField
    );

    static final Map<String, FieldProperty<?>> PROPERTIES = SliceIndexing.SLICE_FEATURE_FLAG.isEnabled()
        ? PROPERTIES_WITH_SLICE
        : PROPERTIES_WITHOUT_SLICE;

    protected final String index;
    protected final String id;
    protected final Long version;
    protected final String routing;
    private boolean routingFromSlice;

    public ReindexMetadata(String index, String id, Long version, String routing, String op, long timestamp) {
        super(metadataMap(index, id, version, routing, op, timestamp), PROPERTIES);
        this.index = index;
        this.id = id;
        this.version = version;
        this.routing = routing;
    }

    /**
     * Create the backing metadata map with the standard contents assuming default validators.
     */
    protected static Map<String, Object> metadataMap(String index, String id, Long version, String routing, String op, long timestamp) {
        Map<String, Object> metadata = Maps.newHashMapWithExpectedSize(IngestDocument.Metadata.values().length);
        metadata.put(INDEX, index);
        metadata.put(ID, id);
        metadata.put(VERSION, version);
        metadata.put(ROUTING, routing);
        if (SliceIndexing.SLICE_FEATURE_FLAG.isEnabled()) {
            metadata.put(SLICE, routing);
        }
        metadata.put(OP, op);
        metadata.put(NOW, timestamp);
        return metadata;
    }

    @Override
    public Object put(String key, Object value) {
        if (SliceIndexing.SLICE_FEATURE_FLAG.isEnabled() && (ROUTING.equals(key) || SLICE.equals(key))) {
            final Object previous = super.put(ROUTING, value);
            super.put(SLICE, value);
            routingFromSlice = SLICE.equals(key);
            return previous;
        }
        return super.put(key, value);
    }

    @Override
    public Object remove(String key) {
        if (SliceIndexing.SLICE_FEATURE_FLAG.isEnabled() && (ROUTING.equals(key) || SLICE.equals(key))) {
            final Object previous = super.remove(ROUTING);
            super.remove(SLICE);
            routingFromSlice = false;
            return previous;
        }
        return super.remove(key);
    }

    @Override
    public Object get(String key) {
        if (SliceIndexing.SLICE_FEATURE_FLAG.isEnabled() && SLICE.equals(key)) {
            return super.get(ROUTING);
        }
        return super.get(key);
    }

    /**
     * Get version, if it's null, return sentinel value {@link Long#MIN_VALUE}
     */
    @Override
    public long getVersion() {
        Number version = getNumber(VERSION);
        if (version == null) {
            return Long.MIN_VALUE;
        }
        return version.longValue();
    }

    public boolean isVersionInternal() {
        return get(VERSION) == null;
    }

    /**
     * Augmentation to allow {@link ReindexScript}s to check if the version is set to "internal"
     */
    public static boolean isVersionInternal(Metadata receiver) {
        return receiver.get(VERSION) == null;
    }

    /**
     * Augmentation to allow {@link ReindexScript}s to set the version to "internal".
     *
     * This is necessary because {@link #setVersion(long)} takes a primitive long.
     */
    public static void setVersionToInternal(Metadata receiver) {
        receiver.put(VERSION, null);
    }

    public boolean versionChanged() {
        Number updated = getNumber(VERSION);
        if (version == null || updated == null) {
            return version != updated;
        }
        return version != updated.longValue();
    }

    public boolean indexChanged() {
        return Objects.equals(index, getString(INDEX)) == false;
    }

    public boolean idChanged() {
        return Objects.equals(id, getString(ID)) == false;
    }

    public boolean routingChanged() {
        return Objects.equals(routing, getString(ROUTING)) == false;
    }

    public boolean routingChangedWithSlice(boolean previousRoutingFromSlice) {
        return Objects.equals(routing, getString(ROUTING)) == false || routingFromSlice != previousRoutingFromSlice;
    }

    public boolean isRoutingFromSlice() {
        return routingFromSlice;
    }
}

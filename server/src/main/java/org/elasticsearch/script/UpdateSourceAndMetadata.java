/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.elasticsearch.common.util.Maps;

import java.time.ZonedDateTime;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Source and metadata for update (as opposed to insert via upsert) in the Update context.
 */
public class UpdateSourceAndMetadata extends SourceAndMetadataMap {
    protected static final String OP = "op";
    protected static final String TIMESTAMP = "_now";
    protected static final String SOURCE = "_source";

    // AbstractAsyncBulkByScrollAction.OpType uses 'noop' rather than 'none', so unify on 'noop' but allow 'none' in
    // the ctx map
    protected static final String LEGACY_NOOP_STRING = "none";
    protected static final Set<String> VALID_UPDATE_OPS = Set.of("noop", "index", "delete", LEGACY_NOOP_STRING);

    public static Map<String, Validator> VALIDATORS = Map.of(
        INDEX,
        UpdateSourceAndMetadata::setOnceStringValidator,
        ID,
        UpdateSourceAndMetadata::setOnceStringValidator,
        VERSION,
        UpdateSourceAndMetadata::setOnceLongValidator,
        ROUTING,
        UpdateSourceAndMetadata::setOnceStringValidator,
        TYPE,
        UpdateSourceAndMetadata::setOnceStringValidator,
        OP,
        UpdateSourceAndMetadata::stringValidator,
        TIMESTAMP,
        UpdateSourceAndMetadata::setOnceLongValidator
    );

    protected final Set<String> validOps;

    public UpdateSourceAndMetadata(
        String index,
        String id,
        long version,
        String routing,
        String type,
        String op,
        long timestamp,
        Map<String, Object> source
    ) {
        super(wrapSource(source), metadataMap(index, id, version, routing, type, op, timestamp), VALIDATORS);
        validOps = VALID_UPDATE_OPS;
    }

    protected UpdateSourceAndMetadata(Map<String, Object> source, Map<String, Object> metadata, Map<String, Validator> validators, Set<String> validOps) {
        super(wrapSource(source), metadata, validators);
        this.validOps = validOps;
    }

    protected static Map<String, Object> wrapSource(Map<String, Object> source) {
        Map<String, Object> wrapper = Maps.newHashMapWithExpectedSize(1);
        wrapper.put(SOURCE, source);
        return wrapper;
    }

    protected static Map<String, Object> metadataMap(
        String index,
        String id,
        long version,
        String routing,
        String type,
        String op,
        long timestamp
    ) {
        Map<String, Object> metadata = Maps.newHashMapWithExpectedSize(VALIDATORS.size());
        metadata.put(INDEX, index);
        metadata.put(ID, id);
        metadata.put(VERSION, version);
        metadata.put(ROUTING, routing);
        metadata.put(TYPE, type);
        metadata.put(OP, op);
        metadata.put(TIMESTAMP, timestamp);
        return metadata;
    }

    @Override
    public boolean hasVersion() {
        return metadata.get(VERSION) != null;
    }

    @Override
    public long getVersion() {
        if (hasVersion() == false) {
            return Long.MIN_VALUE;
        }
        return super.getVersion();
    }

    @Override
    public String getOp() {
        String op = getString(OP);
        if (LEGACY_NOOP_STRING.equals(op) || validOps.contains(op) == false) {
            // UpdateHelper.UpdateOpType.lenientFromString allows anything into the map
            return "noop";
        }
        return op;
    }

    @Override
    public void setOp(String op) {
        if (LEGACY_NOOP_STRING.equals(op)) {
            throw new IllegalArgumentException(LEGACY_NOOP_STRING + " is deprecated, use 'noop' instead");
        }
        if (validOps.contains(op) == false) {
            throw new IllegalArgumentException(
                "[" + op + "] must be one of " + VALID_UPDATE_OPS.stream().sorted().collect(Collectors.joining(", ")) + ", not [" + op + "]"
            );
        }
        put(OP, op);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, Object> getSource() {
        Map<String, Object> wrapped = super.getSource();
        Object rawSource = wrapped.get(SOURCE);
        if (rawSource instanceof Map<?, ?> map) {
            return (Map<String, Object>) map;
        }
        throw new IllegalArgumentException(
            "Expected source to be a map, instead was [" + rawSource + "] with type [" + rawSource.getClass().getCanonicalName() + "]"
        );
    }

    @Override
    public ZonedDateTime getTimestamp() {
        return getZonedDateTime(TIMESTAMP);
    }

    /**
     * A validator that allows string values to be set once but neither updated nor removed.
     */
    public static void setOnceStringValidator(MapOperation op, String key, Object value) {
        if (op != MapOperation.INIT) {
            throw new IllegalArgumentException("Cannot " + op.name().toLowerCase(Locale.ROOT) + " key [" + key + "]");
        }
        stringValidator(op, key, value);
    }

    /**
     * Same as {@link #longValidator(MapOperation, String, Object)} but allows longs to be set once and neither updated nor removed.
     */
    public static void setOnceLongValidator(MapOperation op, String key, Object value) {
        if (op != MapOperation.INIT) {
            throw new IllegalArgumentException("Cannot " + op.name().toLowerCase(Locale.ROOT) + " key [" + key + "]");
        }
        longValidator(op, key, value);
    }

    /**
     * A {@link #stringValidator(MapOperation, String, Object)} that validates against a set of valid strings and forbids removal.
     */
    public static Validator opValidatorFromValidOps(Set<String> validOps) {
        return (op, key, value) -> {
            if (op == MapOperation.REMOVE) {
                throw new IllegalArgumentException("Cannot remove [" + key + "]");
            }
            if (value instanceof String opStr) {
                if (validOps.contains(opStr)) {
                    return;
                }
                throw new IllegalArgumentException(
                    key + " must be one of " + validOps.stream().sorted().collect(Collectors.joining(", ")) + ", not [" + opStr + "]"
                );
            }
            throw new IllegalArgumentException(
                key
                    + " must be String and one of "
                    + validOps.stream().sorted().collect(Collectors.joining(","))
                    + " but was ["
                    + value
                    + "] with type ["
                    + value.getClass().getName()
                    + "]"
            );
        };
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.dataset;

import org.elasticsearch.cluster.metadata.DatasetMapping;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A dataset definition supplied inline by a {@code DATASET(<data source>, {...})} table function, read out of the
 * payload the parser already folded to plain values.
 * <p>
 * The payload is the same JSON a {@code PUT _query/dataset/<name>} body carries, minus the two things the call site
 * supplies instead: the name, which an inline definition never has, and {@code data_source}, which is the call's
 * positional argument. Supplying either in the payload is refused rather than ignored — a definition naming one data
 * source positionally and another in its body has no defensible reading, and silently preferring one would decide
 * which credentials the query runs under.
 * <p>
 * Key names come from {@link PutDatasetAction.Request}'s own {@link ParseField}s and {@code mappings} is parsed by the
 * same {@link DatasetMapping#parseMappings} the registration API uses, so neither the vocabulary nor the mapping
 * grammar can drift. Not shared is the walk over the top-level keys — PUT reads a byte stream, this reads a
 * {@code Map} — so each route has its own unknown-key refusal, kept in step by {@link #KNOWN_KEYS}.
 */
public final class InlineDatasetDefinition {

    /** Every key an inline definition may carry, in the order a diagnostic should list them. */
    public static final Set<String> KNOWN_KEYS = new LinkedHashSet<>(
        List.of(
            PutDatasetAction.Request.RESOURCE.getPreferredName(),
            PutDatasetAction.Request.DESCRIPTION.getPreferredName(),
            PutDatasetAction.Request.SETTINGS.getPreferredName(),
            PutDatasetAction.Request.MAPPINGS.getPreferredName()
        )
    );

    private final String resource;
    @Nullable
    private final String description;
    @Nullable
    private final Map<String, Object> settings;
    @Nullable
    private final DatasetMapping mapping;

    private InlineDatasetDefinition(
        String resource,
        @Nullable String description,
        @Nullable Map<String, Object> settings,
        @Nullable DatasetMapping mapping
    ) {
        this.resource = resource;
        this.description = description;
        this.settings = settings;
        this.mapping = mapping;
    }

    public String resource() {
        return resource;
    }

    @Nullable
    public String description() {
        return description;
    }

    /** The raw settings as typed, for {@code DataSourceValidator#validateDataset}; {@code null} when none were given. */
    @Nullable
    public Map<String, Object> settings() {
        return settings;
    }

    @Nullable
    public DatasetMapping mapping() {
        return mapping;
    }

    /**
     * Reads {@code payload} into a definition, or throws {@link IllegalArgumentException} with a message naming the
     * offending key. Only the shape is settled here: whether the settings are meaningful for the data source's type
     * is the validator's decision, and it runs later with the parent data source in hand.
     */
    public static InlineDatasetDefinition fromPayload(Map<String, Object> payload) {
        String dataSourceKey = PutDatasetAction.Request.DATA_SOURCE.getPreferredName();
        if (payload.containsKey(dataSourceKey)) {
            throw new IllegalArgumentException(
                "[" + dataSourceKey + "] cannot be set in a dataset definition here; the data source is the first argument of the call"
            );
        }
        List<String> unknown = new ArrayList<>();
        for (String key : payload.keySet()) {
            if (KNOWN_KEYS.contains(key) == false) {
                unknown.add(key);
            }
        }
        if (unknown.isEmpty() == false) {
            throw new IllegalArgumentException(
                "unknown dataset definition " + (unknown.size() == 1 ? "key " : "keys ") + unknown + ", expected one of " + KNOWN_KEYS
            );
        }
        String resource = stringValue(payload, PutDatasetAction.Request.RESOURCE.getPreferredName());
        if (resource == null) {
            throw new IllegalArgumentException(
                "dataset definition requires [" + PutDatasetAction.Request.RESOURCE.getPreferredName() + "]"
            );
        }
        return new InlineDatasetDefinition(
            resource,
            stringValue(payload, PutDatasetAction.Request.DESCRIPTION.getPreferredName()),
            mapValue(payload, PutDatasetAction.Request.SETTINGS.getPreferredName()),
            parseMapping(mapValue(payload, PutDatasetAction.Request.MAPPINGS.getPreferredName()))
        );
    }

    @Nullable
    private static String stringValue(Map<String, Object> payload, String key) {
        Object value = payload.get(key);
        if (value == null) {
            return null;
        }
        if (value instanceof String string) {
            return string;
        }
        throw new IllegalArgumentException("dataset definition [" + key + "] must be a string, found [" + value + "]");
    }

    @Nullable
    @SuppressWarnings("unchecked")
    private static Map<String, Object> mapValue(Map<String, Object> payload, String key) {
        Object value = payload.get(key);
        if (value == null) {
            return null;
        }
        if (value instanceof Map<?, ?> map) {
            // The parser folds every payload key to a string, so this cast cannot see a non-string key.
            return (Map<String, Object>) map;
        }
        throw new IllegalArgumentException("dataset definition [" + key + "] must be an object");
    }

    /**
     * Runs the registration API's own mapping parser over the folded map by rendering it back to JSON. The round-trip
     * buys one mapping grammar rather than two: a new mapping field needs no second implementation to reach here.
     */
    @Nullable
    private static DatasetMapping parseMapping(@Nullable Map<String, Object> mappings) {
        if (mappings == null) {
            return null;
        }
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.map(mappings);
            try (
                XContentParser parser = XContentType.JSON.xContent()
                    .createParser(XContentParserConfiguration.EMPTY, BytesReference.bytes(builder).streamInput())
            ) {
                // parseMappings asserts it is already ON the object's START_OBJECT, as ObjectParser would leave it.
                parser.nextToken();
                return DatasetMapping.assemble(DatasetMapping.parseMappings(parser));
            }
        } catch (IOException e) {
            // Rendering a map that is already in memory and reading it straight back cannot fail on I/O; a parse
            // refusal arrives as IllegalArgumentException and is left to propagate.
            throw new IllegalStateException("could not read inline dataset mappings", e);
        }
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.mount;

import org.apache.lucene.index.IndexWriter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.parquet.docvalues.Conversion;
import org.elasticsearch.xpack.parquet.docvalues.RepositoryLocation;
import org.elasticsearch.xpack.parquet.mount.ParquetMappingDeriver.ColumnOverride;
import org.elasticsearch.xpack.parquet.mount.ParquetMappingDeriver.Overrides;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * What a request to preview or to mount Parquet files says. The body is parsed here and nowhere else, so that the request itself is a
 * plain map that crosses the wire as it is.
 *
 * <pre>
 * {
 *   "repository": "lake",
 *   "paths": ["events/part-0001.parquet", "events/part-0002.parquet"],
 *   "data_stream": "events",   // or "index": "events" for one plain index, which needs no timestamp field
 *   "timestamp_field": "event_time",
 *   "fields": { "event_time": { "type": "date", "conversion": "seconds_to_millis" }, "host": { "name": "hostname" } },
 *   "exclude": ["params"],
 *   "max_rows_per_index": 1000000000,
 *   "index_settings": { "number_of_replicas": 1 }
 * }
 * </pre>
 */
public record ParquetMountSpec(
    String repository,
    List<String> paths,
    @Nullable String dataStream,
    @Nullable String index,
    Overrides overrides,
    long maxRowsPerIndex,
    Settings indexSettings
) {

    /** The default of {@code max_rows_per_index}: well below the limit of documents in one index. */
    public static final long DEFAULT_MAX_ROWS_PER_INDEX = 1_000_000_000L;

    private static final Set<String> KEYS = Set.of(
        "repository",
        "paths",
        "data_stream",
        "index",
        "timestamp_field",
        "fields",
        "exclude",
        "max_rows_per_index",
        "index_settings"
    );

    /** The settings that the mount decides, which a request cannot change. */
    private static final List<String> CONTROLLED_SETTING_PREFIXES = List.of(
        "index.parquet.",
        "index.mode",
        "index.mapping.id_field.",
        "index.number_of_shards",
        "index.number_of_routing_shards",
        "index.hidden"
    );

    /**
     * @param requireDataStream whether the request must name a data stream or an index, which a mount needs and a preview does not
     * @throws IllegalArgumentException for a request that is not valid
     */
    @SuppressWarnings("unchecked")
    public static ParquetMountSpec parse(Map<String, Object> body, boolean requireDataStream) {
        for (String key : body.keySet()) {
            if (KEYS.contains(key) == false) {
                throw new IllegalArgumentException("unknown field [" + key + "], the fields are " + KEYS.stream().sorted().toList());
            }
        }
        String repository = string(body, "repository", true);
        List<String> paths = new ArrayList<>();
        Object rawPaths = body.get("paths");
        if (rawPaths instanceof List<?> pathList && pathList.isEmpty() == false && pathList.stream().allMatch(p -> p instanceof String)) {
            pathList.forEach(p -> paths.add((String) p));
        } else {
            throw new IllegalArgumentException("[paths] must be a list of at least one path");
        }
        // a path or a repository that is not valid is rejected here, before any request reaches the repository
        paths.forEach(path -> new RepositoryLocation(repository, path));
        if (new HashSet<>(paths).size() != paths.size()) {
            throw new IllegalArgumentException("[paths] has a path more than once");
        }
        String dataStream = string(body, "data_stream", false);
        String index = string(body, "index", false);
        if (requireDataStream && (dataStream == null) == (index == null)) {
            throw new IllegalArgumentException(
                dataStream == null
                    ? "[data_stream] or [index] is required: a data stream takes any number of files, an index takes the files that fit one"
                    : "give [data_stream] or [index], not both"
            );
        }
        for (String name : new String[] { dataStream, index }) {
            if (name != null && (name.startsWith(".") || name.startsWith("_") || name.contains("*") || name.contains(","))) {
                throw new IllegalArgumentException("invalid name [" + name + "]");
            }
        }

        Map<String, ColumnOverride> columns = new HashMap<>();
        Object rawFields = body.get("fields");
        if (rawFields != null) {
            if (rawFields instanceof Map<?, ?> == false) {
                throw new IllegalArgumentException("[fields] must be an object with a column for each key");
            }
            for (Map.Entry<String, Object> entry : ((Map<String, Object>) rawFields).entrySet()) {
                if (entry.getValue() instanceof Map<?, ?> == false) {
                    throw new IllegalArgumentException("[fields." + entry.getKey() + "] must be an object");
                }
                Map<String, Object> override = (Map<String, Object>) entry.getValue();
                for (String key : override.keySet()) {
                    if (Set.of("name", "type", "conversion").contains(key) == false) {
                        throw new IllegalArgumentException("unknown field [fields." + entry.getKey() + "." + key + "]");
                    }
                }
                String conversion = string(override, "conversion", false);
                columns.put(
                    entry.getKey(),
                    new ColumnOverride(
                        string(override, "name", false),
                        string(override, "type", false),
                        conversion == null ? null : Conversion.fromExternalName(conversion)
                    )
                );
            }
        }
        Set<String> exclude = new HashSet<>();
        Object rawExclude = body.get("exclude");
        if (rawExclude != null) {
            if (rawExclude instanceof List<?> list && list.stream().allMatch(c -> c instanceof String)) {
                list.forEach(c -> exclude.add((String) c));
            } else {
                throw new IllegalArgumentException("[exclude] must be a list of column names");
            }
        }
        long maxRows = DEFAULT_MAX_ROWS_PER_INDEX;
        Object rawMax = body.get("max_rows_per_index");
        if (rawMax != null) {
            if (rawMax instanceof Number number && number.longValue() > 0) {
                maxRows = number.longValue();
            } else {
                throw new IllegalArgumentException("[max_rows_per_index] must be a positive number");
            }
        }
        // an index holds at most IndexWriter.MAX_DOCS documents
        maxRows = Math.min(maxRows, IndexWriter.MAX_DOCS);

        Settings.Builder indexSettings = Settings.builder();
        Object rawSettings = body.get("index_settings");
        if (rawSettings != null) {
            if (rawSettings instanceof Map<?, ?> == false) {
                throw new IllegalArgumentException("[index_settings] must be an object");
            }
            indexSettings.loadFromMap((Map<String, Object>) rawSettings);
            Settings normalized = indexSettings.normalizePrefix("index.").build();
            for (String key : normalized.keySet()) {
                for (String prefix : CONTROLLED_SETTING_PREFIXES) {
                    if (key.startsWith(prefix)) {
                        throw new IllegalArgumentException(
                            "the setting [" + key + "] is decided by the mount and cannot be set in [index_settings]"
                        );
                    }
                }
            }
            indexSettings = Settings.builder().put(normalized);
        }
        return new ParquetMountSpec(
            repository,
            List.copyOf(paths),
            dataStream,
            index,
            new Overrides(Map.copyOf(columns), Set.copyOf(exclude), string(body, "timestamp_field", false)),
            maxRows,
            indexSettings.build()
        );
    }

    @Nullable
    private static String string(Map<String, Object> map, String key, boolean required) {
        Object value = map.get(key);
        if (value == null) {
            if (required) {
                throw new IllegalArgumentException("[" + key + "] is required");
            }
            return null;
        }
        if (value instanceof String string && string.isEmpty() == false) {
            return string;
        }
        throw new IllegalArgumentException("[" + key + "] must be a non-empty string");
    }
}

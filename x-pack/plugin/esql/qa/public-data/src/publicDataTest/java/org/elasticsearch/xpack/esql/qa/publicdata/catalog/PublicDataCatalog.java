/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.qa.publicdata.catalog;

import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.yaml.YamlXContent;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

/**
 * The parsed, typed form of {@code public-data-catalog.yml}: every {@link PublicDataSource} the suite
 * knows about, and every already-public {@link SourceVariant} of it (plan section 3). Parsing is strict:
 * an unknown key, a missing required field, or an enum value outside {@link PublicDataFormat}/
 * {@link PublicDataCodec}/{@link PublicDataProvider}/{@link PartitionLayout}/{@link DataScale} throws
 * immediately, since a silently-ignored typo here would otherwise surface much later as a confusing
 * "dataset not found" failure deep in a remote suite run.
 */
public final class PublicDataCatalog {

    public static final String CLASSPATH_RESOURCE = "/public-data-catalog.yml";

    private final List<PublicDataSource> sources;
    private final Map<String, PublicDataSource> byId;

    private PublicDataCatalog(List<PublicDataSource> sources) {
        this.sources = List.copyOf(sources);
        Map<String, PublicDataSource> index = new LinkedHashMap<>();
        for (PublicDataSource source : sources) {
            if (index.put(source.id(), source) != null) {
                throw new IllegalArgumentException("Duplicate public-data-catalog.yml source id [" + source.id() + "]");
            }
        }
        this.byId = Map.copyOf(index);
    }

    /** Loads and parses {@value #CLASSPATH_RESOURCE} from the classpath. */
    public static PublicDataCatalog loadFromClasspath() {
        try (InputStream in = PublicDataCatalog.class.getResourceAsStream(CLASSPATH_RESOURCE)) {
            if (in == null) {
                throw new IllegalStateException("Classpath resource [" + CLASSPATH_RESOURCE + "] not found");
            }
            return parse(in);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to load " + CLASSPATH_RESOURCE, e);
        }
    }

    public List<PublicDataSource> sources() {
        return sources;
    }

    public Optional<PublicDataSource> bySourceId(String id) {
        return Optional.ofNullable(byId.get(id));
    }

    public PublicDataSource requireSourceId(String id) {
        return bySourceId(id).orElseThrow(() -> new IllegalArgumentException("Unknown public-data-catalog.yml source id [" + id + "]"));
    }

    @SuppressWarnings("unchecked")
    public static PublicDataCatalog parse(InputStream in) throws IOException {
        try (XContentParser parser = YamlXContent.yamlXContent.createParser(XContentParserConfiguration.EMPTY, in)) {
            Map<String, Object> root = parser.map();
            Object rawSources = requireField(root, "sources", "<root>");
            if ((rawSources instanceof List<?>) == false) {
                throw new IllegalArgumentException("[sources] must be a list");
            }
            List<PublicDataSource> sources = new ArrayList<>();
            for (Object rawSource : (List<?>) rawSources) {
                sources.add(parseSource((Map<String, Object>) rawSource));
            }
            return new PublicDataCatalog(sources);
        }
    }

    @SuppressWarnings("unchecked")
    private static PublicDataSource parseSource(Map<String, Object> raw) throws IOException {
        String id = requireString(raw, "id", "<source>");
        String displayName = requireString(raw, "display_name", id);
        String homepage = requireString(raw, "homepage", id);
        String license = requireString(raw, "license", id);
        String queryProvenance = requireString(raw, "query_provenance", id);
        Object rawVariants = requireField(raw, "variants", id);
        if ((rawVariants instanceof List<?>) == false) {
            throw new IllegalArgumentException("[" + id + "].variants must be a list");
        }
        List<SourceVariant> variants = new ArrayList<>();
        for (Object rawVariant : (List<?>) rawVariants) {
            variants.add(parseVariant(id, (Map<String, Object>) rawVariant));
        }
        if (variants.isEmpty()) {
            throw new IllegalArgumentException("[" + id + "] declares no variants");
        }
        return new PublicDataSource(id, displayName, homepage, license, queryProvenance, variants);
    }

    @SuppressWarnings("unchecked")
    private static SourceVariant parseVariant(String sourceId, Map<String, Object> raw) throws IOException {
        String context = sourceId + "." + raw.getOrDefault("id", "<variant>");
        String id = requireString(raw, "id", context);
        String specResource = requireString(raw, "spec", context);
        PublicDataFormat format = PublicDataFormat.parse(requireString(raw, "format", context));
        PublicDataCodec codec = PublicDataCodec.parse(requireString(raw, "codec", context));
        PublicDataProvider provider = PublicDataProvider.parse(requireString(raw, "provider", context));
        String region = optionalString(raw, "region");
        String resource = requireString(raw, "resource", context);
        String pinCheckUri = optionalString(raw, "pin_check_uri");
        if (pinCheckUri == null) {
            pinCheckUri = resource;
        }
        String settingsJson = toJsonOrNull(raw.get("settings"));
        PartitionLayout partitionLayout = PartitionLayout.parse(requireString(raw, "partition_layout", context));
        DataScale scale = DataScale.parse(requireString(raw, "scale", context));
        Object rawPin = requireField(raw, "pin", context);
        PinInfo pin = parsePin(context, (Map<String, Object>) rawPin);
        boolean crossValidated = Boolean.TRUE.equals(raw.get("cross_validated"));
        String notes = requireString(raw, "notes", context);
        return new SourceVariant(
            id,
            specResource,
            format,
            codec,
            provider,
            region,
            resource,
            pinCheckUri,
            settingsJson,
            partitionLayout,
            scale,
            pin,
            crossValidated,
            notes
        );
    }

    private static PinInfo parsePin(String context, Map<String, Object> raw) {
        String etag = requireString(raw, "etag", context + ".pin");
        long sizeBytes = ((Number) requireField(raw, "size_bytes", context + ".pin")).longValue();
        String capturedAt = requireString(raw, "captured_at", context + ".pin");
        Object rawCount = raw.get("object_count");
        Integer objectCount = rawCount == null ? null : ((Number) rawCount).intValue();
        Object rawStrategy = raw.get("strategy");
        PinStrategy strategy = rawStrategy == null ? PinStrategy.ETAG : PinStrategy.parse(rawStrategy.toString());
        String contentSignature = optionalString(raw, "content_signature");
        if (strategy == PinStrategy.CONTENT_SIGNATURE && (contentSignature == null || contentSignature.isBlank())) {
            throw new IllegalArgumentException("[" + context + ".pin] strategy CONTENT_SIGNATURE requires a non-blank content_signature");
        }
        return new PinInfo(etag, sizeBytes, capturedAt, objectCount, strategy, contentSignature);
    }

    private static String toJsonOrNull(Object settings) throws IOException {
        if (settings == null) {
            return null;
        }
        if ((settings instanceof Map<?, ?>) == false) {
            throw new IllegalArgumentException("[settings] must be a mapping, got [" + settings + "]");
        }
        // YamlXContent#map() always produces String-keyed mappings for a YAML mapping node, but the raw
        // return type is Map<?, ?>; re-key explicitly instead of casting so no unchecked cast is needed.
        Map<?, ?> rawMap = (Map<?, ?>) settings;
        Map<String, Object> typedMap = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : rawMap.entrySet()) {
            typedMap.put(String.valueOf(entry.getKey()), entry.getValue());
        }
        return Strings.toString(jsonBuilder().map(typedMap));
    }

    private static Object requireField(Map<String, Object> raw, String key, String context) {
        Object value = raw.get(key);
        if (value == null) {
            throw new IllegalArgumentException("[" + context + "] is missing required field [" + key + "]");
        }
        return value;
    }

    private static String requireString(Map<String, Object> raw, String key, String context) {
        Object value = requireField(raw, key, context);
        if ((value instanceof String) == false || ((String) value).isBlank()) {
            throw new IllegalArgumentException("[" + context + "]." + key + " must be a non-blank string, got [" + value + "]");
        }
        return (String) value;
    }

    private static String optionalString(Map<String, Object> raw, String key) {
        Object value = raw.get(key);
        return value == null ? null : value.toString();
    }
}

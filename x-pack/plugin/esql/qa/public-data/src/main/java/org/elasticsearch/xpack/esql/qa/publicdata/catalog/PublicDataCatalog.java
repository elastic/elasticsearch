/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.publicdata.catalog;

import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.yaml.YamlXContent;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The whole public-data catalog, loaded from {@code public-data-catalog.yml}. The catalog is the
 * single machine-readable description of what the suite covers: corpora, variants, pins, declared
 * gaps. Everything else — parameter enumeration, pin verification, coverage reporting, validation —
 * derives from it, which is what makes "add a corpus = 2 files, no Java" hold.
 *
 * @param version schema version of the catalog file
 * @param corpora all corpora, active and backup alike
 * @param gaps    declared coverage gaps (see {@link GapSpec})
 */
public record PublicDataCatalog(int version, List<CorpusSpec> corpora, List<GapSpec> gaps) {

    /** The classpath resource the shipped catalog lives at. */
    public static final String CATALOG_RESOURCE = "/public-data-catalog.yml";

    /** Loads and parses a catalog from a classpath resource. Fails loudly on any malformation. */
    public static PublicDataCatalog loadFromClasspath(String resource) {
        try (InputStream in = PublicDataCatalog.class.getResourceAsStream(resource)) {
            if (in == null) {
                throw new IllegalArgumentException("Catalog resource [" + resource + "] not found on the classpath");
            }
            try (XContentParser parser = YamlXContent.yamlXContent.createParser(XContentParserConfiguration.EMPTY, in)) {
                // mapOrdered, not map: dataset_mappings properties are order-preserving by
                // contract (declared schemas may bind positionally), and a HashMap here would
                // silently scramble them before the dataset PUT
                return fromMap(parser.mapOrdered());
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read catalog resource [" + resource + "]", e);
        }
    }

    public CorpusSpec corpus(String id) {
        return corpora.stream()
            .filter(c -> c.id().equals(id))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Unknown corpus [" + id + "]"));
    }

    /** The variants backing a given workload csv-spec file name (across all corpora; 1:1 in practice). */
    public List<VariantSpec> variantsForWorkload(String specFileName) {
        return corpora.stream().filter(c -> specFileName.equals(c.workload())).flatMap(c -> c.variants().stream()).toList();
    }

    @SuppressWarnings("unchecked")
    static PublicDataCatalog fromMap(Map<String, Object> map) {
        int version = requireInt(map, "version", "catalog");
        List<CorpusSpec> corpora = new ArrayList<>();
        for (Object corpusEntry : requireList(map, "corpora", "catalog")) {
            corpora.add(corpusFromMap((Map<String, Object>) corpusEntry));
        }
        List<GapSpec> gaps = new ArrayList<>();
        for (Object gapEntry : listOrEmpty(map, "gaps")) {
            Map<String, Object> gap = (Map<String, Object>) gapEntry;
            gaps.add(new GapSpec(requireString(gap, "id", "gap"), requireString(gap, "reason", "gap"), stringList(gap, "cells")));
        }
        return new PublicDataCatalog(version, List.copyOf(corpora), List.copyOf(gaps));
    }

    @SuppressWarnings("unchecked")
    private static CorpusSpec corpusFromMap(Map<String, Object> map) {
        String id = requireString(map, "id", "corpus");
        String context = "corpus [" + id + "]";
        CorpusSpec.Kind kind = CorpusSpec.Kind.fromId(requireString(map, "kind", context));
        List<VariantSpec> variants = new ArrayList<>();
        for (Object variantEntry : requireList(map, "variants", context)) {
            variants.add(variantFromMap(id, (Map<String, Object>) variantEntry));
        }
        return new CorpusSpec(
            id,
            requireString(map, "title", context),
            requireString(map, "registry_url", context),
            requireString(map, "license", context),
            requireString(map, "description", context),
            kind,
            Scale.fromId(requireString(map, "scale", context)),
            DataQuality.fromId(requireString(map, "quality", context)),
            optionalString(map, "workload"),
            map.containsKey("assertion_mode")
                ? CorpusSpec.AssertionMode.fromId(requireString(map, "assertion_mode", context))
                : CorpusSpec.AssertionMode.EXACT,
            List.copyOf(variants)
        );
    }

    @SuppressWarnings("unchecked")
    private static VariantSpec variantFromMap(String corpusId, Map<String, Object> map) {
        String context = "variant of corpus [" + corpusId + "]";
        PinSpec pin = null;
        if (map.get("pin") instanceof Map<?, ?> pinMap) {
            pin = pinFromMap((Map<String, Object>) pinMap, context);
        }
        FailureSpec expectFailure = null;
        if (map.get("expect_failure") instanceof Map<?, ?> failureMap) {
            Map<String, Object> failure = (Map<String, Object>) failureMap;
            expectFailure = new FailureSpec(
                requireString(failure, "status", context + " expect_failure"),
                requireString(failure, "message_regex", context + " expect_failure"),
                requireString(failure, "reason", context + " expect_failure")
            );
        }
        return new VariantSpec(
            corpusId,
            Provider.fromId(requireString(map, "provider", context)),
            Format.fromId(requireString(map, "format", context)),
            Codec.fromId(requireString(map, "codec", context)),
            Layout.fromId(requireString(map, "layout", context)),
            map.containsKey("partitioning") ? requireString(map, "partitioning", context) : "none",
            optionalString(map, "region"),
            requireString(map, "resource", context),
            orderedStringMap(map, "sub_resources"),
            mapOrEmpty(map, "data_source_settings"),
            mapOrEmpty(map, "dataset_settings"),
            mapOrEmpty(map, "dataset_mappings"),
            pin,
            Set.copyOf(stringList(map, "tags")),
            Set.copyOf(stringList(map, "query_subset")),
            optionalString(map, "notes"),
            expectFailure,
            optionalString(map, "case"),
            optionalString(map, "disabled")
        );
    }

    @SuppressWarnings("unchecked")
    private static PinSpec pinFromMap(Map<String, Object> map, String context) {
        List<PinSpec.PinnedObject> samples = new ArrayList<>();
        for (Object sampleEntry : listOrEmpty(map, "samples")) {
            Map<String, Object> sample = (Map<String, Object>) sampleEntry;
            samples.add(
                new PinSpec.PinnedObject(
                    requireString(sample, "key", context + " pin sample"),
                    optionalString(sample, "etag"),
                    requireLong(sample, "size", context + " pin sample")
                )
            );
        }
        return new PinSpec(
            requireString(map, "method", context + " pin"),
            Instant.parse(requireString(map, "verified_at", context + " pin")),
            requireLong(map, "object_count", context + " pin"),
            requireLong(map, "total_bytes", context + " pin"),
            List.copyOf(samples),
            map.get("volatile") instanceof Boolean isVolatile && isVolatile,
            map.get("size_tolerance_pct") instanceof Number tolerance ? tolerance.intValue() : PinSpec.DEFAULT_SIZE_TOLERANCE_PERCENT
        );
    }

    private static String requireString(Map<String, Object> map, String key, String context) {
        Object value = map.get(key);
        if (value instanceof String s && s.isEmpty() == false) {
            return s;
        }
        throw new IllegalArgumentException("Missing or non-string [" + key + "] in " + context);
    }

    private static String optionalString(Map<String, Object> map, String key) {
        Object value = map.get(key);
        return value == null ? null : value.toString();
    }

    private static int requireInt(Map<String, Object> map, String key, String context) {
        if (map.get(key) instanceof Number n) {
            return n.intValue();
        }
        throw new IllegalArgumentException("Missing or non-numeric [" + key + "] in " + context);
    }

    private static long requireLong(Map<String, Object> map, String key, String context) {
        if (map.get(key) instanceof Number n) {
            return n.longValue();
        }
        throw new IllegalArgumentException("Missing or non-numeric [" + key + "] in " + context);
    }

    private static List<Object> requireList(Map<String, Object> map, String key, String context) {
        if (map.get(key) instanceof List<?> list) {
            @SuppressWarnings("unchecked")
            List<Object> cast = (List<Object>) list;
            return cast;
        }
        throw new IllegalArgumentException("Missing or non-list [" + key + "] in " + context);
    }

    private static List<Object> listOrEmpty(Map<String, Object> map, String key) {
        if (map.get(key) instanceof List<?> list) {
            @SuppressWarnings("unchecked")
            List<Object> cast = (List<Object>) list;
            return cast;
        }
        return List.of();
    }

    private static List<String> stringList(Map<String, Object> map, String key) {
        List<String> result = new ArrayList<>();
        for (Object value : listOrEmpty(map, key)) {
            result.add(value.toString());
        }
        // duplicates are rejected loudly where the result feeds Set.copyOf (tags, query_subset)
        return List.copyOf(result);
    }

    /**
     * An insertion-ordered string map. Order is load-bearing for {@code sub_resources}: the named
     * fragments are expected to line up with the comma-separated locations of the variant's
     * {@code resource}, and a reviewer diffing the two must see them in the same order.
     */
    private static Map<String, String> orderedStringMap(Map<String, Object> map, String key) {
        if (map.get(key) instanceof Map<?, ?> value) {
            Map<String, String> result = new LinkedHashMap<>();
            value.forEach((k, v) -> result.put(k.toString(), v.toString()));
            return Collections.unmodifiableMap(result);
        }
        return Map.of();
    }

    private static Map<String, Object> mapOrEmpty(Map<String, Object> map, String key) {
        if (map.get(key) instanceof Map<?, ?> value) {
            Map<String, Object> result = new LinkedHashMap<>();
            value.forEach((k, v) -> result.put(k.toString(), v));
            return Map.copyOf(result);
        }
        return Map.of();
    }
}

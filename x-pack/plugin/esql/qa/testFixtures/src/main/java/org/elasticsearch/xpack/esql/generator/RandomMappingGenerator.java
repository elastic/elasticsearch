/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator;

import org.elasticsearch.common.network.NetworkAddress;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomDoubleBetween;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomInstantBetween;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.elasticsearch.test.ESTestCase.randomIp;
import static org.elasticsearch.test.ESTestCase.randomLongBetween;

/**
 * Generates random Elasticsearch index mappings covering ALL field data types, ALL their
 * settings, and ALL valid values for those settings. Field names are drawn from a shared
 * pool so that names overlap across indices, exercising ESQL's union-type handling,
 * cross-index field resolution, type conflicts, and varied field settings.
 */
public final class RandomMappingGenerator {

    private RandomMappingGenerator() {}

    static final String[] FIELD_NAME_POOL = {
        "status",
        "name",
        "value",
        "count",
        "message",
        "host",
        "level",
        "code",
        "duration",
        "size",
        "path",
        "address",
        "active",
        "ver",
        "score",
        "rate",
        "tag",
        "category",
        "priority",
        "label",
        "result",
        "amount",
        "distance",
        "weight",
        "height",
        "price",
        "title",
        "region",
        "color",
        "mode" };

    /**
     * Every Elasticsearch leaf field type, weighted by typical usage.
     * Common types appear multiple times to increase their selection probability.
     */
    static final String[] WEIGHTED_TYPES = {
        // Common: keyword family
        "keyword",
        "keyword",
        "keyword",
        "constant_keyword",
        "wildcard",
        // Common: text family
        "text",
        "text",
        "text",
        "match_only_text",
        // Common: boolean
        "boolean",
        "boolean",
        // Common: numeric
        "integer",
        "integer",
        "integer",
        "long",
        "long",
        "long",
        "double",
        "double",
        "float",
        "float",
        "short",
        "byte",
        "half_float",
        "scaled_float",
        "unsigned_long",
        // Common: date family
        "date",
        "date",
        "date_nanos",
        // Structured
        "ip",
        "version",
        "binary",
        // Range types
        "integer_range",
        "long_range",
        "float_range",
        "double_range",
        "date_range",
        "ip_range",
        // Object/flattened
        "flattened",
        // Spatial
        "geo_point",
        "geo_shape",
        "point",
        "shape",
        // Text search
        "token_count",
        "completion",
        "search_as_you_type",
        // Ranking
        "rank_feature",
        "rank_features",
        // Aggregate/metric
        "aggregate_metric_double",
        "histogram",
        // Vector
        "dense_vector" };

    static final String[] BUILTIN_ANALYZERS = { "standard", "simple", "whitespace", "stop", "keyword" };

    static final String[] DATE_FORMATS = {
        "strict_date_optional_time||epoch_millis",
        "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis",
        "yyyy-MM-dd",
        "epoch_millis",
        "epoch_second",
        "basic_date",
        "date_optional_time" };

    public record FieldDef(String name, String esType, Map<String, Object> settings, List<FieldDef> subFields) {
        public FieldDef(String name, String esType, Map<String, Object> settings) {
            this(name, esType, settings, List.of());
        }
    }

    public record GeneratedIndex(String name, List<FieldDef> fields, List<Map<String, Object>> documents) {}

    public static List<GeneratedIndex> generateIndices(int numIndices, String prefix) {
        Map<String, FieldDef> firstSeenMapping = new HashMap<>();

        List<GeneratedIndex> result = new ArrayList<>(numIndices);
        for (int i = 0; i < numIndices; i++) {
            String indexName = prefix + i;
            int numFields = randomIntBetween(3, Math.min(15, FIELD_NAME_POOL.length));
            List<FieldDef> fields = new ArrayList<>();
            Set<String> usedNames = new HashSet<>();

            for (int j = 0; j < numFields; j++) {
                String fieldName;
                int attempts = 0;
                do {
                    fieldName = randomFrom(FIELD_NAME_POOL);
                    attempts++;
                } while (usedNames.contains(fieldName) && attempts < FIELD_NAME_POOL.length);
                if (usedNames.contains(fieldName)) {
                    break;
                }
                usedNames.add(fieldName);

                FieldDef existing = firstSeenMapping.get(fieldName);
                FieldDef mapping;
                if (existing != null && randomBoolean()) {
                    mapping = reuseMapping(existing);
                } else {
                    mapping = randomFieldDef(fieldName);
                    if (existing == null) {
                        firstSeenMapping.put(fieldName, mapping);
                    }
                }
                fields.add(mapping);
            }

            int numDocs = randomIntBetween(5, 20);
            List<Map<String, Object>> docs = new ArrayList<>(numDocs);
            for (int d = 0; d < numDocs; d++) {
                docs.add(generateDocument(fields));
            }
            result.add(new GeneratedIndex(indexName, fields, docs));
        }
        return result;
    }

    static FieldDef reuseMapping(FieldDef original) {
        if (randomBoolean()) {
            return original;
        }
        Map<String, Object> tweaked = new LinkedHashMap<>(original.settings());
        tweakSettings(original.esType(), tweaked);
        return new FieldDef(original.name(), original.esType(), tweaked, original.subFields());
    }

    private static void tweakSettings(String type, Map<String, Object> s) {
        switch (type) {
            case "keyword" -> {
                optionalPut(s, "ignore_above", randomIntBetween(64, 512));
                optionalPut(s, "doc_values", randomBoolean());
                optionalPut(s, "store", randomBoolean());
            }
            case "text" -> optionalPut(s, "store", randomBoolean());
            case "integer", "long", "short", "byte", "float", "double", "half_float" -> {
                optionalPut(s, "doc_values", randomBoolean());
                optionalPut(s, "coerce", randomBoolean());
            }
            case "unsigned_long" -> optionalPut(s, "doc_values", randomBoolean());
            case "scaled_float" -> optionalPut(s, "scaling_factor", randomFrom(10, 100, 1000));
            case "date", "date_nanos" -> optionalPut(s, "doc_values", randomBoolean());
            case "geo_point" -> optionalPut(s, "ignore_malformed", randomBoolean());
            case "geo_shape" -> optionalPut(s, "orientation", randomFrom("right", "left"));
            case "flattened" -> optionalPut(s, "depth_limit", randomIntBetween(5, 50));
            default -> {
            }
        }
    }

    private static void optionalPut(Map<String, Object> m, String key, Object val) {
        if (randomBoolean()) {
            m.put(key, val);
        }
    }

    static FieldDef randomFieldDef(String fieldName) {
        String type = randomFrom(WEIGHTED_TYPES);
        Map<String, Object> settings = generateSettings(type);
        List<FieldDef> subFields = generateSubFields(type);
        return new FieldDef(fieldName, type, settings, subFields);
    }

    static Map<String, Object> generateSettings(String type) {
        Map<String, Object> s = new LinkedHashMap<>();
        switch (type) {
            case "keyword" -> {
                optionalSet(s, 3, "doc_values", randomBoolean());
                optionalSet(s, 2, "store", true);
                optionalSet(s, 2, "ignore_above", randomIntBetween(64, 8191));
                optionalSet(s, 1, "index", randomBoolean());
                optionalSet(s, 1, "index_options", randomFrom("docs", "freqs"));
                optionalSet(s, 1, "norms", randomBoolean());
                optionalSet(s, 1, "similarity", randomFrom("BM25", "boolean"));
                optionalSet(s, 1, "split_queries_on_whitespace", true);
                optionalSet(s, 1, "eager_global_ordinals", true);
                optionalSet(s, 1, "null_value", "null_kw_" + randomAlphaOfLength(3));
            }
            case "constant_keyword" -> s.put("value", "const_" + randomAlphaOfLength(randomIntBetween(3, 8)));
            case "wildcard" -> {
                optionalSet(s, 2, "ignore_above", randomIntBetween(64, 8191));
                optionalSet(s, 1, "null_value", "null_wc_" + randomAlphaOfLength(3));
            }
            case "text" -> {
                optionalSet(s, 2, "store", true);
                optionalSet(s, 1, "analyzer", randomFrom(BUILTIN_ANALYZERS));
                optionalSet(s, 1, "search_analyzer", randomFrom(BUILTIN_ANALYZERS));
                optionalSet(s, 1, "index", randomBoolean());
                optionalSet(s, 1, "index_options", randomFrom("docs", "freqs", "positions", "offsets"));
                optionalSet(s, 1, "norms", randomBoolean());
                optionalSet(s, 1, "index_phrases", true);
                optionalSet(s, 1, "position_increment_gap", randomFrom(100, 0, 50, 200));
                optionalSet(s, 1, "similarity", randomFrom("BM25", "boolean"));
                optionalSet(s, 1, "term_vector", randomFrom("no", "yes", "with_positions", "with_offsets", "with_positions_offsets"));
                optionalSet(s, 1, "eager_global_ordinals", true);
                optionalSet(s, 1, "fielddata", true);
            }
            case "match_only_text" -> {
            }
            case "boolean" -> {
                optionalSet(s, 2, "doc_values", randomBoolean());
                optionalSet(s, 2, "store", true);
                optionalSet(s, 1, "index", randomBoolean());
                optionalSet(s, 1, "ignore_malformed", true);
                optionalSet(s, 1, "null_value", randomBoolean());
            }
            case "integer", "long", "short", "byte" -> {
                optionalSet(s, 3, "doc_values", randomBoolean());
                optionalSet(s, 2, "store", true);
                optionalSet(s, 2, "ignore_malformed", true);
                optionalSet(s, 2, "coerce", randomBoolean());
                optionalSet(s, 1, "index", randomBoolean());
                if ("integer".equals(type) || "long".equals(type)) {
                    optionalSet(s, 1, "null_value", randomIntBetween(-100, 100));
                }
            }
            case "float", "double", "half_float" -> {
                optionalSet(s, 3, "doc_values", randomBoolean());
                optionalSet(s, 2, "store", true);
                optionalSet(s, 2, "ignore_malformed", true);
                optionalSet(s, 2, "coerce", randomBoolean());
                optionalSet(s, 1, "index", randomBoolean());
            }
            case "scaled_float" -> {
                s.put("scaling_factor", randomFrom(10, 100, 1000, 10000));
                optionalSet(s, 3, "doc_values", randomBoolean());
                optionalSet(s, 2, "store", true);
                optionalSet(s, 2, "ignore_malformed", true);
                optionalSet(s, 2, "coerce", randomBoolean());
                optionalSet(s, 1, "index", randomBoolean());
            }
            case "unsigned_long" -> {
                optionalSet(s, 3, "doc_values", randomBoolean());
                optionalSet(s, 2, "store", true);
                optionalSet(s, 2, "ignore_malformed", true);
                optionalSet(s, 1, "index", randomBoolean());
            }
            case "date", "date_nanos" -> {
                optionalSet(s, 3, "doc_values", randomBoolean());
                optionalSet(s, 2, "store", true);
                optionalSet(s, 2, "ignore_malformed", true);
                optionalSet(s, 1, "index", randomBoolean());
                optionalSet(s, 1, "format", randomFrom(DATE_FORMATS));
                optionalSet(s, 1, "locale", randomFrom("en", "de", "fr", "ja", "und"));
            }
            case "ip" -> {
                optionalSet(s, 3, "doc_values", randomBoolean());
                optionalSet(s, 2, "store", true);
                optionalSet(s, 2, "ignore_malformed", true);
                optionalSet(s, 1, "index", randomBoolean());
                optionalSet(s, 1, "null_value", "0.0.0.0");
            }
            case "version" -> {
            }
            case "binary" -> {
                optionalSet(s, 2, "doc_values", true);
                optionalSet(s, 2, "store", true);
            }
            case "integer_range", "long_range", "float_range", "double_range" -> {
                optionalSet(s, 2, "coerce", randomBoolean());
                optionalSet(s, 2, "doc_values", randomBoolean());
                optionalSet(s, 1, "index", randomBoolean());
                optionalSet(s, 1, "store", true);
            }
            case "date_range" -> {
                optionalSet(s, 2, "coerce", randomBoolean());
                optionalSet(s, 2, "doc_values", randomBoolean());
                optionalSet(s, 1, "index", randomBoolean());
                optionalSet(s, 1, "store", true);
                optionalSet(s, 1, "format", randomFrom(DATE_FORMATS));
            }
            case "ip_range" -> {
                optionalSet(s, 2, "coerce", randomBoolean());
                optionalSet(s, 2, "doc_values", randomBoolean());
                optionalSet(s, 1, "index", randomBoolean());
                optionalSet(s, 1, "store", true);
            }
            case "flattened" -> {
                optionalSet(s, 2, "doc_values", randomBoolean());
                optionalSet(s, 2, "depth_limit", randomFrom(5, 10, 20, 50));
                optionalSet(s, 1, "eager_global_ordinals", true);
                optionalSet(s, 1, "ignore_above", randomIntBetween(256, 8191));
                optionalSet(s, 1, "index", randomBoolean());
                optionalSet(s, 1, "index_options", randomFrom("docs", "freqs"));
                optionalSet(s, 1, "null_value", "null_flat");
                optionalSet(s, 1, "similarity", randomFrom("BM25", "boolean"));
                optionalSet(s, 1, "split_queries_on_whitespace", true);
            }
            case "geo_point" -> {
                optionalSet(s, 2, "ignore_malformed", true);
                optionalSet(s, 1, "ignore_z_value", randomBoolean());
                optionalSet(s, 1, "index", randomBoolean());
            }
            case "geo_shape" -> {
                optionalSet(s, 2, "orientation", randomFrom("right", "left"));
                optionalSet(s, 2, "ignore_malformed", true);
                optionalSet(s, 1, "ignore_z_value", randomBoolean());
                optionalSet(s, 1, "coerce", true);
                optionalSet(s, 1, "index", randomBoolean());
                optionalSet(s, 1, "doc_values", randomBoolean());
            }
            case "point" -> {
                optionalSet(s, 2, "ignore_malformed", true);
                optionalSet(s, 1, "ignore_z_value", randomBoolean());
            }
            case "shape" -> {
                optionalSet(s, 2, "orientation", randomFrom("right", "left"));
                optionalSet(s, 2, "ignore_malformed", true);
                optionalSet(s, 1, "ignore_z_value", randomBoolean());
                optionalSet(s, 1, "coerce", true);
            }
            case "token_count" -> {
                s.put("analyzer", randomFrom(BUILTIN_ANALYZERS));
                optionalSet(s, 2, "doc_values", randomBoolean());
                optionalSet(s, 1, "index", randomBoolean());
                optionalSet(s, 1, "store", true);
                optionalSet(s, 1, "enable_position_increments", randomBoolean());
                optionalSet(s, 1, "null_value", 0);
            }
            case "completion" -> {
                optionalSet(s, 2, "analyzer", randomFrom(BUILTIN_ANALYZERS));
                optionalSet(s, 1, "search_analyzer", randomFrom(BUILTIN_ANALYZERS));
                optionalSet(s, 1, "max_input_length", randomFrom(50, 20, 100));
                optionalSet(s, 1, "preserve_separators", randomBoolean());
                optionalSet(s, 1, "preserve_position_increments", randomBoolean());
            }
            case "search_as_you_type" -> {
                optionalSet(s, 2, "max_shingle_size", randomFrom(2, 3, 4));
                optionalSet(s, 1, "analyzer", randomFrom(BUILTIN_ANALYZERS));
                optionalSet(s, 1, "search_analyzer", randomFrom(BUILTIN_ANALYZERS));
                optionalSet(s, 1, "index", randomBoolean());
                optionalSet(s, 1, "index_options", randomFrom("docs", "freqs", "positions", "offsets"));
                optionalSet(s, 1, "norms", randomBoolean());
                optionalSet(s, 1, "store", true);
                optionalSet(s, 1, "similarity", randomFrom("BM25", "boolean"));
                optionalSet(s, 1, "term_vector", randomFrom("no", "yes", "with_positions"));
            }
            case "rank_feature" -> optionalSet(s, 3, "positive_score_impact", randomBoolean());
            case "rank_features" -> {
            }
            case "aggregate_metric_double" -> {
                List<String> allMetrics = List.of("min", "max", "sum", "value_count");
                int count = randomIntBetween(1, 4);
                List<String> metrics = new ArrayList<>();
                for (int i = 0; i < count; i++) {
                    String m = allMetrics.get(i);
                    if (metrics.contains(m) == false) metrics.add(m);
                }
                if (metrics.isEmpty()) metrics.add("max");
                s.put("metrics", metrics);
                s.put("default_metric", metrics.get(randomIntBetween(0, metrics.size() - 1)));
            }
            case "histogram" -> {
            }
            case "dense_vector" -> {
                int dims = randomFrom(3, 8, 16, 32, 64, 128);
                s.put("dims", dims);
                boolean indexed = randomBoolean();
                s.put("index", indexed);
                optionalSet(s, 2, "element_type", randomFrom("float", "byte"));
                if (indexed) {
                    optionalSet(s, 2, "similarity", randomFrom("l2_norm", "dot_product", "cosine", "max_inner_product"));
                }
            }
            default -> {
            }
        }
        return s;
    }

    /**
     * Adds a setting with probability chance/10
     */
    private static void optionalSet(Map<String, Object> s, int chance, String key, Object val) {
        if (randomIntBetween(0, 9) < chance) s.put(key, val);
    }

    static List<FieldDef> generateSubFields(String parentType) {
        List<FieldDef> subFields = new ArrayList<>();
        switch (parentType) {
            case "text", "match_only_text" -> {
                if (randomIntBetween(0, 9) < 3) {
                    subFields.add(new FieldDef("raw", "keyword", Map.of()));
                }
            }
            case "keyword" -> {
                if (randomIntBetween(0, 9) < 2) {
                    subFields.add(new FieldDef("text", "text", Map.of()));
                }
            }
            case "search_as_you_type" -> {
                // auto-generated sub-fields; no custom sub-fields needed
            }
            default -> {
                /* no sub-fields */ }
        }
        return subFields;
    }

    public static String toMappingJson(List<FieldDef> fields) {
        StringBuilder sb = new StringBuilder();
        sb.append("\"properties\": {");
        for (int i = 0; i < fields.size(); i++) {
            if (i > 0) sb.append(",");
            appendFieldJson(sb, fields.get(i));
        }
        sb.append("}");
        return sb.toString();
    }

    private static void appendFieldJson(StringBuilder sb, FieldDef field) {
        sb.append("\"").append(field.name()).append("\": {");
        sb.append("\"type\": \"").append(field.esType()).append("\"");
        for (Map.Entry<String, Object> entry : field.settings().entrySet()) {
            sb.append(", \"").append(entry.getKey()).append("\": ");
            appendJsonValue(sb, entry.getValue());
        }
        if (field.subFields().isEmpty() == false) {
            sb.append(", \"fields\": {");
            for (int i = 0; i < field.subFields().size(); i++) {
                if (i > 0) sb.append(",");
                appendFieldJson(sb, field.subFields().get(i));
            }
            sb.append("}");
        }
        sb.append("}");
    }

    @SuppressWarnings("unchecked")
    private static void appendJsonValue(StringBuilder sb, Object value) {
        if (value instanceof String strVal) {
            sb.append("\"").append(escapeJson(strVal)).append("\"");
        } else if (value instanceof Boolean || value instanceof Number) {
            sb.append(value);
        } else if (value instanceof List<?> list) {
            sb.append("[");
            for (int i = 0; i < list.size(); i++) {
                if (i > 0) sb.append(",");
                appendJsonValue(sb, list.get(i));
            }
            sb.append("]");
        } else {
            sb.append("\"").append(escapeJson(String.valueOf(value))).append("\"");
        }
    }

    public static Map<String, Object> generateDocument(List<FieldDef> fields) {
        Map<String, Object> doc = new LinkedHashMap<>();
        for (FieldDef field : fields) {
            if (field.esType().equals("constant_keyword")) continue;
            if (randomIntBetween(0, 9) < 2) continue;
            Object value = generateValue(field);
            if (value != null) {
                if (randomIntBetween(0, 9) == 0 && isMultiValueCapable(field.esType())) {
                    List<Object> mv = new ArrayList<>();
                    mv.add(value);
                    Object second = generateValue(field);
                    if (second != null) mv.add(second);
                    doc.put(field.name(), mv);
                } else {
                    doc.put(field.name(), value);
                }
            }
        }
        return doc;
    }

    private static boolean isMultiValueCapable(String type) {
        return switch (type) {
            case "constant_keyword", "geo_shape", "shape", "dense_vector", "aggregate_metric_double", "histogram", "rank_feature",
                "rank_features", "completion" -> false;
            default -> true;
        };
    }

    @SuppressWarnings("unchecked")
    static Object generateValue(FieldDef field) {
        return switch (field.esType()) {
            case "keyword" -> "kw_" + randomAlphaOfLength(randomIntBetween(3, 10));
            case "constant_keyword" -> null;
            case "wildcard" -> "wc_" + randomAlphaOfLength(randomIntBetween(3, 10));
            case "text" -> randomFrom("alpha", "bravo", "charlie", "delta", "echo") + " " + randomAlphaOfLength(randomIntBetween(3, 12));
            case "match_only_text" -> randomFrom("foxtrot", "golf", "hotel") + " " + randomAlphaOfLength(randomIntBetween(3, 12));
            case "search_as_you_type" -> randomFrom(
                "quick brown fox",
                "lazy dog jumped",
                "hello world test",
                "elastic search query",
                "random text here"
            );
            case "boolean" -> randomBoolean();
            case "integer" -> randomIntBetween(-1000, 1000);
            case "long" -> randomLongBetween(-1_000_000L, 1_000_000L);
            case "short" -> randomIntBetween(Short.MIN_VALUE, Short.MAX_VALUE);
            case "byte" -> randomIntBetween(Byte.MIN_VALUE, Byte.MAX_VALUE);
            case "float", "double", "half_float" -> Math.round(randomDoubleBetween(-100.0, 100.0, true) * 100.0) / 100.0;
            case "scaled_float" -> Math.round(randomDoubleBetween(0, 100.0, true) * 100.0) / 100.0;
            case "unsigned_long" -> randomLongBetween(0, 1_000_000L);
            case "date" -> randomDate();
            case "date_nanos" -> randomDateNanos();
            case "ip" -> randomIpV4();
            case "version" -> randomVersionString();
            case "binary" -> Base64.getEncoder()
                .encodeToString(randomAlphaOfLength(randomIntBetween(4, 20)).getBytes(java.nio.charset.StandardCharsets.UTF_8));
            case "integer_range" -> {
                int lo = randomIntBetween(-100, 50);
                yield Map.of("gte", lo, "lte", lo + randomIntBetween(1, 100));
            }
            case "long_range" -> {
                long lo = randomLongBetween(-10000L, 5000L);
                yield Map.of("gte", lo, "lte", lo + randomIntBetween(1, 10000));
            }
            case "float_range", "double_range" -> {
                double lo = Math.round(randomDoubleBetween(-100.0, 50.0, true) * 100.0) / 100.0;
                yield Map.of("gte", lo, "lte", lo + Math.round(randomDoubleBetween(0.1, 100.0, true) * 100.0) / 100.0);
            }
            case "date_range" -> {
                Instant i1 = randomInstantBetween(DATE_RANGE_START, DATE_RANGE_END).truncatedTo(ChronoUnit.MILLIS);
                Instant i2 = randomInstantBetween(DATE_RANGE_START, DATE_RANGE_END).truncatedTo(ChronoUnit.MILLIS);
                yield i1.isBefore(i2)
                    ? Map.of("gte", i1.toString(), "lte", i2.toString())
                    : Map.of("gte", i2.toString(), "lte", i1.toString());
            }
            case "ip_range" -> randomFrom("10.0.0.0/24", "192.168.1.0/28", Map.of("gte", "10.0.0.0", "lte", "10.0.0.255"));
            case "flattened" -> {
                Map<String, Object> flat = new LinkedHashMap<>();
                flat.put("key_" + randomAlphaOfLength(3), "val_" + randomAlphaOfLength(5));
                flat.put("num", String.valueOf(randomIntBetween(1, 100)));
                if (randomBoolean()) {
                    flat.put("nested_" + randomAlphaOfLength(2), Map.of("inner", "val_" + randomAlphaOfLength(3)));
                }
                yield flat;
            }
            case "geo_point" -> Map.of(
                "lat",
                Math.round(randomDoubleBetween(-90.0, 90.0, true) * 1000.0) / 1000.0,
                "lon",
                Math.round(randomDoubleBetween(-180.0, 180.0, true) * 1000.0) / 1000.0
            );
            case "geo_shape" -> Map.of(
                "type",
                "point",
                "coordinates",
                List.of(
                    Math.round(randomDoubleBetween(-180.0, 180.0, true) * 1000.0) / 1000.0,
                    Math.round(randomDoubleBetween(-90.0, 90.0, true) * 1000.0) / 1000.0
                )
            );
            case "point" -> List.of(
                Math.round(randomDoubleBetween(-1000.0, 1000.0, true) * 100.0) / 100.0,
                Math.round(randomDoubleBetween(-1000.0, 1000.0, true) * 100.0) / 100.0
            );
            case "shape" -> Map.of(
                "type",
                "point",
                "coordinates",
                List.of(
                    Math.round(randomDoubleBetween(-1000.0, 1000.0, true) * 100.0) / 100.0,
                    Math.round(randomDoubleBetween(-1000.0, 1000.0, true) * 100.0) / 100.0
                )
            );
            case "token_count" -> randomFrom("one two three", "hello", "quick brown fox jumped", "a b c d e f", "single");
            case "completion" -> randomBoolean()
                ? randomFrom("Elasticsearch", "Elastic Stack", "Kibana", "Logstash", "Beats")
                : Map.of("input", List.of(randomFrom("search", "query", "index", "mapping", "field")), "weight", randomIntBetween(1, 100));
            case "rank_feature" -> Math.round(randomDoubleBetween(0.1, 1000.0, true) * 100.0) / 100.0;
            case "rank_features" -> {
                Map<String, Object> feats = new LinkedHashMap<>();
                feats.put("feat_" + randomAlphaOfLength(3), Math.round(randomDoubleBetween(0.1, 100.0, true) * 100.0) / 100.0);
                feats.put("feat_" + randomAlphaOfLength(3), Math.round(randomDoubleBetween(0.1, 100.0, true) * 100.0) / 100.0);
                yield feats;
            }
            case "aggregate_metric_double" -> {
                List<String> metrics = (List<String>) field.settings().getOrDefault("metrics", List.of("min", "max", "sum", "value_count"));
                Map<String, Object> amd = new LinkedHashMap<>();
                double base = randomDoubleBetween(-100.0, 100.0, true);
                for (String m : metrics) {
                    switch (m) {
                        case "min" -> amd.put("min", Math.round(base * 100.0) / 100.0);
                        case "max" -> amd.put("max", Math.round((base + randomDoubleBetween(1, 200, true)) * 100.0) / 100.0);
                        case "sum" -> amd.put("sum", Math.round(randomDoubleBetween(-1000, 1000, true) * 100.0) / 100.0);
                        case "value_count" -> amd.put("value_count", randomIntBetween(1, 1000));
                        default -> {
                        }
                    }
                }
                yield amd;
            }
            case "histogram" -> {
                int bins = randomIntBetween(2, 6);
                List<Double> values = new ArrayList<>(bins);
                List<Integer> counts = new ArrayList<>(bins);
                double v = randomDoubleBetween(-100.0, 0, true);
                for (int b = 0; b < bins; b++) {
                    values.add(Math.round(v * 100.0) / 100.0);
                    counts.add(randomIntBetween(0, 100));
                    v += randomDoubleBetween(0.1, 50.0, true);
                }
                yield Map.of("values", values, "counts", counts);
            }
            case "dense_vector" -> {
                int dims = ((Number) field.settings().getOrDefault("dims", 3)).intValue();
                String elemType = String.valueOf(field.settings().getOrDefault("element_type", "float"));
                List<Number> vec = new ArrayList<>(dims);
                for (int d = 0; d < dims; d++) {
                    if ("byte".equals(elemType)) {
                        vec.add(randomIntBetween(-128, 127));
                    } else {
                        vec.add(Math.round(randomDoubleBetween(-10.0, 10.0, true) * 1000.0) / 1000.0);
                    }
                }
                yield vec;
            }

            default -> null;
        };
    }

    private static final Instant DATE_RANGE_START = Instant.parse("2020-01-01T00:00:00Z");
    private static final Instant DATE_RANGE_END = Instant.parse("2025-12-28T23:59:59Z");

    static String randomDate() {
        return randomInstantBetween(DATE_RANGE_START, DATE_RANGE_END).truncatedTo(ChronoUnit.MILLIS).toString();
    }

    static String randomDateNanos() {
        return randomInstantBetween(DATE_RANGE_START, DATE_RANGE_END).toString();
    }

    static String randomIpV4() {
        return NetworkAddress.format(randomIp(true));
    }

    static String randomVersionString() {
        return String.format(Locale.ROOT, "%d.%d.%d", randomIntBetween(0, 10), randomIntBetween(0, 20), randomIntBetween(0, 100));
    }

    public static String toDocumentJson(Map<String, Object> doc) {
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, Object> entry : doc.entrySet()) {
            if (first == false) sb.append(",");
            first = false;
            sb.append("\"").append(entry.getKey()).append("\":");
            appendDocValue(sb, entry.getValue());
        }
        sb.append("}");
        return sb.toString();
    }

    private static void appendDocValue(StringBuilder sb, Object value) {
        if (value == null) {
            sb.append("null");
        } else if (value instanceof String s) {
            sb.append("\"").append(escapeJson(s)).append("\"");
        } else if (value instanceof Boolean || value instanceof Number) {
            sb.append(value);
        } else if (value instanceof List<?> list) {
            sb.append("[");
            for (int i = 0; i < list.size(); i++) {
                if (i > 0) sb.append(",");
                appendDocValue(sb, list.get(i));
            }
            sb.append("]");
        } else if (value instanceof Map<?, ?> map) {
            sb.append("{");
            boolean first = true;
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                if (first == false) sb.append(",");
                first = false;
                sb.append("\"").append(entry.getKey()).append("\":");
                appendDocValue(sb, entry.getValue());
            }
            sb.append("}");
        } else {
            sb.append("\"").append(escapeJson(String.valueOf(value))).append("\"");
        }
    }

    private static String escapeJson(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t");
    }
}

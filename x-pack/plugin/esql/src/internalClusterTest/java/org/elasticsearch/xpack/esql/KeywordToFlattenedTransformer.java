/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * POC helper that rewrites every {@code keyword} field declaration in a csv-spec mapping JSON to
 * {@code flattened}, and wraps document source values for those fields so that they remain valid
 * input to a {@code flattened} field.
 * <p>
 * The point of this helper is to take the existing csv-spec test corpus (which assumes keyword
 * fields) and run it against indices where those fields are mapped as {@code flattened} instead.
 * That surfaces which ES|QL functions and operators behave the same way against {@code flattened}
 * as they do against {@code keyword} — the broader goal being to converge their behavior.
 * <p>
 * Wrapping convention: for each keyword field path {@code F} that this helper rewrites to
 * {@code flattened}, a source value originally written as {@code "F": <value>} is replaced with
 * {@code "F": {"v": <value>}}, where {@code "v"} is {@link #WRAPPER_SUBKEY}. The wrapper key is a
 * fixed constant so callers can later retrieve the original keyword value with
 * {@code FIELD_EXTRACT(F, "v")} or compare against the JSON form when running the unmodified spec.
 * <p>
 * Sub-fields under a {@code "fields"} block (multi-fields) are intentionally left alone. Their
 * source data is the parent field's scalar value, and {@code flattened} requires a JSON object as
 * input, so converting a multi-field's keyword type to {@code flattened} would not produce valid
 * documents.
 *
 * <h2>Keyword fields that are intentionally NOT rewritten</h2>
 * Some mapping parameters either do not exist on {@code flattened} at all (and would cause
 * {@code MapperParsingException} at index creation time) or rely on the source value being a scalar
 * (which it no longer is once we wrap it as {@code {"v": ...}}). Keyword fields declaring any of
 * the following parameters are passed through unchanged &mdash; they keep type {@code keyword},
 * are not added to the path set, and so neither their source values nor query references to them
 * are rewritten elsewhere in the POC pipeline. The denylist is exposed via
 * {@link #PARAMS_INCOMPATIBLE_WITH_FLATTENED}.
 * <ul>
 *   <li>{@code time_series_dimension} &mdash; a TSDB dimension marker that exists on
 *       {@code keyword} but not on {@code flattened} (the flattened equivalent is the
 *       {@code time_series_dimensions} plural list with sub-paths, which is incompatible with the
 *       wrapping convention used here). Without this exclusion, every TSDB dataset's index
 *       creation fails with
 *       {@code unknown parameter [time_series_dimension] on mapper [...] of type [flattened]}.</li>
 *   <li>{@code time_series_metric} &mdash; a TSDB metric marker. Defensive: in practice only set
 *       on numeric fields, but a stray entry on a keyword would also fail mapper parsing.</li>
 *   <li>{@code fields} &mdash; multi-field declarations. The sub-fields under {@code fields}
 *       index the parent's source, which becomes a JSON object once we wrap it. That breaks any
 *       sub-field whose mapper expects a scalar (typically all of {@code keyword}, {@code text},
 *       {@code wildcard}, etc.).</li>
 *   <li>{@code copy_to} &mdash; copies the field's source value into a sibling field. After
 *       wrapping, the destination receives {@code {"v": ...}} instead of the original scalar,
 *       which usually does not match the destination's mapper.</li>
 *   <li>{@code script} &mdash; scripted/runtime fields. Their value is not produced from
 *       {@code _source}, so wrapping the source has no effect, and converting the type silently
 *       changes the field's semantics in ways that are not the focus of this POC.</li>
 * </ul>
 * Anything else that flattened still rejects can be added to the denylist; the failure mode is
 * always the same {@code MapperParsingException} at index creation time, so new entries are easy
 * to locate from the test failure log.
 */
public final class KeywordToFlattenedTransformer {

    /** The single sub-key under which scalar keyword values are wrapped inside the flattened object. */
    public static final String WRAPPER_SUBKEY = "v";

    /**
     * Mapping parameters whose presence on a {@code keyword} field declaration makes the
     * keyword&rarr;flattened rewrite unsafe. Encountering any of these causes the field to be
     * left untouched. See class-level Javadoc for the per-parameter rationale.
     */
    public static final Set<String> PARAMS_INCOMPATIBLE_WITH_FLATTENED = Set.of(
        "time_series_dimension",
        "time_series_metric",
        "fields",
        "copy_to",
        "script"
    );

    private static final String KEYWORD_TYPE = "keyword";
    private static final String FLATTENED_TYPE = "flattened";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private KeywordToFlattenedTransformer() {}

    /**
     * Result of {@link #transformMapping(String)}.
     *
     * @param transformedMapping the rewritten mapping JSON, or the original input when no keyword fields were found
     * @param keywordFieldPaths  the dotted paths of every field whose type was rewritten from keyword to flattened
     */
    public record MappingResult(String transformedMapping, Set<String> keywordFieldPaths) {}

    /**
     * Walks {@code originalMapping} JSON, rewrites every {@code "type":"keyword"} declaration reachable
     * from the root via {@code "properties"} chains to {@code "type":"flattened"}, and returns the
     * transformed mapping along with the dotted paths of every rewritten field.
     * <p>
     * Multi-field sub-fields under {@code "fields"} are not rewritten — see class-level Javadoc.
     * <p>
     * If no keyword fields are found, the original mapping JSON is returned unchanged and the path
     * set is empty.
     */
    public static MappingResult transformMapping(String originalMapping) throws IOException {
        JsonNode root = MAPPER.readTree(originalMapping);
        if (root.isObject() == false) {
            return new MappingResult(originalMapping, Collections.emptySet());
        }
        JsonNode properties = root.path("properties");
        if (properties.isObject() == false) {
            return new MappingResult(originalMapping, Collections.emptySet());
        }
        Set<String> paths = new HashSet<>();
        rewriteKeywords((ObjectNode) properties, "", paths);
        if (paths.isEmpty()) {
            return new MappingResult(originalMapping, Collections.emptySet());
        }
        return new MappingResult(MAPPER.writeValueAsString(root), paths);
    }

    /**
     * Returns a new document source JSON where every top-level key matching a path in
     * {@code keywordFieldPaths} has its value wrapped in {@code {"v": <value>}}. Keys not in
     * {@code keywordFieldPaths}, and missing keys, are passed through unchanged.
     * <p>
     * Documents that are not JSON objects, and documents with no matching keys, are returned
     * unchanged.
     * <p>
     * Note: csv-spec source documents use flat dotted keys (e.g. {@code "city.name": "Amsterdam"}),
     * so a dotted path like {@code city.name} resolves to a single literal top-level key here, which
     * matches what {@code CsvTestsDataLoader.parseDocument} produces.
     */
    public static String wrapKeywordValuesAsFlattened(String documentJson, Set<String> keywordFieldPaths) throws IOException {
        if (keywordFieldPaths.isEmpty()) {
            return documentJson;
        }
        JsonNode root = MAPPER.readTree(documentJson);
        if (root.isObject() == false) {
            return documentJson;
        }
        ObjectNode doc = (ObjectNode) root;
        boolean modified = false;
        for (String path : keywordFieldPaths) {
            JsonNode existing = doc.get(path);
            if (existing == null) {
                continue;
            }
            ObjectNode wrapped = MAPPER.createObjectNode();
            wrapped.set(WRAPPER_SUBKEY, existing);
            doc.set(path, wrapped);
            modified = true;
        }
        return modified ? MAPPER.writeValueAsString(doc) : documentJson;
    }

    private static void rewriteKeywords(ObjectNode propertiesNode, String prefix, Set<String> paths) {
        Iterator<Map.Entry<String, JsonNode>> it = propertiesNode.fields();
        while (it.hasNext()) {
            Map.Entry<String, JsonNode> entry = it.next();
            JsonNode fieldNode = entry.getValue();
            if (fieldNode.isObject() == false) {
                continue;
            }
            ObjectNode fieldObj = (ObjectNode) fieldNode;
            String fullPath = prefix.isEmpty() ? entry.getKey() : prefix + "." + entry.getKey();

            JsonNode typeNode = fieldObj.get("type");
            if (typeNode != null
                && typeNode.isTextual()
                && KEYWORD_TYPE.equals(typeNode.asText())
                && hasIncompatibleParameter(fieldObj) == false) {
                fieldObj.put("type", FLATTENED_TYPE);
                paths.add(fullPath);
            }
            JsonNode nested = fieldObj.path("properties");
            if (nested.isObject()) {
                rewriteKeywords((ObjectNode) nested, fullPath, paths);
            }
            // Multi-fields under "fields" are intentionally skipped here; if a keyword parent has
            // a "fields" block we already refuse to rewrite the parent itself via
            // hasIncompatibleParameter, which keeps the multi-field children consistent with their
            // (still scalar) parent source.
        }
    }

    /**
     * Returns {@code true} if {@code fieldObj} declares any parameter that makes the
     * keyword&rarr;flattened rewrite unsafe. See {@link #PARAMS_INCOMPATIBLE_WITH_FLATTENED} and
     * the class-level Javadoc for the full rationale per parameter.
     */
    private static boolean hasIncompatibleParameter(ObjectNode fieldObj) {
        for (String param : PARAMS_INCOMPATIBLE_WITH_FLATTENED) {
            if (fieldObj.has(param)) {
                return true;
            }
        }
        return false;
    }
}

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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Helper that rewrites every {@code keyword} field declaration in a csv-spec mapping JSON to
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
 * are rewritten elsewhere in the pipeline. The denylist is exposed via
 * {@link #PARAMS_INCOMPATIBLE_WITH_FLATTENED}.
 * <ul>
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
 *       changes the field's semantics in ways that are not the focus of this test variant.</li>
 * </ul>
 * Anything else that flattened still rejects can be added to the denylist; the failure mode is
 * always the same {@code MapperParsingException} at index creation time, so new entries are easy
 * to locate from the test failure log.
 *
 * <h2>Keyword sub-fields of an existing flattened field</h2>
 * A field already declared {@code flattened} is treated as a leaf: the walk does not descend into
 * its {@code "properties"}. Those entries are the flattened field's typed <em>mapped sub-fields</em>,
 * not object sub-fields, and they may only be a fixed set of scalar types &mdash; {@code flattened}
 * is not one of them. Rewriting such a keyword sub-field (e.g. {@code labels.service} in
 * {@code flattened_typed}) to {@code flattened} would emit an illegal "flattened sub-field of a
 * flattened field" mapping and fail index creation. Mapped sub-fields are also queried directly
 * (e.g. {@code labels.service}) rather than through {@code field_extract}, so they are outside this
 * variant's rewrite scope regardless. See {@link #isFlattenedField}.
 *
 * <h2>TSDB dimension keyword fields</h2>
 * {@code keyword} fields that declare {@code time_series_dimension: true} are handled specially
 * rather than being left as {@code keyword}: they are converted to {@code flattened} with
 * {@code time_series_dimensions: ["v"]}. This makes the dimension value reachable via
 * {@code field_extract(field, "v")} just like any other converted field. Callers that also create
 * the index must update {@code index.routing_path} accordingly &mdash; bare paths such as
 * {@code "pod"} must become {@code "pod.v"} &mdash; because TSDB resolves routing through the
 * flattened sub-key, not the parent field name.
 *
 * <h2>Caller-supplied path exclusions</h2>
 * The overload {@link #transformMapping(String, Set)} accepts a set of dotted field paths that the
 * caller wants to leave as {@code keyword} regardless of their declared parameters. This is the
 * extension point used by callers that need to keep certain keyword fields intact for reasons that
 * are not visible in the mapping itself, the canonical example being enrich-policy {@code match}
 * fields: converting the source field to {@code flattened} breaks the enrich-policy reindex into
 * the internal {@code .enrich-*} index (which expects a {@code keyword}), and that failure poisons
 * the test resource loader for the rest of the JVM. Excluded paths are not added to the returned
 * {@code keywordFieldPaths} either, so source-value wrapping for those fields is also suppressed.
 */
public final class KeywordToFlattenedTransformer {

    /** The single sub-key under which scalar keyword values are wrapped inside the flattened object. */
    public static final String WRAPPER_SUBKEY = "v";

    /**
     * Mapping parameters whose presence on a {@code keyword} field declaration makes the
     * keyword&rarr;flattened rewrite unsafe. Encountering any of these causes the field to be
     * left untouched. See class-level Javadoc for the per-parameter rationale.
     */
    public static final Set<String> PARAMS_INCOMPATIBLE_WITH_FLATTENED = Set.of("time_series_metric", "fields", "copy_to", "script");

    private static final String KEYWORD_TYPE = "keyword";
    private static final String FLATTENED_TYPE = "flattened";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private KeywordToFlattenedTransformer() {}

    /**
     * One {@code keyword} field declaration that this helper intentionally left untouched, with
     * the reason for the skip. Skips fall into two categories:
     * <ul>
     *   <li>{@link SkipReason#INCOMPATIBLE_PARAMETER}: the field declares one of the parameters
     *       in {@link #PARAMS_INCOMPATIBLE_WITH_FLATTENED}; {@link #parameter} carries the
     *       offending parameter name.</li>
     *   <li>{@link SkipReason#CALLER_EXCLUDED_PATH}: the field's dotted path appeared in the
     *       caller-supplied {@code excludedPaths} set passed to
     *       {@link #transformMapping(String, Set)}; {@link #parameter} is {@code null}.</li>
     * </ul>
     * Callers (typically the test variant's logger) inventory these so the set of "fields where
     * {@code field_extract} is never exercised" is visible in run output rather than implicit.
     */
    public record SkippedField(String fieldPath, SkipReason reason, String parameter) {}

    /** Why {@link #transformMapping(String, Set)} left a {@code keyword} field declaration unchanged. */
    public enum SkipReason {
        /** Field declared a parameter in {@link #PARAMS_INCOMPATIBLE_WITH_FLATTENED}. */
        INCOMPATIBLE_PARAMETER,
        /** Field's dotted path was in the caller-supplied {@code excludedPaths} set. */
        CALLER_EXCLUDED_PATH
    }

    /**
     * Result of {@link #transformMapping(String)}.
     *
     * @param transformedMapping the rewritten mapping JSON, or the original input when no keyword fields were found
     * @param keywordFieldPaths  the dotted paths of every field whose type was rewritten from keyword to flattened
     * @param skippedFields      every {@code keyword} field declaration that was intentionally left untouched, in
     *                           declaration order, with the reason for the skip
     */
    public record MappingResult(String transformedMapping, Set<String> keywordFieldPaths, List<SkippedField> skippedFields) {}

    /**
     * Convenience overload that excludes no paths. Equivalent to
     * {@code transformMapping(originalMapping, Set.of())}.
     */
    public static MappingResult transformMapping(String originalMapping) throws IOException {
        return transformMapping(originalMapping, Set.of());
    }

    /**
     * Walks {@code originalMapping} JSON, rewrites every {@code "type":"keyword"} declaration reachable
     * from the root via {@code "properties"} chains to {@code "type":"flattened"}, and returns the
     * transformed mapping along with the dotted paths of every rewritten field.
     * <p>
     * Multi-field sub-fields under {@code "fields"} are not rewritten &mdash; see class-level
     * Javadoc.
     * <p>
     * Any field whose dotted path is in {@code excludedPaths} is left as {@code keyword} and is
     * <em>not</em> added to {@code keywordFieldPaths}. Use this to keep specific keyword fields
     * intact for reasons that are not encoded in the mapping itself (e.g. enrich-policy
     * {@code match_field}s; see class-level Javadoc).
     * <p>
     * If no keyword fields are rewritten the original mapping JSON is returned unchanged and the
     * path set is empty.
     */
    public static MappingResult transformMapping(String originalMapping, Set<String> excludedPaths) throws IOException {
        JsonNode root = MAPPER.readTree(originalMapping);
        if (root.isObject() == false) {
            return new MappingResult(originalMapping, Collections.emptySet(), List.of());
        }
        JsonNode properties = root.path("properties");
        if (properties.isObject() == false) {
            return new MappingResult(originalMapping, Collections.emptySet(), List.of());
        }
        Set<String> paths = new HashSet<>();
        List<SkippedField> skipped = new ArrayList<>();
        rewriteKeywords((ObjectNode) properties, "", paths, excludedPaths, skipped);
        if (paths.isEmpty()) {
            return new MappingResult(originalMapping, Collections.emptySet(), List.copyOf(skipped));
        }
        return new MappingResult(MAPPER.writeValueAsString(root), paths, List.copyOf(skipped));
    }

    /**
     * Returns the dotted paths of every leaf field declared in {@code originalMapping}, regardless
     * of declared type. The walk follows the same {@code "properties"} chain as
     * {@link #transformMapping(String, Set)} but emits a path for every entry that has a
     * {@code "type"} declaration, including non-keyword leaves (e.g. {@code long}, {@code ip},
     * {@code text}, {@code geo_point}). Multi-field sub-fields under {@code "fields"} are
     * intentionally excluded for the same reasons listed in the class-level Javadoc.
     * <p>
     * The motivating use case is cross-dataset scoping in
     * {@code CsvFlattenedKeywordIT.KeywordToFlattenedStrategy}: subtracting the union of
     * <em>non-keyword</em> paths across every dataset a query touches from the union of
     * <em>keyword</em> paths excludes a field whose name happens to be {@code keyword} in one
     * dataset and (say) {@code ip} in another, which would otherwise be wrapped in
     * {@code field_extract(...)} and produce a verifier error on the dataset where the field is
     * non-keyword.
     */
    public static Set<String> extractAllFieldPaths(String originalMapping) throws IOException {
        JsonNode root = MAPPER.readTree(originalMapping);
        if (root.isObject() == false) {
            return Set.of();
        }
        JsonNode properties = root.path("properties");
        if (properties.isObject() == false) {
            return Set.of();
        }
        Set<String> paths = new HashSet<>();
        collectFieldPaths((ObjectNode) properties, "", paths);
        return Set.copyOf(paths);
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

    /**
     * Walks {@code propertiesNode} (and its nested {@code properties} sub-objects) and adds the
     * dotted path of every entry that has a {@code "type"} declaration to {@code paths}.
     * Multi-field sub-fields under {@code "fields"} are skipped in line with the rest of this
     * helper. The walk does not look at the type's value, so this method captures all leaf
     * fields regardless of whether they would have been candidates for the keyword&rarr;flattened
     * rewrite.
     */
    private static void collectFieldPaths(ObjectNode propertiesNode, String prefix, Set<String> paths) {
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
            if (typeNode != null && typeNode.isTextual()) {
                paths.add(fullPath);
            }
            JsonNode nested = fieldObj.path("properties");
            if (nested.isObject() && isFlattenedField(fieldObj) == false) {
                collectFieldPaths((ObjectNode) nested, fullPath, paths);
            }
        }
    }

    private static void rewriteKeywords(
        ObjectNode propertiesNode,
        String prefix,
        Set<String> paths,
        Set<String> excludedPaths,
        List<SkippedField> skipped
    ) {
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
            if (typeNode != null && typeNode.isTextual() && KEYWORD_TYPE.equals(typeNode.asText())) {
                if (excludedPaths.contains(fullPath)) {
                    skipped.add(new SkippedField(fullPath, SkipReason.CALLER_EXCLUDED_PATH, null));
                } else if (fieldObj.has("time_series_dimension")) {
                    // TSDB dimension: convert to flattened with the wrapper sub-key as the dimension.
                    // The caller must also rewrite index.routing_path entries for this field from
                    // "field" to "field.v" so TSDB resolves routing through the flattened sub-key.
                    fieldObj.put("type", FLATTENED_TYPE);
                    fieldObj.remove("time_series_dimension");
                    fieldObj.putArray("time_series_dimensions").add(WRAPPER_SUBKEY);
                    paths.add(fullPath);
                } else {
                    String incompatibleParam = findIncompatibleParameter(fieldObj);
                    if (incompatibleParam != null) {
                        skipped.add(new SkippedField(fullPath, SkipReason.INCOMPATIBLE_PARAMETER, incompatibleParam));
                    } else {
                        fieldObj.put("type", FLATTENED_TYPE);
                        paths.add(fullPath);
                    }
                }
            }
            JsonNode nested = fieldObj.path("properties");
            if (nested.isObject() && isFlattenedField(fieldObj) == false) {
                rewriteKeywords((ObjectNode) nested, fullPath, paths, excludedPaths, skipped);
            }
            // Multi-fields under "fields" are intentionally skipped here; if a keyword parent has
            // a "fields" block we already refuse to rewrite the parent itself via
            // findIncompatibleParameter, which keeps the multi-field children consistent with their
            // (still scalar) parent source.
        }
    }

    /**
     * Returns {@code true} if {@code fieldObj} declares {@code "type":"flattened"}.
     * <p>
     * A {@code flattened} field's {@code "properties"} are typed <em>mapped sub-fields</em> (a
     * flattened-specific construct restricted to a fixed set of scalar types that does <em>not</em>
     * include {@code flattened} itself), not the object sub-fields that {@code "properties"} denotes
     * on an {@code object} mapper. Both walks therefore treat a flattened field as a leaf and do not
     * descend into it: rewriting a keyword mapped sub-field to {@code flattened} would emit an illegal
     * "flattened sub-field of a flattened field" mapping and fail index creation, and mapped sub-fields
     * are queried directly (e.g. {@code labels.service}) rather than through {@code field_extract},
     * so they are outside this variant's keyword&rarr;flattened rewrite scope anyway.
     */
    private static boolean isFlattenedField(ObjectNode fieldObj) {
        JsonNode typeNode = fieldObj.get("type");
        return typeNode != null && typeNode.isTextual() && FLATTENED_TYPE.equals(typeNode.asText());
    }

    /**
     * Returns the name of the first parameter on {@code fieldObj} that makes the
     * keyword&rarr;flattened rewrite unsafe, or {@code null} if {@code fieldObj} declares none of
     * them. See {@link #PARAMS_INCOMPATIBLE_WITH_FLATTENED} and the class-level Javadoc for the
     * full rationale per parameter. The order in which the set is iterated is unspecified, so the
     * returned parameter is "one of" the offending parameters &mdash; sufficient to identify the
     * declared mapping feature for logging while keeping the test variant deterministic enough to
     * grep against (the field path, not the parameter, is the stable identifier of a skip).
     */
    private static String findIncompatibleParameter(ObjectNode fieldObj) {
        for (String param : PARAMS_INCOMPATIBLE_WITH_FLATTENED) {
            if (fieldObj.has(param)) {
                return param;
            }
        }
        return null;
    }
}

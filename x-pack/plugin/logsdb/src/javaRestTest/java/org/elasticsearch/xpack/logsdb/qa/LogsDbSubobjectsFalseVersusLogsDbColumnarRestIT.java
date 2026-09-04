/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.qa;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.FormatNames;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Variant of {@link StaticLogsDbSubobjectsFalseVersusLogsDbColumnarRestIT} that uses
 * randomly generated mappings and documents from {@link DataGenerationHelper} instead of
 * hand-coded ones, to exercise a wider set of field types and configurations.
 *
 * <p>Object depth and nested field limit are both set to zero so the generated mapping is
 * always flat (no sub-objects of any kind). This is required because the baseline uses
 * {@code subobjects: false} to match the flat field structure imposed by
 * {@code index.mode=logsdb_columnar} on the contender side.
 *
 * <p>Mapping parameters unsupported or behaviorally different in {@code logsdb_columnar}
 * are stripped from both sides before registration so that the challenge test compares
 * the common supported subset:
 * <ul>
 *   <li>{@code store} – not allowed in columnar mode</li>
 *   <li>{@code synthetic_source_keep} – not allowed in columnar mode</li>
 *   <li>{@code subobjects} – not allowed in columnar mode</li>
 *   <li>{@code copy_to} – not allowed in columnar mode; stripped from both sides so
 *       the common supported subset is compared without cross-field copying.</li>
 *   <li>{@code dynamic: runtime} – not supported in strict columnar mode</li>
 *   <li>{@code dynamic: false} – logsdb stores ignored field values in {@code _ignored_source}
 *       (they appear in the reconstructed source), but logsdb_columnar drops them entirely
 *       ({@code sourceKeepMode=NONE}). Stripped from both sides so both fall back to
 *       {@code dynamic:true}; unmapped template fields are then dynamically mapped via the
 *       {@code strings_as_keyword} dynamic template on both sides and their values appear
 *       consistently in the source.</li>
 *   <li>{@code index: false} – in columnar mode some field types (e.g. text) enable
 *       {@code doc_values} by default, so {@code index:false} still yields a non-NONE
 *       {@code IndexType} and {@code isSearchable()} returns {@code true}, while on the logsdb
 *       baseline the same field would be NONE and non-searchable. Stripped from both sides.</li>
 *   <li>{@code doc_values: false} on non-text types – stripped so both sides fall back to
 *       their mode default ({@code true}); numeric/geo fields then take the doc-values-skippers
 *       path in columnar mode and remain searchable and aggregatable, matching the baseline.</li>
 *   <li>{@code text} and {@code match_only_text} without explicit {@code doc_values} –
 *       {@code TextFieldMapper} and {@code MatchOnlyTextFieldMapper} both return {@code enabled=true}
 *       from {@code defaultDocValuesParameters()} when {@code isStrictColumnar()}, making these
 *       fields aggregatable on the contender but not on the baseline. Normalised to
 *       {@code doc_values:true} on both sides (the columnar default) when the mapping carries no
 *       explicit value.</li>
 * </ul>
 *
 * <p>Fully-dynamic mapping (where no fields are in the static mapping) is disabled. With
 * fully-dynamic mapping, dynamically generated fields would be mapped differently on each side
 * for non-string types (e.g. an {@code ip}-typed template field generates IP-format string values
 * that would match {@code strings_as_keyword} and become {@code keyword} instead of {@code ip},
 * causing query divergence). Fully-dynamic mapping is therefore disabled so most fields land in
 * the static mapping with the correct type and columnar-safe settings.
 *
 * <p>Even with fully-dynamic mapping disabled, {@code MappingGenerator} still leaves roughly half
 * of all leaf fields out of the static mapping (via its 50 % {@code DynamicMappingGenerator}
 * probability). When those fields' values are indexed, ES creates dynamic field mappings. The
 * contender template (priority 101) overrides the built-in {@code logs} template (priority 100),
 * so without explicit intervention the built-in {@code strings_as_keyword} dynamic template is
 * not inherited and dynamic string fields become {@code text}. To prevent this, the contender
 * mapping explicitly includes its own {@code strings_as_keyword} dynamic template so that dynamic
 * string fields are mapped as {@code keyword} on both sides (matching the built-in logs template).
 *
 * <p>{@code geo_shape} and {@code shape} fields are excluded entirely from both the mapping
 * and documents. The reason is that their wire format is a JSON object (e.g. GeoJSON
 * {@code {"type":"Point","coordinates":[1,2]}}). When a field is statically mapped as
 * {@code geo_shape}, the document parser delegates the token stream to the
 * {@code GeoShapeFieldMapper}, which consumes the JSON object as a single composite value —
 * {@code subobjects: false} has no effect in that case. However, when the field is
 * <em>not</em> present in the static mapping, the dynamic-mapping path is taken instead.
 * With {@code subobjects: false}, the document parser flattens any JSON object it encounters
 * into dot-notation child paths (e.g. {@code field.type}, {@code field.coordinates}) rather
 * than routing the whole object to a field mapper. With {@code dynamic: strict} this results
 * in a {@code strict_dynamic_mapping_exception}. With {@code dynamic: true} the index would
 * accept the document, but the sub-keys would be mapped as ordinary leaf fields (keyword,
 * float, etc.) instead of a {@code geo_shape}, causing baseline and contender mappings to
 * diverge. Excluding these field types from both the mapping and documents avoids both
 * failure modes.
 */
public class LogsDbSubobjectsFalseVersusLogsDbColumnarRestIT extends BulkChallengeRestIT {

    private static final Set<String> STRIPPED_PARAMS = Set.of("store", "synthetic_source_keep", "subobjects", "copy_to");
    private static final Set<String> SHAPE_TYPES = Set.of("geo_shape", "shape");

    private Set<String> shapeFieldPaths;

    public LogsDbSubobjectsFalseVersusLogsDbColumnarRestIT() {
        super(
            new DataGenerationHelper(
                b -> b.withMaxObjectDepth(0).withNestedFieldsLimit(0).withMaxFieldCountPerLevel(30).withFullyDynamicMapping(false)
            )
        );
    }

    @Override
    public void baselineSettings(Settings.Builder builder) {
        builder.put("index.mode", "logsdb");
        // logsdb_columnar defaults disable_sequence_numbers to true; align the baseline.
        builder.put("index.disable_sequence_numbers", true);
        // Do NOT delegate to dataGenerationHelper.logsDbSettings() here. That method randomly adds
        // synthetic_source_keep=arrays when keepArraySource=true, which stores raw array values for
        // unmapped fields (dynamic:false) via _ignored_source. In logsdb_columnar sourceKeepMode=NONE
        // so those values are dropped, causing source-match divergence in testMatchAllQuery.
    }

    @Override
    public void contenderSettings(Settings.Builder builder) {
        builder.put("index.mode", "logsdb_columnar");
    }

    @Override
    @SuppressWarnings("unchecked")
    public void baselineMappings(XContentBuilder builder) throws IOException {
        // mapping.raw() wraps the actual mapping parameters under a "_doc" type key; extract the
        // inner map so we can add top-level parameters alongside "properties" in typeless form.
        var innerMapping = new LinkedHashMap<>(
            stripShapeFields(strip((Map<String, Object>) dataGenerationHelper.mapping().raw().get("_doc")))
        );
        // logsdb_columnar does not support subobjects; mirror with subobjects:false on the baseline
        innerMapping.put("subobjects", false);
        // In columnar mode _id and _routing are stored as doc values by default
        innerMapping.put("_id", Map.of("mode", "columnar"));
        innerMapping.put("_routing", Map.of("doc_values", true));
        // The contender data stream matches the built-in "logs" index template (logs-*-*),
        // which maps dynamic string fields as keyword. Align the baseline.
        innerMapping.put(
            "dynamic_templates",
            List.of(Map.of("strings_as_keyword", Map.of("match_mapping_type", "string", "mapping", Map.of("type", "keyword"))))
        );
        builder.map(innerMapping);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void contenderMappings(XContentBuilder builder) throws IOException {
        var raw = new LinkedHashMap<>(stripShapeFields(strip(dataGenerationHelper.mapping().raw())));
        // The contender template (priority 101) overrides the built-in logs template (priority 100),
        // so the built-in strings_as_keyword dynamic template is not inherited. Without it, dynamically
        // mapped string fields become text in logsdb_columnar. Add strings_as_keyword here to match
        // the baseline's dynamic mapping behaviour (same as the built-in logs template).
        if (raw.get("_doc") instanceof Map<?, ?> docMap) {
            var inner = new LinkedHashMap<>((Map<String, Object>) docMap);
            inner.put(
                "dynamic_templates",
                List.of(Map.of("strings_as_keyword", Map.of("match_mapping_type", "string", "mapping", Map.of("type", "keyword"))))
            );
            raw.put("_doc", inner);
        }
        builder.map(raw);
    }

    @Override
    protected XContentBuilder generateDocument(final Instant timestamp) throws IOException {
        var document = XContentFactory.jsonBuilder();
        dataGenerationHelper.generateDocument(
            document,
            Map.of("@timestamp", DateFormatter.forPattern(FormatNames.STRICT_DATE_OPTIONAL_TIME.getName()).format(timestamp))
        );
        Set<String> shapes = shapeFieldPaths();
        if (shapes.isEmpty()) {
            return document;
        }
        // geo_shape/shape JSON-object values are incompatible with subobjects:false;
        // remove them from the document so indexing does not fail.
        var docMap = new LinkedHashMap<>(XContentHelper.convertToMap(XContentType.JSON.xContent(), Strings.toString(document), true));
        shapes.forEach(docMap::remove);
        return XContentFactory.jsonBuilder().map(docMap);
    }

    @Override
    protected boolean autoGenerateId() {
        return false;
    }

    /**
     * Recursively removes mapping parameters that are either unsupported or behaviorally
     * different in {@code logsdb_columnar} from both sides of the challenge test so that
     * only the common supported subset is compared.
     */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> strip(Map<String, Object> map) {
        var result = new LinkedHashMap<String, Object>(map.size());
        for (var entry : map.entrySet()) {
            var key = entry.getKey();
            var value = entry.getValue();
            if (STRIPPED_PARAMS.contains(key)) {
                continue;
            }
            // dynamic:runtime is not supported in strict columnar mode
            if ("dynamic".equals(key) && "runtime".equals(value)) {
                continue;
            }
            // dynamic:false stores ignored field values in _ignored_source in logsdb (they appear in
            // the reconstructed source), but logsdb_columnar drops them (sourceKeepMode=NONE). Strip
            // dynamic:false so both sides fall back to dynamic:true; unmapped template fields are then
            // dynamically mapped via the strings_as_keyword dynamic template on both sides and their
            // values appear consistently in the source.
            if ("dynamic".equals(key) && "false".equals(value)) {
                continue;
            }
            // In logsdb_columnar some field types (e.g. text) enable doc_values by default because
            // columnar storage is built on doc values. A field with index:false but doc_values:true
            // gets IndexType=terms(false,true) which is not NONE, so isSearchable() returns true.
            // On the logsdb baseline those same types default doc_values to false, so index:false
            // yields NONE → searchable:false, causing field-caps divergence. Strip index:false from
            // both sides so the comparison reflects the common supported subset.
            if ("index".equals(key) && (Boolean.FALSE.equals(value) || "false".equals(value))) {
                continue;
            }
            if (value instanceof Map<?, ?> nested) {
                result.put(key, strip((Map<String, Object>) nested));
            } else {
                result.put(key, value);
            }
        }
        // TextFieldMapper and MatchOnlyTextFieldMapper both call defaultDocValuesParameters() which
        // returns enabled=true when isStrictColumnar(), so text-family fields are aggregatable=true
        // by default in logsdb_columnar but aggregatable=false in logsdb. Inject doc_values:true on
        // both sides (the columnar default) when the mapping carries no explicit value.
        var fieldType = result.get("type");
        boolean isTextFamily = "text".equals(fieldType) || "match_only_text".equals(fieldType);
        if (isTextFamily) {
            if (result.containsKey("doc_values") == false) {
                result.put("doc_values", true);
            }
        } else if (Boolean.FALSE.equals(result.get("doc_values")) || "false".equals(result.get("doc_values"))) {
            // For non-text types, strip doc_values:false. In columnar mode numeric/geo/etc.
            // fields take the doc_values-skippers path, so they remain searchable and aggregatable
            // like the baseline. Stripping lets both sides fall back to mode default (true).
            result.remove("doc_values");
        }
        return result;
    }

    /**
     * Removes geo_shape and shape type fields from the {@code properties} map.
     * These types produce JSON-object document values that are incompatible with
     * {@code subobjects: false} and cause indexing failures under strict dynamic mapping.
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> stripShapeFields(Map<String, Object> mappingMap) {
        if (shapeFieldPaths().isEmpty()) {
            return mappingMap;
        }
        var result = new LinkedHashMap<>(mappingMap);
        // Strip from _doc wrapper if present, otherwise from the map directly
        if (result.get("_doc") instanceof Map<?, ?> doc) {
            var inner = new LinkedHashMap<>((Map<String, Object>) doc);
            stripShapeFieldsFromProperties(inner);
            result.put("_doc", inner);
        } else {
            stripShapeFieldsFromProperties(result);
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private void stripShapeFieldsFromProperties(Map<String, Object> mappingMap) {
        if (mappingMap.get("properties") instanceof Map<?, ?> props) {
            var filteredProps = new LinkedHashMap<>((Map<String, Object>) props);
            shapeFieldPaths().forEach(filteredProps::remove);
            mappingMap.put("properties", filteredProps);
        }
    }

    private Set<String> shapeFieldPaths() {
        if (shapeFieldPaths == null) {
            shapeFieldPaths = dataGenerationHelper.getTemplateFieldTypes()
                .entrySet()
                .stream()
                .filter(e -> SHAPE_TYPES.contains(e.getValue()))
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
        }
        return shapeFieldPaths;
    }
}

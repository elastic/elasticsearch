/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesIndexResponse;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.fieldcaps.IndexFieldCapabilities;
import org.elasticsearch.action.fieldcaps.IndexFieldCapabilitiesBuilder;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.InvalidMappedField;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.session.IndexResolver;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.TestAnalyzer.loadMapping;

public final class AnalyzerTestUtils {

    private AnalyzerTestUtils() {}

    public static UnresolvedRelation unresolvedRelation(String index) {
        return new UnresolvedRelation(
            Source.EMPTY,
            new IndexPattern(Source.EMPTY, index),
            false,
            List.of(),
            IndexMode.STANDARD,
            null,
            "FROM"
        );
    }

    public static Map<IndexPattern, IndexResolution> indexResolutions(EsIndex... indexes) {
        Map<IndexPattern, IndexResolution> map = new HashMap<>();
        for (EsIndex index : indexes) {
            map.put(new IndexPattern(Source.EMPTY, index.name()), IndexResolution.valid(index));
        }
        return map;
    }

    public static Map<IndexPattern, IndexResolution> indexResolutions(IndexResolution... indexes) {
        Map<IndexPattern, IndexResolution> map = new HashMap<>();
        for (IndexResolution index : indexes) {
            map.put(new IndexPattern(Source.EMPTY, index.get().name()), index);
        }
        return map;
    }

    public static Map<String, IndexResolution> defaultLookupResolution() {
        return Map.of(
            "languages_lookup",
            loadMapping("mapping-languages.json", "languages_lookup", IndexMode.LOOKUP),
            "test_lookup",
            loadMapping("mapping-basic.json", "test_lookup", IndexMode.LOOKUP),
            "spatial_lookup",
            loadMapping("mapping-multivalue_geometries.json", "spatial_lookup", IndexMode.LOOKUP)
        );
    }

    public static final String RERANKING_INFERENCE_ID = "reranking-inference-id";
    public static final String COMPLETION_INFERENCE_ID = "completion-inference-id";
    public static final String TEXT_EMBEDDING_INFERENCE_ID = "text-embedding-inference-id";
    public static final String CHAT_COMPLETION_INFERENCE_ID = "chat-completion-inference-id";
    public static final String SPARSE_EMBEDDING_INFERENCE_ID = "sparse-embedding-inference-id";
    public static final List<String> VALID_INFERENCE_IDS = List.of(
        RERANKING_INFERENCE_ID,
        COMPLETION_INFERENCE_ID,
        TEXT_EMBEDDING_INFERENCE_ID,
        CHAT_COMPLETION_INFERENCE_ID,
        SPARSE_EMBEDDING_INFERENCE_ID
    );

    public static String randomInferenceId() {
        return ESTestCase.randomFrom(VALID_INFERENCE_IDS);
    }

    public static String randomInferenceIdOtherThan(String... excludes) {
        return ESTestCase.randomValueOtherThanMany(Arrays.asList(excludes)::contains, AnalyzerTestUtils::randomInferenceId);
    }

    public static IndexResolution indexWithDateDateNanosUnionType() {
        // this method is shared by AnalyzerTest, QueryTranslatorTests and LocalPhysicalPlanOptimizerTests
        String dateDateNanos = "date_and_date_nanos"; // mixed date and date_nanos
        String dateDateNanosLong = "date_and_date_nanos_and_long"; // mixed date, date_nanos and long
        LinkedHashMap<String, Set<String>> typesToIndices1 = new LinkedHashMap<>();
        typesToIndices1.put("date", Set.of("index1", "index2"));
        typesToIndices1.put("date_nanos", Set.of("index3"));
        LinkedHashMap<String, Set<String>> typesToIndices2 = new LinkedHashMap<>();
        typesToIndices2.put("date", Set.of("index1"));
        typesToIndices2.put("date_nanos", Set.of("index2"));
        typesToIndices2.put("long", Set.of("index3"));
        EsField dateDateNanosField = new InvalidMappedField(dateDateNanos, typesToIndices1);
        EsField dateDateNanosLongField = new InvalidMappedField(dateDateNanosLong, typesToIndices2);
        EsIndex index = new EsIndex(
            "index*",
            Map.of(dateDateNanos, dateDateNanosField, dateDateNanosLong, dateDateNanosLongField),
            Map.of("index1", IndexMode.STANDARD, "index2", IndexMode.STANDARD, "index3", IndexMode.STANDARD),
            Map.of(),
            Map.of(),
            Map.of()
        );
        return IndexResolution.valid(index);
    }

    public static FieldCapabilitiesIndexResponse fieldCapabilitiesIndexResponse(
        String indexName,
        Map<String, IndexFieldCapabilities> fields
    ) {
        String indexMappingHash = new String(
            MessageDigests.sha256().digest(fields.toString().getBytes(StandardCharsets.UTF_8)),
            StandardCharsets.UTF_8
        );
        return new FieldCapabilitiesIndexResponse(indexName, indexMappingHash, fields, false, IndexMode.STANDARD);
    }

    public static Map<String, IndexFieldCapabilities> fieldResponseMap(String fieldName, String type) {
        return Map.of(fieldName, new IndexFieldCapabilitiesBuilder(fieldName, type).build());
    }

    public static Map<String, IndexFieldCapabilities> fieldResponseMap(Map<String, String> fieldTypes) {
        Map<String, IndexFieldCapabilities> result = new HashMap<>();
        for (Map.Entry<String, String> entry : fieldTypes.entrySet()) {
            result.putAll(fieldResponseMap(entry.getKey(), entry.getValue()));
        }
        return result;
    }

    public static IndexResolver.FieldsInfo fieldsInfoOnCurrentVersion(FieldCapabilitiesResponse caps) {
        return new IndexResolver.FieldsInfo(caps, TransportVersion.current(), false, false, false, false);
    }

    public static IndexResolution mergedResolution(String indexPattern, FieldCapabilitiesResponse caps) {
        return IndexResolver.mergedMappings(indexPattern, false, fieldsInfoOnCurrentVersion(caps), IndexResolver.DO_NOT_GROUP);
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.dataframe;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsSource;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;

/**
 * Merges mappings in a best effort and naive manner.
 * The merge will fail if there is any conflict, i.e. the mappings of a field are not exactly the same.
 */
public final class MappingsMerger {

    private MappingsMerger() {}

    public static void mergeMappings(
        Client client,
        Map<String, String> headers,
        DataFrameAnalyticsSource source,
        ActionListener<ImmutableOpenMap<String, MappingMetadata>> listener
    ) {
        ActionListener<GetMappingsResponse> mappingsListener = ActionListener.wrap(
            getMappingsResponse -> listener.onResponse(MappingsMerger.mergeMappings(source, getMappingsResponse)),
            listener::onFailure
        );

        GetMappingsRequest getMappingsRequest = new GetMappingsRequest();
        getMappingsRequest.indices(source.getIndex());
        ClientHelper.executeWithHeadersAsync(headers, ML_ORIGIN, client, GetMappingsAction.INSTANCE, getMappingsRequest, mappingsListener);
    }

    static ImmutableOpenMap<String, MappingMetadata> mergeMappings(
        DataFrameAnalyticsSource source,
        GetMappingsResponse getMappingsResponse
    ) {
        Map<String, Object> mappings = new HashMap<>();
        mappings.put("dynamic", false);

        ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetadata>> indexToMappings = getMappingsResponse.getMappings();
        for (MappingsType mappingsType : MappingsType.values()) {
            Map<String, IndexAndMapping> mergedMappingsForType = mergeAcrossIndices(source, indexToMappings, mappingsType);
            if (mergedMappingsForType.isEmpty() == false) {
                mappings.put(
                    mappingsType.type,
                    mergedMappingsForType.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().mapping))
                );
            }
        }

        ImmutableOpenMap.Builder<String, MappingMetadata> result = ImmutableOpenMap.builder();
        try {
            result.put(MapperService.SINGLE_MAPPING_NAME, new MappingMetadata(MapperService.SINGLE_MAPPING_NAME, mappings));
        } catch (IOException e) {
            throw ExceptionsHelper.serverError("Failed to parse mappings: " + mappings);
        }
        return result.build();
    }

    private static Map<String, IndexAndMapping> mergeAcrossIndices(
        DataFrameAnalyticsSource source,
        ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetadata>> indexToMappings,
        MappingsType mappingsType
    ) {

        Map<String, IndexAndMapping> mergedMappings = new HashMap<>();

        Iterator<ObjectObjectCursor<String, ImmutableOpenMap<String, MappingMetadata>>> iterator = indexToMappings.iterator();
        while (iterator.hasNext()) {
            ObjectObjectCursor<String, ImmutableOpenMap<String, MappingMetadata>> indexMappings = iterator.next();
            Iterator<ObjectObjectCursor<String, MappingMetadata>> typeIterator = indexMappings.value.iterator();
            while (typeIterator.hasNext()) {
                ObjectObjectCursor<String, MappingMetadata> typeMapping = typeIterator.next();
                Map<String, Object> currentMappings = typeMapping.value.getSourceAsMap();
                if (currentMappings.containsKey(mappingsType.type)) {

                    @SuppressWarnings("unchecked")
                    Map<String, Object> fieldMappings = (Map<String, Object>) currentMappings.get(mappingsType.type);

                    for (Map.Entry<String, Object> fieldMapping : fieldMappings.entrySet()) {
                        String field = fieldMapping.getKey();
                        if (source.isFieldExcluded(field) == false) {
                            if (mergedMappings.containsKey(field)) {
                                IndexAndMapping existingIndexAndMapping = mergedMappings.get(field);
                                if (existingIndexAndMapping.mapping.equals(fieldMapping.getValue()) == false) {
                                    throw ExceptionsHelper.badRequestException(
                                        "cannot merge [{}] mappings because of differences for field [{}]; mapped as [{}] in index [{}]; "
                                            + "mapped as [{}] in index [{}]",
                                        mappingsType.type,
                                        field,
                                        fieldMapping.getValue(),
                                        indexMappings.key,
                                        existingIndexAndMapping.mapping,
                                        existingIndexAndMapping.index
                                    );

                                }
                            } else {
                                mergedMappings.put(field, new IndexAndMapping(indexMappings.key, fieldMapping.getValue()));
                            }
                        }
                    }
                }
            }
        }

        return mergedMappings;
    }

    private static class IndexAndMapping {
        private final String index;
        private final Object mapping;

        private IndexAndMapping(String index, Object mapping) {
            this.index = Objects.requireNonNull(index);
            this.mapping = Objects.requireNonNull(mapping);
        }
    }

    private enum MappingsType {
        PROPERTIES("properties"),
        RUNTIME("runtime");

        private String type;

        MappingsType(String type) {
            this.type = type;
        }
    }
}

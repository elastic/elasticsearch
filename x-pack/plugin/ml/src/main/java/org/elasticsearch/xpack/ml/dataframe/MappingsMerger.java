/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
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

import java.util.Collections;
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

    public static void mergeMappings(Client client, Map<String, String> headers, DataFrameAnalyticsSource source,
                                     ActionListener<MappingMetadata> listener) {
        ActionListener<GetMappingsResponse> mappingsListener = ActionListener.wrap(
            getMappingsResponse -> listener.onResponse(MappingsMerger.mergeMappings(source, getMappingsResponse)),
            listener::onFailure
        );

        GetMappingsRequest getMappingsRequest = new GetMappingsRequest();
        getMappingsRequest.indices(source.getIndex());
        ClientHelper.executeWithHeadersAsync(headers, ML_ORIGIN, client, GetMappingsAction.INSTANCE, getMappingsRequest, mappingsListener);
    }

    static MappingMetadata mergeMappings(DataFrameAnalyticsSource source,
                                                                   GetMappingsResponse getMappingsResponse) {
        ImmutableOpenMap<String, MappingMetadata> indexToMappings = getMappingsResponse.getMappings();

        Map<String, IndexAndMapping> mergedMappings = new HashMap<>();

        Iterator<ObjectObjectCursor<String, MappingMetadata>> iterator = indexToMappings.iterator();
        while (iterator.hasNext()) {
            ObjectObjectCursor<String, MappingMetadata> indexMappings = iterator.next();
            MappingMetadata mapping = indexMappings.value;
            if (mapping != null) {
                Map<String, Object> currentMappings = mapping.getSourceAsMap();
                if (currentMappings.containsKey("properties")) {

                    @SuppressWarnings("unchecked")
                    Map<String, Object> fieldMappings = (Map<String, Object>) currentMappings.get("properties");

                    for (Map.Entry<String, Object> fieldMapping : fieldMappings.entrySet()) {
                        String field = fieldMapping.getKey();
                        if (source.isFieldExcluded(field) == false) {
                            if (mergedMappings.containsKey(field)) {
                                IndexAndMapping existingIndexAndMapping = mergedMappings.get(field);
                                if (existingIndexAndMapping.mapping.equals(fieldMapping.getValue()) == false) {
                                    throw ExceptionsHelper.badRequestException(
                                        "cannot merge mappings because of differences for field [{}]; mapped as [{}] in index [{}]; " +
                                            "mapped as [{}] in index [{}]", field, fieldMapping.getValue(), indexMappings.key,
                                            existingIndexAndMapping.mapping, existingIndexAndMapping.index);

                                }
                            } else {
                                mergedMappings.put(field, new IndexAndMapping(indexMappings.key, fieldMapping.getValue()));
                            }
                        }
                    }
                }
            }
        }

        return createMappingMetadata(MapperService.SINGLE_MAPPING_NAME,
            mergedMappings.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().mapping)));
    }

    private static MappingMetadata createMappingMetadata(String type, Map<String, Object> mappings) {
        return new MappingMetadata(type, Collections.singletonMap("properties", mappings));
    }

    private static class IndexAndMapping {
        private final String index;
        private final Object mapping;

        private IndexAndMapping(String index, Object mapping) {
            this.index = Objects.requireNonNull(index);
            this.mapping = Objects.requireNonNull(mapping);
        }
    }
}

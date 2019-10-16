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
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;

/**
 * Merges mappings in a best effort and naive manner.
 * The merge will fail if there is any conflict, i.e. the mappings of a field are not exactly the same.
 */
public final class MappingsMerger {

    private MappingsMerger() {}

    public static void mergeMappings(Client client, Map<String, String> headers, String[] index,
                                     ActionListener<ImmutableOpenMap<String, MappingMetaData>> listener) {
        ActionListener<GetMappingsResponse> mappingsListener = ActionListener.wrap(
            getMappingsResponse -> listener.onResponse(MappingsMerger.mergeMappings(getMappingsResponse)),
            listener::onFailure
        );

        GetMappingsRequest getMappingsRequest = new GetMappingsRequest();
        getMappingsRequest.indices(index);
        ClientHelper.executeWithHeadersAsync(headers, ML_ORIGIN, client, GetMappingsAction.INSTANCE, getMappingsRequest, mappingsListener);
    }

    static ImmutableOpenMap<String, MappingMetaData> mergeMappings(GetMappingsResponse getMappingsResponse) {
        ImmutableOpenMap<String, MappingMetaData> indexToMappings = getMappingsResponse.getMappings();

        String type = null;
        Map<String, Object> mergedMappings = new HashMap<>();

        Iterator<ObjectObjectCursor<String, MappingMetaData>> iterator = indexToMappings.iterator();
        while (iterator.hasNext()) {
            MappingMetaData mapping = iterator.next().value;
            if (mapping != null) {
                Map<String, Object> currentMappings = mapping.getSourceAsMap();
                if (currentMappings.containsKey("properties")) {

                    @SuppressWarnings("unchecked")
                    Map<String, Object> fieldMappings = (Map<String, Object>) currentMappings.get("properties");

                    for (Map.Entry<String, Object> fieldMapping : fieldMappings.entrySet()) {
                        if (mergedMappings.containsKey(fieldMapping.getKey())) {
                            if (mergedMappings.get(fieldMapping.getKey()).equals(fieldMapping.getValue()) == false) {
                                throw ExceptionsHelper.badRequestException("cannot merge mappings because of differences for field [{}]",
                                    fieldMapping.getKey());
                            }
                        } else {
                            mergedMappings.put(fieldMapping.getKey(), fieldMapping.getValue());
                        }
                    }
                }
            }
        }

        MappingMetaData mappingMetaData = createMappingMetaData(MapperService.SINGLE_MAPPING_NAME, mergedMappings);
        ImmutableOpenMap.Builder<String, MappingMetaData> result = ImmutableOpenMap.builder();
        result.put(MapperService.SINGLE_MAPPING_NAME, mappingMetaData);
        return result.build();
    }

    private static MappingMetaData createMappingMetaData(String type, Map<String, Object> mappings) {
        return new MappingMetaData(type, Collections.singletonMap("properties", mappings));
    }
}

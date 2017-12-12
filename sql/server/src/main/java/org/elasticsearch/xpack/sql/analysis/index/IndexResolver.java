/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.analysis.index;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest.Feature;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.Types;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class IndexResolver {

    private final Client client;

    public IndexResolver(Client client) {
        this.client = client;
    }

    /**
     * Resolves a single index by name.
     */
    public void asIndex(final String index, ActionListener<GetIndexResult> listener) {
        GetIndexRequest getIndexRequest = createGetIndexRequest(index);
        client.admin().indices().getIndex(getIndexRequest, ActionListener.wrap(getIndexResponse -> {
            GetIndexResult result;
            if (getIndexResponse.getMappings().size() > 1) {
                result = GetIndexResult.invalid(
                        "[" + index + "] is an alias pointing to more than one index which is currently incompatible with sql");
            } else if (getIndexResponse.getMappings().size() == 1){
                ObjectObjectCursor<String, ImmutableOpenMap<String, MappingMetaData>> indexMappings =
                        getIndexResponse.getMappings().iterator().next();
                String concreteIndex = indexMappings.key;
                /*
                 * here we don't support wildcards: we can either have an alias or an index. However names get resolved (through
                 * security or not) we need to preserve the original names as they will be used in the subsequent search request.
                 * With security enabled, if the user is authorized for an alias and not its corresponding concrete index, we have to
                 * make sure that the search is executed against the same alias name from the original command, rather than
                 * the resolved concrete index that we get back from the get index API
                 */
                result = buildGetIndexResult(concreteIndex, index, indexMappings.value);
            } else {
                result = GetIndexResult.notFound(index);
            }
            listener.onResponse(result);
        }, listener::onFailure));
    }

    /**
     *  Discover (multiple) matching indices for a given name.
     */
    public void asList(String index, ActionListener<List<EsIndex>> listener) {
        GetIndexRequest getIndexRequest = createGetIndexRequest(index);
        client.admin().indices().getIndex(getIndexRequest, ActionListener.wrap(getIndexResponse -> {
            ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings = getIndexResponse.getMappings();
            List<EsIndex> results = new ArrayList<>(mappings.size());
            for (ObjectObjectCursor<String, ImmutableOpenMap<String, MappingMetaData>> indexMappings : mappings) {
                /*
                 * We support wildcard expressions here, and it's only for commands that only perform the get index call.
                 * We can and simply have to use the concrete index name and show that to users.
                 * Get index against an alias with security enabled, where the user has only access to get mappings for the alias
                 * and not the concrete index: there is a well known information leak of the concrete index name in the response.
                 */
                String concreteIndex = indexMappings.key;
                GetIndexResult getIndexResult = buildGetIndexResult(concreteIndex, concreteIndex, indexMappings.value);
                if (getIndexResult.isValid()) {
                    results.add(getIndexResult.get());
                }
            }
            results.sort(Comparator.comparing(EsIndex::name));
            listener.onResponse(results);
        }, listener::onFailure));
    }

    private static GetIndexRequest createGetIndexRequest(String index) {
        return new GetIndexRequest()
                .local(true)
                .indices(index)
                .features(Feature.MAPPINGS)
                //lenient because we throw our own errors looking at the response e.g. if something was not resolved
                //also because this way security doesn't throw authorization exceptions but rather honours ignore_unavailable
                .indicesOptions(IndicesOptions.lenientExpandOpen());
    }

    private static GetIndexResult buildGetIndexResult(String concreteIndex, String indexOrAlias,
                                                      ImmutableOpenMap<String, MappingMetaData> mappings) {
        if (concreteIndex.startsWith(".")) {
            //Indices that start with "." are considered internal and should not be available to SQL
            return GetIndexResult.notFound(indexOrAlias);
        }

        // Make sure that the index contains only a single type
        MappingMetaData singleType = null;
        List<String> typeNames = null;
        for (ObjectObjectCursor<String, MappingMetaData> type : mappings) {
            //Default mappings are ignored as they are applied to each type. Each type alone holds all of its fields.
            if ("_default_".equals(type.key)) {
                continue;
            }
            if (singleType != null) {
                // There are more than one types
                if (typeNames == null) {
                    typeNames = new ArrayList<>();
                    typeNames.add(singleType.type());
                }
                typeNames.add(type.key);
            }
            singleType = type.value;
        }

        if (singleType == null) {
            return GetIndexResult.invalid("[" + indexOrAlias + "] doesn't have any types so it is incompatible with sql");
        } else if (typeNames != null) {
            Collections.sort(typeNames);
            return GetIndexResult.invalid(
                    "[" + indexOrAlias + "] contains more than one type " + typeNames + " so it is incompatible with sql");
        } else {
            Map<String, DataType> mapping = Types.fromEs(singleType.sourceAsMap());
            return GetIndexResult.valid(new EsIndex(indexOrAlias, mapping));
        }
    }
}
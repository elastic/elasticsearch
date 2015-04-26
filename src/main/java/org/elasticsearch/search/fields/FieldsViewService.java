/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.fields;

import com.google.common.collect.Sets;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationRequest;
import org.elasticsearch.action.support.single.shard.SingleShardOperationRequest;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.aliases.IndexAliasesService;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.search.internal.ShardSearchRequest;

import java.util.HashSet;
import java.util.Set;

// TODO: not sure about the name of this class
public class FieldsViewService {

    private final IndicesService indicesServices;
    private final ClusterService clusterService;

    @Inject
    public FieldsViewService(IndicesService indicesServices, ClusterService clusterService) {
        this.indicesServices = indicesServices;
        this.clusterService = clusterService;
    }

    public void prepareView(BroadcastShardOperationRequest request) {
        prepareView(request.shardId().getIndex(), request.indices());
    }

    public void prepareView(SingleShardOperationRequest request, String concreteIndex) {
        prepareView(concreteIndex, request.indices());
    }

    public void prepareView(ShardSearchRequest request) {
        prepareView(request.index(), request.filteringAliases());
    }

    public void prepareView(String concreteIndex, String... indicesOrAliases) {
        ClusterState state = clusterService.state();
        String[] filteringAliases = state.getMetaData().filteringAliases(concreteIndex, indicesOrAliases);
        IndexService indexService = indicesServices.indexServiceSafe(concreteIndex);
        IndexAliasesService aliasService = indexService.aliasesService();
        Set<FieldMapper<?>> fields = aliasService.aliasFields(filteringAliases);
        if (fields.isEmpty()) {
            return;
        }

        // We need to include the meta fields, otherwise features will stop working.
        Set<String> indexedFieldNames = Sets.newHashSet(MapperService.getMetaFields());
        // We need to exclude the _all field here, because that includes all a copy of all values from all fields.
        // which would break the filtering by field
        indexedFieldNames.remove("_all");

        Set<String> fullFieldNames = new HashSet<>();
        for (FieldMapper field : fields) {
            indexedFieldNames.add(field.names().indexName());
            fullFieldNames.add(field.names().fullName());
        }

        FieldsViewContext.createAndSet(indexedFieldNames, fullFieldNames);
    }

    public void clearView() {
        FieldsViewContext.clear();
    }

}

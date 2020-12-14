/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.query;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.shard.IndexLongFieldRange;

import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

public class CoordinatorRewriteContextProvider {
    private final NamedXContentRegistry xContentRegistry;
    private final NamedWriteableRegistry writeableRegistry;
    private final Client client;
    private final LongSupplier nowInMillis;
    private final Supplier<ClusterState> clusterStateSupplier;
    private final Function<Index, DateFieldMapper.DateFieldType> mappingSupplier;

    public CoordinatorRewriteContextProvider(NamedXContentRegistry xContentRegistry,
                                             NamedWriteableRegistry writeableRegistry,
                                             Client client,
                                             LongSupplier nowInMillis,
                                             Supplier<ClusterState> clusterStateSupplier,
                                             Function<Index, DateFieldMapper.DateFieldType> mappingSupplier) {
        this.xContentRegistry = xContentRegistry;
        this.writeableRegistry = writeableRegistry;
        this.client = client;
        this.nowInMillis = nowInMillis;
        this.clusterStateSupplier = clusterStateSupplier;
        this.mappingSupplier = mappingSupplier;
    }

    @Nullable
    public CoordinatorRewriteContext getCoordinatorRewriteContext(Index index) {
        ClusterState clusterState = clusterStateSupplier.get();
        IndexMetadata indexMetadata = clusterState.metadata().index(index);

        if (indexMetadata == null || indexMetadata.getTimestampMillisRange().containsAllShardRanges() == false) {
            return null;
        }

        DateFieldMapper.DateFieldType dateFieldType = mappingSupplier.apply(index);

        if (dateFieldType == null) {
            return null;
        }

        IndexLongFieldRange timestampMillisRange = indexMetadata.getTimestampMillisRange();
        return new CoordinatorRewriteContext(xContentRegistry,
            writeableRegistry,
            client,
            nowInMillis,
            index,
            timestampMillisRange,
            dateFieldType
        );
    }
}

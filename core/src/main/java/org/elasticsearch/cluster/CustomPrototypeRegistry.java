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
package org.elasticsearch.cluster;

import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class CustomPrototypeRegistry {

    public static final CustomPrototypeRegistry EMPTY = new CustomPrototypeRegistry(
            Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap()
    );

    private final Map<String, ClusterState.Custom> clusterStatePrototypes;
    private final Map<String, MetaData.Custom> metaDataPrototypes;
    private final Map<String, IndexMetaData.Custom> indexMetaDataPrototypes;

    public CustomPrototypeRegistry(Map<String, ClusterState.Custom> clusterStatePrototypes,
                                   Map<String, MetaData.Custom> metaDataPrototypes,
                                   Map<String, IndexMetaData.Custom> indexMetaDataPrototypes) {
        this.clusterStatePrototypes = Collections.unmodifiableMap(clusterStatePrototypes);
        this.metaDataPrototypes = Collections.unmodifiableMap(metaDataPrototypes);
        this.indexMetaDataPrototypes = Collections.unmodifiableMap(indexMetaDataPrototypes);
    }

    @Nullable
    public <T extends ClusterState.Custom> T getClusterStatePrototype(String type) {
        //noinspection unchecked
        return (T) clusterStatePrototypes.get(type);
    }

    public <T extends ClusterState.Custom> T getClusterStatePrototypeSafe(String type) {
        T proto = getClusterStatePrototype(type);
        if (proto == null) {
            throw new IllegalArgumentException(
                "No custom state prototype registered for type [" + type + "], node likely missing plugins");
        }
        return proto;
    }

    @Nullable
    public MetaData.Custom getMetadataPrototype(String type) {
        return metaDataPrototypes.get(type);
    }

    public MetaData.Custom getMetadataPrototypeSafe(String type) {
        MetaData.Custom custom = getMetadataPrototype(type);
        if (custom == null) {
            throw new IllegalArgumentException(
                "No custom metadata prototype registered for type [" + type + "], node likely missing plugins");
        }
        return custom;
    }

    @Nullable
    public <T extends IndexMetaData.Custom> T getIndexMetadataPrototype(String type) {
        //noinspection unchecked
        return (T) indexMetaDataPrototypes.get(type);
    }

    public <T extends IndexMetaData.Custom> T getIndexMetadataPrototypeSafe(String type) {
        T proto = getIndexMetadataPrototype(type);
        if (proto == null) {
            throw new IllegalArgumentException("No custom metadata prototype registered for type [" + type + "]");
        }
        return proto;
    }

    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        List<NamedWriteableRegistry.Entry> namedWriteables = new ArrayList<>();
        clusterStatePrototypes.values().stream()
                .map(custom -> new NamedWriteableRegistry.Entry(ClusterState.Custom.class, custom.type(), custom::readFrom))
                .collect(Collectors.toCollection(() -> namedWriteables));
        metaDataPrototypes.values().stream()
                .map(custom -> new NamedWriteableRegistry.Entry(MetaData.Custom.class, custom.type(), custom::readFrom))
                .collect(Collectors.toCollection(() -> namedWriteables));
        indexMetaDataPrototypes.values().stream()
                .map(custom -> new NamedWriteableRegistry.Entry(IndexMetaData.Custom.class, custom.type(), custom::readFrom))
                .collect(Collectors.toCollection(() -> namedWriteables));
        return namedWriteables;
    }

    public Set<String> getCustomIndexMetadataNames() {
        return indexMetaDataPrototypes.keySet();
    }

}

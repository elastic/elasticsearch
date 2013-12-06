/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.action.admin.indices.mapping.get;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse.FieldMappingMetaData;
import org.elasticsearch.action.support.master.info.TransportClusterInfoAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Collection;

/**
 */
public class TransportGetFieldMappingsAction extends TransportClusterInfoAction<GetFieldMappingsRequest, GetFieldMappingsResponse> {

    private final IndicesService indicesService;

    @Inject
    public TransportGetFieldMappingsAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                           IndicesService indicesService, ThreadPool threadPool) {
        super(settings, transportService, clusterService, threadPool);
        this.indicesService = indicesService;
    }

    @Override
    protected String transportAction() {
        return GetFieldMappingsAction.NAME;
    }

    @Override
    protected GetFieldMappingsRequest newRequest() {
        return new GetFieldMappingsRequest();
    }

    @Override
    protected GetFieldMappingsResponse newResponse() {
        return new GetFieldMappingsResponse();
    }

    @Override
    protected void doMasterOperation(final GetFieldMappingsRequest request, final ClusterState state, final ActionListener<GetFieldMappingsResponse> listener) throws ElasticSearchException {

        listener.onResponse(new GetFieldMappingsResponse(findMappings(request.indices(), request.types(), request.fields(), request.includeDefaults())));
    }

    private ImmutableMap<String, ImmutableMap<String, ImmutableMap<String, FieldMappingMetaData>>> findMappings(String[] concreteIndices,
                                                                                                                final String[] types,
                                                                                                                final String[] fields,
                                                                                                                boolean includeDefaults) {
        assert types != null;
        assert concreteIndices != null;
        if (concreteIndices.length == 0) {
            return ImmutableMap.of();
        }

        ImmutableMap.Builder<String, ImmutableMap<String, ImmutableMap<String, FieldMappingMetaData>>> indexMapBuilder = ImmutableMap.builder();
        Sets.SetView<String> intersection = Sets.intersection(Sets.newHashSet(concreteIndices), indicesService.indices());
        for (String index : intersection) {
            IndexService indexService = indicesService.indexService(index);
            Collection<String> typeIntersection;
            if (types.length == 0) {
                typeIntersection = indexService.mapperService().types();

            } else {
                typeIntersection = Collections2.filter(indexService.mapperService().types(), new Predicate<String>() {

                    @Override
                    public boolean apply(String type) {
                        return Regex.simpleMatch(types, type);
                    }

                });
            }

            MapBuilder<String, ImmutableMap<String, FieldMappingMetaData>> typeMappings = new MapBuilder<String, ImmutableMap<String, FieldMappingMetaData>>();
            for (String type : typeIntersection) {
                DocumentMapper documentMapper = indexService.mapperService().documentMapper(type);
                ImmutableMap<String, FieldMappingMetaData> fieldMapping = findFieldMappingsByType(documentMapper, fields, includeDefaults);
                if (!fieldMapping.isEmpty()) {
                    typeMappings.put(type, fieldMapping);
                }
            }

            if (!typeMappings.isEmpty()) {
                indexMapBuilder.put(index, typeMappings.immutableMap());
            }
        }

        return indexMapBuilder.build();
    }

    private static final ToXContent.Params includeDefaultsParams = new ToXContent.Params() {

        final static String INCLUDE_DEFAULTS = "include_defaults";

        @Override
        public String param(String key) {
            if (INCLUDE_DEFAULTS.equals(key)) {
                return "true";
            }
            return null;
        }

        @Override
        public String param(String key, String defaultValue) {
            if (INCLUDE_DEFAULTS.equals(key)) {
                return "true";
            }
            return defaultValue;
        }

        @Override
        public boolean paramAsBoolean(String key, boolean defaultValue) {
            if (INCLUDE_DEFAULTS.equals(key)) {
                return true;
            }
            return defaultValue;
        }

        @Override
        public Boolean paramAsBooleanOptional(String key, Boolean defaultValue) {
            if (INCLUDE_DEFAULTS.equals(key)) {
                return true;
            }
            return defaultValue;

        }
    };

    private ImmutableMap<String, FieldMappingMetaData> findFieldMappingsByType(DocumentMapper documentMapper, String[] fields,
                                                                               boolean includeDefaults) throws ElasticSearchException {
        MapBuilder<String, FieldMappingMetaData> fieldMappings = new MapBuilder<String, FieldMappingMetaData>();
        ImmutableList<FieldMapper> allFieldMappers = documentMapper.mappers().mappers();
        for (String field : fields) {
            if (Regex.isMatchAllPattern(field)) {
                for (FieldMapper fieldMapper : allFieldMappers) {
                    addFieldMapper(fieldMapper.names().fullName(), fieldMapper, fieldMappings, includeDefaults);
                }
            } else if (Regex.isSimpleMatchPattern(field)) {
                // go through the field mappers 3 times, to make sure we give preference to the resolve order: full name, index name, name.
                // also make sure we only store each mapper once.
                boolean[] resolved = new boolean[allFieldMappers.size()];
                for (int i = 0; i < allFieldMappers.size(); i++) {
                    FieldMapper fieldMapper = allFieldMappers.get(i);
                    if (Regex.simpleMatch(field, fieldMapper.names().fullName())) {
                        addFieldMapper(fieldMapper.names().fullName(), fieldMapper, fieldMappings, includeDefaults);
                        resolved[i] = true;
                    }
                }
                for (int i = 0; i < allFieldMappers.size(); i++) {
                    if (resolved[i]) {
                        continue;
                    }
                    FieldMapper fieldMapper = allFieldMappers.get(i);
                    if (Regex.simpleMatch(field, fieldMapper.names().indexName())) {
                        addFieldMapper(fieldMapper.names().indexName(), fieldMapper, fieldMappings, includeDefaults);
                        resolved[i] = true;
                    }
                }
                for (int i = 0; i < allFieldMappers.size(); i++) {
                    if (resolved[i]) {
                        continue;
                    }
                    FieldMapper fieldMapper = allFieldMappers.get(i);
                    if (Regex.simpleMatch(field, fieldMapper.names().name())) {
                        addFieldMapper(fieldMapper.names().name(), fieldMapper, fieldMappings, includeDefaults);
                        resolved[i] = true;
                    }
                }

            } else {
                // not a pattern
                FieldMapper fieldMapper = documentMapper.mappers().smartNameFieldMapper(field);
                if (fieldMapper != null) {
                    addFieldMapper(field, fieldMapper, fieldMappings, includeDefaults);
                }
            }
        }
        return fieldMappings.immutableMap();
    }

    private void addFieldMapper(String field, FieldMapper fieldMapper, MapBuilder<String, FieldMappingMetaData> fieldMappings, boolean includeDefaults) {
        if (fieldMappings.containsKey(field)) {
            return;
        }
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            builder.startObject();
            fieldMapper.toXContent(builder, includeDefaults ? includeDefaultsParams : ToXContent.EMPTY_PARAMS);
            builder.endObject();
            fieldMappings.put(field, new FieldMappingMetaData(fieldMapper.names().fullName(), builder.bytes()));
        } catch (IOException e) {
            throw new ElasticSearchException("failed to serialize XContent of field [" + field + "]", e);
        }
    }


}
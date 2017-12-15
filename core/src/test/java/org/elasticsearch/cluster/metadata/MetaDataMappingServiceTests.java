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
package org.elasticsearch.cluster.metadata;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingClusterStateUpdateRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;

import java.util.Collection;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class MetaDataMappingServiceTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singleton(InternalSettingsPlugin.class);
    }

    // Tests _parent meta field logic, because part of the validation is in MetaDataMappingService
    public void testAddChildTypePointingToAlreadyExistingType() throws Exception {
        createIndex("test", Settings.EMPTY, "type", "field", "type=keyword");

        // Shouldn't be able the add the _parent field pointing to an already existing type, which isn't a parent type
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> client().admin()
                .indices()
                .preparePutMapping("test")
                .setType("child")
                .setSource("_parent", "type=type")
                .get());
        assertThat(e.getMessage(),
                equalTo("can't add a _parent field that points to an already existing type, that isn't already a parent"));
    }

    // Tests _parent meta field logic, because part of the validation is in MetaDataMappingService
    public void testAddExtraChildTypePointingToAlreadyParentExistingType() throws Exception {
        IndexService indexService = createIndex("test", client().admin().indices().prepareCreate("test")
                .setSettings(Settings.builder().put("index.version.created", Version.V_5_6_0.id))
                .addMapping("parent")
                .addMapping("child1", "_parent", "type=parent")
        );

        // adding the extra child type that points to an already existing parent type is allowed:
        client().admin()
                .indices()
                .preparePutMapping("test")
                .setType("child2")
                .setSource("_parent", "type=parent")
                .get();

        DocumentMapper documentMapper = indexService.mapperService().documentMapper("child2");
        assertThat(documentMapper.parentFieldMapper().type(), equalTo("parent"));
        assertThat(documentMapper.parentFieldMapper().active(), is(true));
    }

    public void testParentIsAString() throws Exception {
        // Shouldn't be able the add the _parent field pointing to an already existing type, which isn't a parent type
        Exception e = expectThrows(MapperParsingException.class, () -> client().admin().indices().prepareCreate("test")
                .addMapping("parent", "{\"properties\":{}}", XContentType.JSON)
                .addMapping("child", "{\"_parent\": \"parent\",\"properties\":{}}", XContentType.JSON)
                .get());
        assertEquals("Failed to parse mapping [child]: [_parent] must be an object containing [type]", e.getMessage());
    }

    public void testMappingClusterStateUpdateDoesntChangeExistingIndices() throws Exception {
        final IndexService indexService = createIndex("test", client().admin().indices().prepareCreate("test").addMapping("type"));
        final CompressedXContent currentMapping = indexService.mapperService().documentMapper("type").mappingSource();

        final MetaDataMappingService mappingService = getInstanceFromNode(MetaDataMappingService.class);
        final ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        // TODO - it will be nice to get a random mapping generator
        final PutMappingClusterStateUpdateRequest request = new PutMappingClusterStateUpdateRequest().type("type");
        request.source("{ \"properties\" { \"field\": { \"type\": \"text\" }}}");
        mappingService.putMappingExecutor.execute(clusterService.state(), Collections.singletonList(request));
        assertThat(indexService.mapperService().documentMapper("type").mappingSource(), equalTo(currentMapping));
    }

    public void testClusterStateIsNotChangedWithIdenticalMappings() throws Exception {
        createIndex("test", client().admin().indices().prepareCreate("test").addMapping("type"));

        final MetaDataMappingService mappingService = getInstanceFromNode(MetaDataMappingService.class);
        final ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        final PutMappingClusterStateUpdateRequest request = new PutMappingClusterStateUpdateRequest().type("type");
        request.source("{ \"properties\" { \"field\": { \"type\": \"text\" }}}");
        ClusterState result = mappingService.putMappingExecutor.execute(clusterService.state(), Collections.singletonList(request))
            .resultingState;

        assertFalse(result != clusterService.state());

        ClusterState result2 = mappingService.putMappingExecutor.execute(result, Collections.singletonList(request))
            .resultingState;

        assertSame(result, result2);
    }
}

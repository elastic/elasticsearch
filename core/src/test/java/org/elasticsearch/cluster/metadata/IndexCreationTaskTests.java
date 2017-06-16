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

import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.Sort;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESTestCase;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Arrays;
import java.util.Set;
import java.util.Collections;
import java.util.function.Supplier;

import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class IndexCreationTaskTests extends ESTestCase {

    private final IndicesService indicesService = mock(IndicesService.class);
    private final AliasValidator aliasValidator = mock(AliasValidator.class);
    private final NamedXContentRegistry xContentRegistry = mock(NamedXContentRegistry.class);
    private final CreateIndexClusterStateUpdateRequest request = mock(CreateIndexClusterStateUpdateRequest.class);
    private final Logger logger = mock(Logger.class);
    private final AllocationService allocationService = mock(AllocationService.class);
    private final MetaDataCreateIndexService.IndexValidator validator = mock(MetaDataCreateIndexService.IndexValidator.class);
    private final ActionListener listener = mock(ActionListener.class);
    private final ClusterState state = mock(ClusterState.class);
    private final Settings settings = Settings.builder().build();
    private final MapperService mapper = mock(MapperService.class);

    final ImmutableOpenMap.Builder<String, IndexTemplateMetaData> tplBuilder = ImmutableOpenMap.builder();
    final ImmutableOpenMap.Builder<String, MetaData.Custom> customBuilder = ImmutableOpenMap.builder();
    final ImmutableOpenMap.Builder<String, IndexMetaData> idxBuilder = ImmutableOpenMap.builder();

    public void testMatchTemplates() throws Exception {
        tplBuilder.put("template_1", createTemplateMetadata("template_1", "te*"));
        tplBuilder.put("template_2", createTemplateMetadata("template_2", "tes*"));
        tplBuilder.put("template_3", createTemplateMetadata("template_3", "zzz*"));

        final ClusterState result = executeTask(state);

        assertTrue(result.metaData().index("test").getAliases().containsKey("template_1_alias"));
        assertTrue(result.metaData().index("test").getAliases().containsKey("template_2_alias"));
        assertFalse(result.metaData().index("test").getAliases().containsKey("template_3_alias"));
    }

    public void testApplyDataFromTemplate() throws Exception {
        tplBuilder.put("template_1", createTemplateMetadata(
            "template_1", "te*",
            AliasMetaData.builder("alias1").build(),
            "customType", null,
            "mappingType", createMapping()
        ));

        final ClusterState result = executeTask(state);

        assertTrue(result.metaData().index("test").getAliases().containsKey("alias1"));
        assertTrue(result.metaData().index("test").getCustoms().containsKey("customType"));

        final ArgumentCaptor<Map> argument = ArgumentCaptor.forClass(Map.class);

        verify(mapper).merge(argument.capture(), anyObject(), anyBoolean());
        assertTrue(argument.getValue().containsKey("mappingType"));
    }

    public void testRequestDataHavePriorityOverTemplateData() throws Exception {
        final AliasMetaData tplAlias = AliasMetaData.builder("alias1").searchRouting("fromTpl").build();
        final CompressedXContent tplMapping = createMapping("text");
        final CompressedXContent reqMapping = createMapping("keyword");

        tplBuilder.put("template_1", createTemplateMetadata(
            "template_1", "te*",
            tplAlias,
            "customType", null,
            "mappingType", tplMapping
        ));

        setupRequestAlias(new Alias("alias1").searchRouting("fromReq"));
        setupRequestMapping(reqMapping);

        final ClusterState result = executeTask(state);

        assertEquals("fromReq", result.metaData().index("test").getAliases().get("alias1").getSearchRouting());
        assertEquals("{type={properties={field={type=keyword}}}}", getMappingsFromResponse().get("mappingType").toString());
    }

    public void testApplyDataFromRequest() throws Exception {
        final Map<String, String> mappings = new HashMap<>();
        mappings.put("mapping1", createMapping().string());

        final Map<String, IndexMetaData.Custom> customs = new HashMap<>();
        customs.put("custom1", mock(IndexMetaData.Custom.class));

        when(request.aliases()).thenReturn(new HashSet<>(Arrays.asList(new Alias("alias1"))));
        when(request.mappings()).thenReturn(mappings);
        when(request.customs()).thenReturn(customs);

        final ClusterState result = executeTask(state);

        assertTrue(result.metaData().index("test").getAliases().containsKey("alias1"));
        assertTrue(result.metaData().index("test").getCustoms().containsKey("custom1"));

        final ArgumentCaptor<Map> argument = ArgumentCaptor.forClass(Map.class);

        verify(mapper).merge(argument.capture(), anyObject(), anyBoolean());
        assertTrue(argument.getValue().containsKey("mapping1"));
    }

    /*
     * @todo test settings (template + request)
     * @todo test custom (template + request)
     * @todo test template order
     * @todo test shrink == true
     */

    private Map<String, Object> getMappingsFromResponse() {
        final ArgumentCaptor<Map> argument = ArgumentCaptor.forClass(Map.class);
        verify(mapper).merge(argument.capture(), anyObject(), anyBoolean());
        return argument.getValue();
    }

    private void setupRequestAlias(Alias alias) {
        when(request.aliases()).thenReturn(new HashSet<>(Arrays.asList(alias)));
    }

    private void setupRequestMapping(CompressedXContent mapping) throws IOException {
        final Map<String, String> mappings = new HashMap<>();
        mappings.put("mappingType", mapping.string());
        when(request.mappings()).thenReturn(mappings);
    }

    private CompressedXContent createMapping() throws IOException {
        return createMapping("text");
    }

    private CompressedXContent createMapping(String fieldType) throws IOException {
        final String mapping = XContentFactory.jsonBuilder()
            .startObject()
                .startObject("type")
                    .startObject("properties")
                        .startObject("field")
                            .field("type", fieldType)
                        .endObject()
                    .endObject()
                .endObject()
            .endObject().string();

        return new CompressedXContent(mapping);
    }

    @SuppressWarnings("unchecked")
    private ClusterState executeTask(ClusterState state) throws Exception {
        setupState();
        setupRequest();
        setupClusterState();
        setupIndicesService();
        final MetaDataCreateIndexService.IndexCreationTask task = new MetaDataCreateIndexService.IndexCreationTask(
            logger, allocationService, request, listener, indicesService, aliasValidator, xContentRegistry, settings, validator
        );
        return task.execute(state);
    }

    private IndexTemplateMetaData createTemplateMetadata(String name, String pattern,
                                                         AliasMetaData aliasesMetaData,
                                                         String customType, IndexMetaData.Custom customMetaData,
                                                         String mappingType, CompressedXContent mappingSource) throws Exception {
        return IndexTemplateMetaData
            .builder(name)
            .patterns(Arrays.asList(pattern))
            .putAlias(aliasesMetaData)
            .putCustom(customType, customMetaData)
            .putMapping(mappingType, mappingSource)
            .build();
    }

    private IndexTemplateMetaData createTemplateMetadata(String name, String pattern) {
        return IndexTemplateMetaData
            .builder(name)
            .patterns(Arrays.asList(pattern))
            .putAlias(AliasMetaData.builder(name + "_alias").build())
            .build();
    }

    private void setupState() {
        final ImmutableOpenMap.Builder<String, ClusterState.Custom> stateCustomsBuilder = ImmutableOpenMap.builder();

        final MetaData.Builder builder = MetaData.builder();
        builder
            .customs(customBuilder.build())
            .templates(tplBuilder.build())
            .indices(idxBuilder.build());
        when(state.metaData()).thenReturn(builder.build());

        final ImmutableOpenMap.Builder<String, Set<ClusterBlock>> blockIdxBuilder = ImmutableOpenMap.builder();
        final ClusterBlocks blocks = mock(ClusterBlocks.class);
        when(blocks.indices()).thenReturn(blockIdxBuilder.build());

        when(state.blocks()).thenReturn(blocks);
        when(state.customs()).thenReturn(stateCustomsBuilder.build());
    }

    private void setupRequest() {
        final Settings reqSettings = Settings.builder().build();
        when(request.settings()).thenReturn(reqSettings);
        when(request.index()).thenReturn("test");
        when(request.waitForActiveShards()).thenReturn(ActiveShardCount.DEFAULT);

        final Set<ClusterBlock> blocks = Sets.newHashSet();
        when(request.blocks()).thenReturn(blocks);
    }

    private void setupClusterState() {
        final DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        when(nodes.getSmallestNonClientNodeVersion()).thenReturn(Version.CURRENT);

        when(state.nodes()).thenReturn(nodes);
    }

    @SuppressWarnings("unchecked")
    private void setupIndicesService() throws Exception {
        final Index index = mock(Index.class);
        final DocumentMapper docMapper = mock(DocumentMapper.class);
        when(docMapper.routingFieldMapper()).thenReturn(mock(RoutingFieldMapper.class));
        when(docMapper.parentFieldMapper()).thenReturn(mock(ParentFieldMapper.class));

        when(mapper.docMappers(anyBoolean())).thenReturn(Collections.singletonList(docMapper));

        final Supplier<Sort> supplier = mock(Supplier.class);

        final IndexService service = mock(IndexService.class);
        when(service.index()).thenReturn(index);
        when(service.mapperService()).thenReturn(mapper);
        when(service.getIndexSortSupplier()).thenReturn(supplier);
        when(service.getIndexEventListener()).thenReturn(mock(IndexEventListener.class));

        when(indicesService.createIndex(anyObject(), anyObject())).thenReturn(service);
    }
}

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

package org.elasticsearch.index.mapper;

import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Collection;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;


public class UpdateMappingTests extends ESSingleNodeTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class);
    }

    public void testConflictFieldsMapping(String fieldName) throws Exception {
        //test store, ... all the parameters that are not to be changed just like in other fields
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
                .startObject("_doc")
                    .startObject(fieldName)
                        .field("enabled", true)
                            .field("store", false)
                        .endObject()
                    .endObject()
            .endObject();
        XContentBuilder mappingUpdate = XContentFactory.jsonBuilder()
            .startObject()
                .startObject("_doc")
                    .startObject(fieldName)
                        .field("enabled", true)
                        .field("store", true)
                    .endObject()
                    .startObject("properties")
                        .startObject("text")
                            .field("type", "text")
                        .endObject()
                    .endObject()
                .endObject()
            .endObject();
        testConflictWhileMergingAndMappingUnchanged(mapping, mappingUpdate);
    }

    protected void testConflictWhileMergingAndMappingUnchanged(XContentBuilder mapping, XContentBuilder mappingUpdate) throws IOException {
        IndexService indexService = createIndex("test", Settings.builder().build(), mapping);
        CompressedXContent mappingBeforeUpdate = indexService.mapperService().documentMapper().mappingSource();
        // simulate like in MetadataMappingService#putMapping
        try {
            indexService.mapperService().merge("type", new CompressedXContent(BytesReference.bytes(mappingUpdate)),
                MapperService.MergeReason.MAPPING_UPDATE);
            fail();
        } catch (IllegalArgumentException e) {
            // expected
        }
        // make sure simulate flag actually worked - no mappings applied
        CompressedXContent mappingAfterUpdate = indexService.mapperService().documentMapper().mappingSource();
        assertThat(mappingAfterUpdate, equalTo(mappingBeforeUpdate));
    }

    public void testConflictSameType() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("_doc")
                .startObject("properties").startObject("foo").field("type", "long").endObject()
                .endObject().endObject().endObject();
        MapperService mapperService = createIndex("test", Settings.builder().build(), mapping).mapperService();

        XContentBuilder update = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("foo").field("type", "double").endObject()
                .endObject().endObject().endObject();

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () ->
            mapperService.merge("type", new CompressedXContent(Strings.toString(update)), MapperService.MergeReason.MAPPING_UPDATE));
        assertThat(e.getMessage(), containsString("mapper [foo] cannot be changed from type [long] to [double]"));

        e = expectThrows(IllegalArgumentException.class, () ->
            mapperService.merge("type", new CompressedXContent(Strings.toString(update)), MapperService.MergeReason.MAPPING_UPDATE));
        assertThat(e.getMessage(), containsString("mapper [foo] cannot be changed from type [long] to [double]"));

        assertThat(((FieldMapper) mapperService.documentMapper().mapping().root().getMapper("foo")).fieldType().typeName(),
                equalTo("long"));
    }

    public void testConflictNewType() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("_doc")
                .startObject("properties").startObject("foo").field("type", "long").endObject()
                .endObject().endObject().endObject();
        MapperService mapperService = createIndex("test", Settings.builder().build(), mapping).mapperService();

        XContentBuilder update = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("foo").field("type", "double").endObject()
                .endObject().endObject().endObject();

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () ->
            mapperService.merge("type", new CompressedXContent(Strings.toString(update)), MapperService.MergeReason.MAPPING_UPDATE));
        assertThat(e.getMessage(), containsString("mapper [foo] cannot be changed from type [long] to [double]"));

        assertThat(((FieldMapper) mapperService.documentMapper().mapping().root().getMapper("foo")).fieldType().typeName(),
                equalTo("long"));
    }

    public void testReuseMetaField() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("_id").field("type", "text").endObject()
                .endObject().endObject().endObject();
        MapperService mapperService = createIndex("test", Settings.builder().build()).mapperService();

        MapperParsingException e = expectThrows(MapperParsingException.class, () ->
            mapperService.merge("type", new CompressedXContent(Strings.toString(mapping)), MapperService.MergeReason.MAPPING_UPDATE));
        assertThat(e.getMessage(), containsString("Field [_id] is defined more than once"));

        MapperParsingException e2 = expectThrows(MapperParsingException.class, () ->
            mapperService.merge("type", new CompressedXContent(Strings.toString(mapping)), MapperService.MergeReason.MAPPING_UPDATE));
        assertThat(e2.getMessage(), containsString("Field [_id] is defined more than once"));
    }

    public void testRejectFieldDefinedTwice() throws IOException {
        String mapping1 = Strings.toString(XContentFactory.jsonBuilder().startObject()
                .startObject("type")
                    .startObject("properties")
                        .startObject("foo")
                            .field("type", "object")
                        .endObject()
                    .endObject()
                .endObject().endObject());
        String mapping2 = Strings.toString(XContentFactory.jsonBuilder().startObject()
                .startObject("type")
                    .startObject("properties")
                        .startObject("foo")
                            .field("type", "long")
                        .endObject()
                    .endObject()
                .endObject().endObject());

        MapperService mapperService1 = createIndex("test1").mapperService();
        mapperService1.merge("type", new CompressedXContent(mapping1), MergeReason.MAPPING_UPDATE);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> mapperService1.merge("type", new CompressedXContent(mapping2), MergeReason.MAPPING_UPDATE));
        assertThat(e.getMessage(), equalTo("can't merge a non object mapping [foo] with an object mapping"));

        MapperService mapperService2 = createIndex("test2").mapperService();
        mapperService2.merge("type", new CompressedXContent(mapping2), MergeReason.MAPPING_UPDATE);
        e = expectThrows(IllegalArgumentException.class,
                () -> mapperService2.merge("type", new CompressedXContent(mapping1), MergeReason.MAPPING_UPDATE));
        assertThat(e.getMessage(), equalTo("can't merge a non object mapping [foo] with an object mapping"));
    }

    public void testMappingVersion() {
        createIndex("test", client().admin().indices().prepareCreate("test"));
        final ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        {
            final long previousVersion = clusterService.state().metadata().index("test").getMappingVersion();
            final PutMappingRequest request = new PutMappingRequest();
            request.indices("test");
            request.source("field", "type=text");
            client().admin().indices().putMapping(request).actionGet();
            assertThat(clusterService.state().metadata().index("test").getMappingVersion(), Matchers.equalTo(1 + previousVersion));
        }

        {
            final long previousVersion = clusterService.state().metadata().index("test").getMappingVersion();
            final PutMappingRequest request = new PutMappingRequest();
            request.indices("test");
            request.source("field", "type=text");
            client().admin().indices().putMapping(request).actionGet();
            // the version should be unchanged after putting the same mapping again
            assertThat(clusterService.state().metadata().index("test").getMappingVersion(), Matchers.equalTo(previousVersion));
        }
    }

}

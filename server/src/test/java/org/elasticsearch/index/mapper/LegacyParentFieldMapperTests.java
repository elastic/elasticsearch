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

import org.elasticsearch.Version;
import org.elasticsearch.action.RoutingMissingException;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.io.IOException;

import static org.elasticsearch.index.mapper.ParentFieldMapperTests.getNumberOfFieldWithParentPrefix;
import static org.hamcrest.Matchers.equalTo;

public class LegacyParentFieldMapperTests extends ESSingleNodeTestCase {

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    public void testJoinFieldSet() throws Exception {
        String parentMapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("parent_type")
                .endObject().endObject());
        String childMapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("child_type")
                .startObject("_parent").field("type", "parent_type").endObject()
                .endObject().endObject());
        IndexService indexService = createIndex("test", Settings.builder().put("index.version.created", Version.V_5_6_0).build());
        indexService.mapperService().merge("parent_type", new CompressedXContent(parentMapping), MergeReason.MAPPING_UPDATE, false);
        indexService.mapperService().merge("child_type", new CompressedXContent(childMapping), MergeReason.MAPPING_UPDATE, false);

        // Indexing parent doc:
        DocumentMapper parentDocMapper = indexService.mapperService().documentMapper("parent_type");
        ParsedDocument doc =
                parentDocMapper.parse(SourceToParse.source("test", "parent_type", "1122", new BytesArray("{}"), XContentType.JSON));
        assertEquals(1, getNumberOfFieldWithParentPrefix(doc.rootDoc()));
        assertEquals("1122", doc.rootDoc().getBinaryValue("_parent#parent_type").utf8ToString());

        // Indexing child doc:
        DocumentMapper childDocMapper = indexService.mapperService().documentMapper("child_type");
        doc = childDocMapper.parse(SourceToParse.source("test", "child_type", "1", new BytesArray("{}"), XContentType.JSON).parent("1122"));

        assertEquals(1, getNumberOfFieldWithParentPrefix(doc.rootDoc()));
        assertEquals("1122", doc.rootDoc().getBinaryValue("_parent#parent_type").utf8ToString());
    }

    public void testUpdateEagerGlobalOrds() throws IOException {
        String parentMapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("parent_type")
                .endObject().endObject());
        String childMapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("child_type")
                .startObject("_parent").field("type", "parent_type").endObject()
                .endObject().endObject());
        IndexService indexService = createIndex("test", Settings.builder().put("index.version.created", Version.V_5_6_0).build());
        indexService.mapperService().merge("parent_type", new CompressedXContent(parentMapping), MergeReason.MAPPING_UPDATE, false);
        indexService.mapperService().merge("child_type", new CompressedXContent(childMapping), MergeReason.MAPPING_UPDATE, false);

        assertTrue(indexService.mapperService().documentMapper("child_type").parentFieldMapper().fieldType().eagerGlobalOrdinals());

        String childMappingUpdate = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("child_type")
                .startObject("_parent").field("type", "parent_type").field("eager_global_ordinals", false).endObject()
                .endObject().endObject());
        indexService.mapperService().merge("child_type", new CompressedXContent(childMappingUpdate), MergeReason.MAPPING_UPDATE, false);

        assertFalse(indexService.mapperService().documentMapper("child_type").parentFieldMapper().fieldType().eagerGlobalOrdinals());
    }

    public void testIndexAndGet() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject()
            .startObject("child_type")
                .startObject("_parent")
                    .field("type", "parent_type")
                .endObject()
            .endObject().endObject();
        createIndex("test", Settings.builder().put("index.version.created", Version.V_5_6_0).build(), "child_type", mapping);

        expectThrows(RoutingMissingException.class,
            () -> client().prepareIndex("test", "child_type", "1").setSource("foo", "bar").get());
        client().prepareIndex("test", "child_type", "1")
            .setParent("0")
            .setSource("foo", "bar")
            .get();

        expectThrows(RoutingMissingException.class,
            () -> client().prepareGet("test", "child_type", "1").get());

        GetResponse resp = client().prepareGet("test", "child_type", "1")
            .setParent("0")
            .get();
        assertTrue(resp.isExists());
        assertThat(resp.getId(), equalTo("1"));
        assertThat(resp.getField("_routing").getValue(), equalTo("0"));
        assertThat(resp.getField("_parent").getValue(), equalTo("0"));

        expectThrows(RoutingMissingException.class,
            () -> client().prepareDelete("test", "child_type", "1").get());
        DeleteResponse deleteResp = client().prepareDelete("test", "child_type", "1").setParent("0").get();
        assertThat(deleteResp.status(), equalTo(RestStatus.OK));
    }

    public void testUpdateAndGet() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject()
            .startObject("child_type")
            .startObject("_parent")
            .field("type", "parent_type")
            .endObject()
            .endObject().endObject();
        createIndex("test", Settings.builder().put("index.version.created", Version.V_5_6_0).build(), "child_type", mapping);

        expectThrows(RoutingMissingException.class,
            () -> client().prepareUpdate("test", "child_type", "1").setDoc("foo", "bar").setUpsert("foo", "bar").get());
        client().prepareUpdate("test", "child_type", "1")
            .setParent("0")
            .setDoc("foo", "bar")
            .setUpsert("foo", "bar")
            .get();

        expectThrows(RoutingMissingException.class,
            () -> client().prepareGet("test", "child_type", "1").get());

        GetResponse resp = client().prepareGet("test", "child_type", "1")
            .setParent("0")
            .get();
        assertTrue(resp.isExists());
        assertThat(resp.getId(), equalTo("1"));
        assertThat(resp.getField("_routing").getValue(), equalTo("0"));
        assertThat(resp.getField("_parent").getValue(), equalTo("0"));

        expectThrows(RoutingMissingException.class,
            () -> client().prepareDelete("test", "child_type", "1").get());
        DeleteResponse deleteResp = client().prepareDelete("test", "child_type", "1").setParent("0").get();
        assertThat(deleteResp.status(), equalTo(RestStatus.OK));
    }
}

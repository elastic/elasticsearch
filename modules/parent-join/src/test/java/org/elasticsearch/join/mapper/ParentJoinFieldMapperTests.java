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

package org.elasticsearch.join.mapper;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperException;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.join.ParentJoinPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.Collection;
import java.util.Collections;

import static org.hamcrest.Matchers.containsString;

public class ParentJoinFieldMapperTests extends ESSingleNodeTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singletonList(ParentJoinPlugin.class);
    }

    public void testSingleLevel() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
            .startObject("properties")
                .startObject("join_field")
                    .field("type", "join")
                    .startObject("relations")
                        .field("parent", "child")
                    .endObject()
                .endObject()
            .endObject()
            .endObject());
        IndexService service = createIndex("test");
        DocumentMapper docMapper = service.mapperService().merge("type", new CompressedXContent(mapping),
            MapperService.MergeReason.MAPPING_UPDATE);
        assertTrue(docMapper.mappers().getMapper("join_field") == ParentJoinFieldMapper.getMapper(service.mapperService()));

        // Doc without join
        ParsedDocument doc = docMapper.parse(new SourceToParse("test", "0",
            BytesReference.bytes(XContentFactory.jsonBuilder().startObject().endObject()), XContentType.JSON));
        assertNull(doc.rootDoc().getBinaryValue("join_field"));

        // Doc parent
        doc = docMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder().startObject()
                .field("join_field", "parent")
                .endObject()), XContentType.JSON));
        assertEquals("1", doc.rootDoc().getBinaryValue("join_field#parent").utf8ToString());
        assertEquals("parent", doc.rootDoc().getBinaryValue("join_field").utf8ToString());

        // Doc child
        doc = docMapper.parse(new SourceToParse("test", "2",
            BytesReference.bytes(XContentFactory.jsonBuilder().startObject()
                .startObject("join_field")
                    .field("name", "child")
                    .field("parent", "1")
                .endObject()
                .endObject()), XContentType.JSON, "1"));
        assertEquals("1", doc.rootDoc().getBinaryValue("join_field#parent").utf8ToString());
        assertEquals("child", doc.rootDoc().getBinaryValue("join_field").utf8ToString());

        // Unknown join name
        MapperException exc = expectThrows(MapperParsingException.class,
            () -> docMapper.parse(new SourceToParse("test", "1",
                BytesReference.bytes(XContentFactory.jsonBuilder().startObject()
                    .field("join_field", "unknown")
                    .endObject()), XContentType.JSON)));
        assertThat(exc.getRootCause().getMessage(), containsString("unknown join name [unknown] for field [join_field]"));
    }

    public void testParentIdSpecifiedAsNumber() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
            .startObject("properties")
                .startObject("join_field")
                    .field("type", "join")
                        .startObject("relations")
                            .field("parent", "child")
                        .endObject()
                    .endObject()
                .endObject()
            .endObject());
        IndexService service = createIndex("test");
        DocumentMapper docMapper = service.mapperService().merge("type", new CompressedXContent(mapping),
            MapperService.MergeReason.MAPPING_UPDATE);
        ParsedDocument doc = docMapper.parse(new SourceToParse("test", "2",
            BytesReference.bytes(XContentFactory.jsonBuilder().startObject()
                .startObject("join_field")
                .field("name", "child")
                .field("parent", 1)
                .endObject()
                .endObject()), XContentType.JSON, "1"));
        assertEquals("1", doc.rootDoc().getBinaryValue("join_field#parent").utf8ToString());
        assertEquals("child", doc.rootDoc().getBinaryValue("join_field").utf8ToString());
        doc = docMapper.parse(new SourceToParse("test", "2",
            BytesReference.bytes(XContentFactory.jsonBuilder().startObject()
                .startObject("join_field")
                .field("name", "child")
                .field("parent", 1.0)
                .endObject()
                .endObject()), XContentType.JSON, "1"));
        assertEquals("1.0", doc.rootDoc().getBinaryValue("join_field#parent").utf8ToString());
        assertEquals("child", doc.rootDoc().getBinaryValue("join_field").utf8ToString());
    }

    public void testMultipleLevels() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
            .startObject("properties")
                .startObject("join_field")
                    .field("type", "join")
                    .startObject("relations")
                        .field("parent", "child")
                        .field("child", "grand_child")
                    .endObject()
                .endObject()
            .endObject()
            .endObject());
        IndexService service = createIndex("test");
        DocumentMapper docMapper = service.mapperService().merge("type", new CompressedXContent(mapping),
            MapperService.MergeReason.MAPPING_UPDATE);
        assertTrue(docMapper.mappers().getMapper("join_field") == ParentJoinFieldMapper.getMapper(service.mapperService()));

        // Doc without join
        ParsedDocument doc = docMapper.parse(new SourceToParse("test", "0",
            BytesReference.bytes(XContentFactory.jsonBuilder().startObject().endObject()), XContentType.JSON));
        assertNull(doc.rootDoc().getBinaryValue("join_field"));

        // Doc parent
        doc = docMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject()
                    .field("join_field", "parent")
                .endObject()), XContentType.JSON));
        assertEquals("1", doc.rootDoc().getBinaryValue("join_field#parent").utf8ToString());
        assertEquals("parent", doc.rootDoc().getBinaryValue("join_field").utf8ToString());

        // Doc child
        doc = docMapper.parse(new SourceToParse("test", "2",
            BytesReference.bytes(XContentFactory.jsonBuilder().startObject()
                .startObject("join_field")
                    .field("name", "child")
                    .field("parent", "1")
                .endObject()
                .endObject()), XContentType.JSON, "1"));
        assertEquals("1", doc.rootDoc().getBinaryValue("join_field#parent").utf8ToString());
        assertEquals("2", doc.rootDoc().getBinaryValue("join_field#child").utf8ToString());
        assertEquals("child", doc.rootDoc().getBinaryValue("join_field").utf8ToString());

        // Doc child missing parent
        MapperException exc = expectThrows(MapperParsingException.class,
            () -> docMapper.parse(new SourceToParse("test", "2",
                BytesReference.bytes(XContentFactory.jsonBuilder().startObject()
                    .field("join_field", "child")
                    .endObject()), XContentType.JSON, "1")));
        assertThat(exc.getRootCause().getMessage(), containsString("[parent] is missing for join field [join_field]"));

        // Doc child missing routing
        exc = expectThrows(MapperParsingException.class,
            () -> docMapper.parse(new SourceToParse("test", "2",
                BytesReference.bytes(XContentFactory.jsonBuilder().startObject()
                    .startObject("join_field")
                    .field("name", "child")
                    .field("parent", "1")
                    .endObject()
                    .endObject()), XContentType.JSON)));
        assertThat(exc.getRootCause().getMessage(), containsString("[routing] is missing for join field [join_field]"));

        // Doc grand_child
        doc = docMapper.parse(new SourceToParse("test", "3",
            BytesReference.bytes(XContentFactory.jsonBuilder().startObject()
                .startObject("join_field")
                    .field("name", "grand_child")
                    .field("parent", "2")
                .endObject()
                .endObject()), XContentType.JSON, "1"));
        assertEquals("2", doc.rootDoc().getBinaryValue("join_field#child").utf8ToString());
        assertEquals("grand_child", doc.rootDoc().getBinaryValue("join_field").utf8ToString());

        // Unknown join name
        exc = expectThrows(MapperParsingException.class,
            () -> docMapper.parse(new SourceToParse("test", "1",
                BytesReference.bytes(XContentFactory.jsonBuilder().startObject()
                    .field("join_field", "unknown")
                    .endObject()), XContentType.JSON)));
        assertThat(exc.getRootCause().getMessage(), containsString("unknown join name [unknown] for field [join_field]"));
    }

    public void testUpdateRelations() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("properties")
            .startObject("join_field")
                .field("type", "join")
                .startObject("relations")
                    .field("parent", "child")
                    .array("child", "grand_child1", "grand_child2")
                .endObject()
            .endObject()
            .endObject().endObject());
        IndexService indexService = createIndex("test");
        DocumentMapper docMapper = indexService.mapperService().merge("type", new CompressedXContent(mapping),
            MapperService.MergeReason.MAPPING_UPDATE);
        assertTrue(docMapper.mappers().getMapper("join_field") == ParentJoinFieldMapper.getMapper(indexService.mapperService()));

        {
            final String updateMapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("properties")
                .startObject("join_field")
                    .field("type", "join")
                    .startObject("relations")
                        .array("child", "grand_child1", "grand_child2")
                    .endObject()
                .endObject()
                .endObject().endObject());
            IllegalArgumentException exc = expectThrows(IllegalArgumentException.class,
                () -> indexService.mapperService().merge("type", new CompressedXContent(updateMapping),
                    MapperService.MergeReason.MAPPING_UPDATE));
            assertThat(exc.getMessage(), containsString("cannot remove parent [parent] in join field [join_field]"));
        }

        {
            final String updateMapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("properties")
                .startObject("join_field")
                    .field("type", "join")
                    .startObject("relations")
                        .field("parent", "child")
                        .field("child", "grand_child1")
                    .endObject()
                .endObject()
                .endObject().endObject());
            IllegalArgumentException exc = expectThrows(IllegalArgumentException.class,
                () -> indexService.mapperService().merge("type", new CompressedXContent(updateMapping),
                    MapperService.MergeReason.MAPPING_UPDATE));
            assertThat(exc.getMessage(), containsString("cannot remove child [grand_child2] in join field [join_field]"));
        }

        {
            final String updateMapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("properties")
                .startObject("join_field")
                    .field("type", "join")
                    .startObject("relations")
                        .field("uber_parent", "parent")
                        .field("parent", "child")
                        .array("child", "grand_child1", "grand_child2")
                    .endObject()
                .endObject()
                .endObject().endObject());
            IllegalArgumentException exc = expectThrows(IllegalArgumentException.class,
                () -> indexService.mapperService().merge("type", new CompressedXContent(updateMapping),
                    MapperService.MergeReason.MAPPING_UPDATE));
            assertThat(exc.getMessage(), containsString("cannot create child [parent] from an existing parent"));
        }

        {
            final String updateMapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("properties")
                .startObject("join_field")
                    .field("type", "join")
                    .startObject("relations")
                        .field("parent", "child")
                        .array("child", "grand_child1", "grand_child2")
                        .field("grand_child2", "grand_grand_child")
                    .endObject()
                .endObject()
                .endObject().endObject());
            IllegalArgumentException exc = expectThrows(IllegalArgumentException.class,
                () -> indexService.mapperService().merge("type", new CompressedXContent(updateMapping),
                    MapperService.MergeReason.MAPPING_UPDATE));
            assertThat(exc.getMessage(), containsString("cannot create parent [grand_child2] from an existing child]"));
        }

        {
            final String updateMapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("properties")
                .startObject("join_field")
                    .field("type", "join")
                    .startObject("relations")
                        .array("parent", "child", "child2")
                        .array("child", "grand_child1", "grand_child2")
                    .endObject()
                .endObject()
                .endObject().endObject());
            docMapper = indexService.mapperService().merge("type", new CompressedXContent(updateMapping),
                MapperService.MergeReason.MAPPING_UPDATE);
            assertTrue(docMapper.mappers().getMapper("join_field") == ParentJoinFieldMapper.getMapper(indexService.mapperService()));
            ParentJoinFieldMapper mapper = ParentJoinFieldMapper.getMapper(indexService.mapperService());
            assertTrue(mapper.hasChild("child2"));
            assertFalse(mapper.hasParent("child2"));
            assertTrue(mapper.hasChild("grand_child2"));
            assertFalse(mapper.hasParent("grand_child2"));
        }

        {
            final String updateMapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("properties")
                .startObject("join_field")
                    .field("type", "join")
                    .startObject("relations")
                        .array("parent", "child", "child2")
                        .array("child", "grand_child1", "grand_child2")
                        .array("other", "child_other1", "child_other2")
                    .endObject()
                .endObject()
                .endObject().endObject());
            docMapper = indexService.mapperService().merge("type", new CompressedXContent(updateMapping),
                MapperService.MergeReason.MAPPING_UPDATE);
            assertTrue(docMapper.mappers().getMapper("join_field") == ParentJoinFieldMapper.getMapper(indexService.mapperService()));
            ParentJoinFieldMapper mapper = ParentJoinFieldMapper.getMapper(indexService.mapperService());
            assertTrue(mapper.hasParent("other"));
            assertFalse(mapper.hasChild("other"));
            assertTrue(mapper.hasChild("child_other1"));
            assertFalse(mapper.hasParent("child_other1"));
            assertTrue(mapper.hasChild("child_other2"));
            assertFalse(mapper.hasParent("child_other2"));
        }
    }

    public void testInvalidJoinFieldInsideObject() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("properties")
            .startObject("object")
                .startObject("properties")
                    .startObject("join_field")
                        .field("type", "join")
                        .startObject("relations")
                            .field("parent", "child")
                        .endObject()
                    .endObject()
                .endObject()
            .endObject()
            .endObject().endObject());
        IndexService indexService = createIndex("test");
        MapperParsingException exc = expectThrows(MapperParsingException.class,
            () -> indexService.mapperService().merge("type", new CompressedXContent(mapping),
                MapperService.MergeReason.MAPPING_UPDATE));
        assertThat(exc.getRootCause().getMessage(),
            containsString("join field [object.join_field] cannot be added inside an object or in a multi-field"));
    }

    public void testInvalidJoinFieldInsideMultiFields() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("properties")
            .startObject("number")
                .field("type", "integer")
                .startObject("fields")
                    .startObject("join_field")
                        .field("type", "join")
                        .startObject("relations")
                            .field("parent", "child")
                        .endObject()
                    .endObject()
                .endObject()
            .endObject()
            .endObject().endObject());
        IndexService indexService = createIndex("test");
        MapperParsingException exc = expectThrows(MapperParsingException.class,
            () -> indexService.mapperService().merge("type", new CompressedXContent(mapping),
                MapperService.MergeReason.MAPPING_UPDATE));
        assertThat(exc.getRootCause().getMessage(),
            containsString("join field [number.join_field] cannot be added inside an object or in a multi-field"));
    }

    public void testMultipleJoinFields() throws Exception {
        IndexService indexService = createIndex("test");
        {
            String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
                .startObject("properties")
                    .startObject("join_field")
                        .field("type", "join")
                        .startObject("relations")
                            .field("parent", "child")
                            .field("child", "grand_child")
                        .endObject()
                    .endObject()
                    .startObject("another_join_field")
                        .field("type", "join")
                        .startObject("relations")
                            .field("product", "item")
                        .endObject()
                    .endObject()
                .endObject()
                .endObject());
            IllegalArgumentException exc = expectThrows(IllegalArgumentException.class, () -> indexService.mapperService().merge("type",
                new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE));
            assertThat(exc.getMessage(), containsString("Field [_parent_join] is defined twice."));
        }

        {
            String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
                .startObject("properties")
                    .startObject("join_field")
                        .field("type", "join")
                        .startObject("relations")
                            .field("parent", "child")
                            .field("child", "grand_child")
                        .endObject()
                    .endObject()
                .endObject()
                .endObject());
            indexService.mapperService().merge("type",
                new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE);
            String updateMapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
                .startObject("properties")
                    .startObject("another_join_field")
                        .field("type", "join")
                    .endObject()
                .endObject()
                .endObject());
            IllegalArgumentException exc = expectThrows(IllegalArgumentException.class, () -> indexService.mapperService().merge("type",
                new CompressedXContent(updateMapping), MapperService.MergeReason.MAPPING_UPDATE));
            assertThat(exc.getMessage(), containsString("Field [_parent_join] is defined twice."));
        }
    }

    public void testEagerGlobalOrdinals() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
            .startObject("properties")
                .startObject("join_field")
                    .field("type", "join")
                    .startObject("relations")
                        .field("parent", "child")
                        .field("child", "grand_child")
                    .endObject()
                .endObject()
            .endObject()
            .endObject());
        IndexService service = createIndex("test");
        DocumentMapper docMapper = service.mapperService().merge("type", new CompressedXContent(mapping),
            MapperService.MergeReason.MAPPING_UPDATE);
        assertTrue(docMapper.mappers().getMapper("join_field") == ParentJoinFieldMapper.getMapper(service.mapperService()));
        assertFalse(service.mapperService().fieldType("join_field").eagerGlobalOrdinals());
        assertNotNull(service.mapperService().fieldType("join_field#parent"));
        assertTrue(service.mapperService().fieldType("join_field#parent").eagerGlobalOrdinals());
        assertNotNull(service.mapperService().fieldType("join_field#child"));
        assertTrue(service.mapperService().fieldType("join_field#child").eagerGlobalOrdinals());

        mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
            .startObject("properties")
                .startObject("join_field")
                    .field("type", "join")
                    .field("eager_global_ordinals", false)
                    .startObject("relations")
                        .field("parent", "child")
                        .field("child", "grand_child")
                    .endObject()
                .endObject()
            .endObject()
            .endObject());
        service.mapperService().merge("type", new CompressedXContent(mapping),
            MapperService.MergeReason.MAPPING_UPDATE);
        assertFalse(service.mapperService().fieldType("join_field").eagerGlobalOrdinals());
        assertNotNull(service.mapperService().fieldType("join_field#parent"));
        assertFalse(service.mapperService().fieldType("join_field#parent").eagerGlobalOrdinals());
        assertNotNull(service.mapperService().fieldType("join_field#child"));
        assertFalse(service.mapperService().fieldType("join_field#child").eagerGlobalOrdinals());
    }
}

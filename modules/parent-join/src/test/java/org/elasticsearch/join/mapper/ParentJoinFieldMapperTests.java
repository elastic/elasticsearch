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

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperException;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.join.ParentJoinPlugin;
import org.elasticsearch.plugins.Plugin;

import java.util.Collection;

import static java.util.Collections.singleton;
import static org.hamcrest.Matchers.containsString;

public class ParentJoinFieldMapperTests extends MapperServiceTestCase {
    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return singleton(new ParentJoinPlugin());
    }

    public void testSingleLevel() throws Exception {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("join_field");
            b.field("type", "join").startObject("relations").field("parent", "child").endObject();
            b.endObject();
        }));
        DocumentMapper docMapper = mapperService.documentMapper();
        assertSame(
            docMapper.mappers().getMapper("join_field"),
            ParentJoinFieldMapper.getMapper(mapperService::fieldType, mapperService.documentMapper().mappers()::getMapper)
        );

        // Doc without join
        ParsedDocument doc = docMapper.parse(source(b -> {}));
        assertNull(doc.rootDoc().getBinaryValue("join_field"));

        // Doc parent
        doc = docMapper.parse(source(b -> b.field("join_field", "parent")));
        assertEquals("1", doc.rootDoc().getBinaryValue("join_field#parent").utf8ToString());
        assertEquals("parent", doc.rootDoc().getBinaryValue("join_field").utf8ToString());

        // Doc child
        doc = docMapper.parse(source("2", b -> b.startObject("join_field").field("name", "child").field("parent", "1").endObject(), "1"));
        assertEquals("1", doc.rootDoc().getBinaryValue("join_field#parent").utf8ToString());
        assertEquals("child", doc.rootDoc().getBinaryValue("join_field").utf8ToString());

        // Unknown join name
        MapperException exc = expectThrows(MapperParsingException.class,
            () -> docMapper.parse(source(b -> b.field("join_field", "unknown"))));
        assertThat(exc.getRootCause().getMessage(), containsString("unknown join name [unknown] for field [join_field]"));
    }

    public void testParentIdSpecifiedAsNumber() throws Exception {
        DocumentMapper docMapper = createDocumentMapper(mapping(b -> {
            b.startObject("join_field");
            b.field("type", "join").startObject("relations").field("parent", "child").endObject();
            b.endObject();
        }));

        ParsedDocument doc = docMapper.parse(
            source("2", b -> b.startObject("join_field").field("name", "child").field("parent", 1).endObject(), "1")
        );
        assertEquals("1", doc.rootDoc().getBinaryValue("join_field#parent").utf8ToString());
        assertEquals("child", doc.rootDoc().getBinaryValue("join_field").utf8ToString());

        doc = docMapper.parse(source("2", b -> b.startObject("join_field").field("name", "child").field("parent", 1.0).endObject(), "1"));
        assertEquals("1.0", doc.rootDoc().getBinaryValue("join_field#parent").utf8ToString());
        assertEquals("child", doc.rootDoc().getBinaryValue("join_field").utf8ToString());
    }

    public void testMultipleLevels() throws Exception {
        DocumentMapper docMapper = createDocumentMapper(mapping(b -> {
            b.startObject("join_field");
            b.field("type", "join");
            b.startObject("relations").field("parent", "child").field("child", "grand_child").endObject();
            b.endObject();
        }));

        // Doc without join
        ParsedDocument doc = docMapper.parse(new SourceToParse("test", "0",
            BytesReference.bytes(XContentFactory.jsonBuilder().startObject().endObject()), XContentType.JSON));
        assertNull(doc.rootDoc().getBinaryValue("join_field"));

        // Doc parent
        doc = docMapper.parse(source(b -> b.field("join_field", "parent")));
        assertEquals("1", doc.rootDoc().getBinaryValue("join_field#parent").utf8ToString());
        assertEquals("parent", doc.rootDoc().getBinaryValue("join_field").utf8ToString());

        // Doc child
        doc = docMapper.parse(source("2", b -> b.startObject("join_field").field("name", "child").field("parent", "1").endObject(), "1"));
        assertEquals("1", doc.rootDoc().getBinaryValue("join_field#parent").utf8ToString());
        assertEquals("2", doc.rootDoc().getBinaryValue("join_field#child").utf8ToString());
        assertEquals("child", doc.rootDoc().getBinaryValue("join_field").utf8ToString());

        // Doc child missing parent
        MapperException exc = expectThrows(
            MapperParsingException.class,
            () -> docMapper.parse(source("2", b -> b.field("join_field", "child"), "1"))
        );
        assertThat(exc.getRootCause().getMessage(), containsString("[parent] is missing for join field [join_field]"));

        // Doc child missing routing
        exc = expectThrows(
            MapperParsingException.class,
            () -> docMapper.parse(source(b -> b.startObject("join_field").field("name", "child").field("parent", "1").endObject()))
        );
        assertThat(exc.getRootCause().getMessage(), containsString("[routing] is missing for join field [join_field]"));

        // Doc grand_child
        doc = docMapper.parse(
            source("3", b -> b.startObject("join_field").field("name", "grand_child").field("parent", "2").endObject(), "1")
        );
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
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("join_field");
            {
                b.field("type", "join");
                b.startObject("relations");
                {
                    b.field("parent", "child");
                    b.array("child", "grand_child1", "grand_child2");
                }
                b.endObject();
            }
            b.endObject();
        }));

        IllegalArgumentException exc = expectThrows(IllegalArgumentException.class, () -> merge(mapperService, mapping(b -> {
            b.startObject("join_field");
            {
                b.field("type", "join");
                b.startObject("relations");
                {
                    b.array("child", "grand_child1", "grand_child2");
                }
                b.endObject();
            }
            b.endObject();
        })));
        assertThat(exc.getMessage(), containsString("cannot remove parent [parent] in join field [join_field]"));

        exc = expectThrows(IllegalArgumentException.class, () -> merge(mapperService, mapping(b -> {
            b.startObject("join_field");
            {
                b.field("type", "join");
                b.startObject("relations");
                {
                    b.field("parent", "child");
                    b.array("child", "grand_child1");
                }
                b.endObject();
            }
            b.endObject();
        })));
        assertThat(exc.getMessage(), containsString("cannot remove child [grand_child2] in join field [join_field]"));

        exc = expectThrows(IllegalArgumentException.class, () -> merge(mapperService, mapping(b -> {
            b.startObject("join_field");
            {
                b.field("type", "join");
                b.startObject("relations");
                {
                    b.field("uber_parent", "parent");
                    b.field("parent", "child");
                    b.array("child", "grand_child1", "grand_child2");
                }
                b.endObject();
            }
            b.endObject();
        })));
        assertThat(exc.getMessage(), containsString("cannot create child [parent] from an existing parent"));

        exc = expectThrows(IllegalArgumentException.class, () -> merge(mapperService, mapping(b -> {
            b.startObject("join_field");
            {
                b.field("type", "join");
                b.startObject("relations");
                {
                    b.field("parent", "child");
                    b.array("child", "grand_child1", "grand_child2");
                    b.field("grand_child2", "grand_grand_child");
                }
                b.endObject();
            }
            b.endObject();
        })));
        assertThat(exc.getMessage(), containsString("cannot create parent [grand_child2] from an existing child]"));

        merge(mapperService, mapping(b -> {
            b.startObject("join_field");
            {
                b.field("type", "join");
                b.startObject("relations");
                {
                    b.array("parent", "child", "child2");
                    b.array("child", "grand_child1", "grand_child2");
                }
                b.endObject();
            }
            b.endObject();
        }));
        ParentJoinFieldMapper mapper = ParentJoinFieldMapper.getMapper(
            mapperService::fieldType,
            mapperService.documentMapper().mappers()::getMapper
        );
        assertNotNull(mapper);
        assertEquals("join_field", mapper.name());
        assertTrue(mapper.hasChild("child2"));
        assertFalse(mapper.hasParent("child2"));
        assertTrue(mapper.hasChild("grand_child2"));
        assertFalse(mapper.hasParent("grand_child2"));

        merge(mapperService, mapping(b -> {
            b.startObject("join_field");
            {
                b.field("type", "join");
                b.startObject("relations");
                {
                    b.array("parent", "child", "child2");
                    b.array("child", "grand_child1", "grand_child2");
                    b.array("other", "child_other1", "child_other2");
                }
                b.endObject();
            }
            b.endObject();
        }));
        mapper = ParentJoinFieldMapper.getMapper(
            mapperService::fieldType,
            mapperService.documentMapper().mappers()::getMapper
        );
        assertNotNull(mapper);
        assertEquals("join_field", mapper.name());
        assertTrue(mapper.hasParent("other"));
        assertFalse(mapper.hasChild("other"));
        assertTrue(mapper.hasChild("child_other1"));
        assertFalse(mapper.hasParent("child_other1"));
        assertTrue(mapper.hasChild("child_other2"));
        assertFalse(mapper.hasParent("child_other2"));
    }

    public void testInvalidJoinFieldInsideObject() throws Exception {
        MapperParsingException exc = expectThrows(MapperParsingException.class, () -> createMapperService(mapping(b -> {
            b.startObject("object");
            {
                b.startObject("properties");
                {
                    b.startObject("join_field");
                    b.field("type", "join").startObject("relations").field("parent", "child").endObject();
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        })));
        assertThat(exc.getRootCause().getMessage(),
            containsString("join field [object.join_field] cannot be added inside an object or in a multi-field"));
    }

    public void testInvalidJoinFieldInsideMultiFields() throws Exception {
        MapperParsingException exc = expectThrows(MapperParsingException.class, () -> createMapperService(mapping(b -> {
            b.startObject("number");
            {
                b.field("type", "integer");
                b.startObject("fields");
                {
                    b.startObject("join_field");
                    {
                        b.field("type", "join").startObject("relations").field("parent", "child").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        })));
        assertThat(
            exc.getRootCause().getMessage(),
            containsString("join field [number.join_field] cannot be added inside an object or in a multi-field")
        );
    }

    public void testMultipleJoinFields() throws Exception {
        {
            MapperParsingException exc = expectThrows(MapperParsingException.class, () -> createMapperService(mapping(b -> {
                b.startObject("join_field");
                b.field("type", "join");
                b.startObject("relations").field("parent", "child").field("child", "grand_child").endObject();
                b.endObject();
                b.startObject("another_join_field");
                b.field("type", "join");
                b.startObject("relations").field("product", "item").endObject().endObject();
            })));
            assertThat(exc.getMessage(), containsString("Field [_parent_join] is defined more than once"));
        }

        {
            MapperService mapperService = createMapperService(mapping(b -> {
                b.startObject("join_field");
                b.field("type", "join");
                b.startObject("relations").field("parent", "child").field("child", "grand_child").endObject();
                b.endObject();
            }));
            // Updating the mapping with another join field also fails
            MapperParsingException exc = expectThrows(
                MapperParsingException.class,
                () -> merge(mapperService, mapping(b -> b.startObject("another_join_field").field("type", "join").endObject()))
            );
            assertThat(exc.getMessage(), containsString("Field [_parent_join] is defined more than once"));
        }
    }

    public void testEagerGlobalOrdinals() throws Exception {
        MapperService mapperService = createMapperService(mapping(b -> b
                .startObject("join_field")
                    .field("type", "join")
                    .startObject("relations")
                        .field("parent", "child")
                        .field("child", "grand_child")
                    .endObject()
                .endObject()));
        assertFalse(mapperService.fieldType("join_field").eagerGlobalOrdinals());
        assertNotNull(mapperService.fieldType("join_field#parent"));
        assertTrue(mapperService.fieldType("join_field#parent").eagerGlobalOrdinals());
        assertNotNull(mapperService.fieldType("join_field#child"));
        assertTrue(mapperService.fieldType("join_field#child").eagerGlobalOrdinals());

        merge(mapperService, mapping(b -> b
                .startObject("join_field")
                    .field("type", "join")
                    .field("eager_global_ordinals", false)
                    .startObject("relations")
                        .field("parent", "child")
                        .field("child", "grand_child")
                    .endObject()
                .endObject()));
        assertFalse(mapperService.fieldType("join_field").eagerGlobalOrdinals());
        assertNotNull(mapperService.fieldType("join_field#parent"));
        assertFalse(mapperService.fieldType("join_field#parent").eagerGlobalOrdinals());
        assertNotNull(mapperService.fieldType("join_field#child"));
        assertFalse(mapperService.fieldType("join_field#child").eagerGlobalOrdinals());
    }
}

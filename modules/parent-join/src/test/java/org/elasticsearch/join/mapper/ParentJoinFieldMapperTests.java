/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.join.mapper;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperException;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.join.ParentJoinPlugin;
import org.elasticsearch.plugins.Plugin;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

import static java.util.Collections.singleton;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

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
        Joiner joiner = Joiner.getJoiner(mapperService.mappingLookup().getMatchingFieldNames("*").stream()
            .map(mapperService.mappingLookup()::getFieldType));
        assertNotNull(joiner);
        assertEquals("join_field", joiner.getJoinField());

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
        MapperParsingException exc = expectThrows(
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
        assertThat(exc.getMessage(), containsString("Cannot remove parent [parent]"));

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
        assertThat(exc.getMessage(), containsString("Cannot remove child [grand_child2]"));

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
        assertThat(exc.getMessage(), containsString("Cannot create child [parent] from an existing root"));

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
        assertThat(exc.getMessage(), containsString("Cannot create parent [grand_child2] from an existing child"));

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

        Joiner joiner = Joiner.getJoiner(mapperService.mappingLookup().getMatchingFieldNames("*").stream()
            .map(mapperService.mappingLookup()::getFieldType));
        assertNotNull(joiner);
        assertEquals("join_field", joiner.getJoinField());
        assertTrue(joiner.childTypeExists("child2"));
        assertFalse(joiner.parentTypeExists("child2"));
        assertEquals(new TermQuery(new Term("join_field", "parent")), joiner.parentFilter("child"));
        assertEquals(new TermQuery(new Term("join_field", "parent")), joiner.parentFilter("child2"));
        assertTrue(joiner.childTypeExists("grand_child2"));
        assertFalse(joiner.parentTypeExists("grand_child2"));
        assertEquals(new TermQuery(new Term("join_field", "child")), joiner.parentFilter("grand_child1"));
        assertEquals(new TermQuery(new Term("join_field", "child")), joiner.parentFilter("grand_child2"));

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
        joiner = Joiner.getJoiner(mapperService.mappingLookup().getMatchingFieldNames("*").stream()
            .map(mapperService.mappingLookup()::getFieldType));
        assertNotNull(joiner);
        assertEquals("join_field", joiner.getJoinField());
        assertTrue(joiner.childTypeExists("child2"));
        assertFalse(joiner.parentTypeExists("child2"));
        assertEquals(new TermQuery(new Term("join_field", "parent")), joiner.parentFilter("child"));
        assertEquals(new TermQuery(new Term("join_field", "parent")), joiner.parentFilter("child2"));
        assertTrue(joiner.childTypeExists("grand_child2"));
        assertFalse(joiner.parentTypeExists("grand_child2"));
        assertEquals(new TermQuery(new Term("join_field", "child")), joiner.parentFilter("grand_child1"));
        assertEquals(new TermQuery(new Term("join_field", "child")), joiner.parentFilter("grand_child2"));
        assertTrue(joiner.parentTypeExists("other"));
        assertFalse(joiner.childTypeExists("other"));
        assertTrue(joiner.childTypeExists("child_other1"));
        assertFalse(joiner.parentTypeExists("child_other1"));
        assertTrue(joiner.childTypeExists("child_other2"));
        assertFalse(joiner.parentTypeExists("child_other2"));
        assertEquals(new TermQuery(new Term("join_field", "other")), joiner.parentFilter("child_other2"));
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
            IllegalArgumentException exc = expectThrows(IllegalArgumentException.class, () -> createMapperService(mapping(b -> {
                b.startObject("join_field");
                b.field("type", "join");
                b.startObject("relations").field("parent", "child").field("child", "grand_child").endObject();
                b.endObject();
                b.startObject("another_join_field");
                b.field("type", "join");
                b.startObject("relations").field("product", "item").endObject().endObject();
            })));
            assertThat(exc.getMessage(),
                equalTo("Only one [parent-join] field can be defined per index, got [join_field, another_join_field]"));
        }

        {
            MapperService mapperService = createMapperService(mapping(b -> {
                b.startObject("join_field");
                b.field("type", "join");
                b.startObject("relations").field("parent", "child").field("child", "grand_child").endObject();
                b.endObject();
            }));
            // Updating the mapping with another join field also fails
            IllegalArgumentException exc = expectThrows(
                IllegalArgumentException.class,
                () -> merge(mapperService, mapping(b -> b.startObject("another_join_field").field("type", "join").endObject()))
            );
            assertThat(exc.getMessage(),
                equalTo("Only one [parent-join] field can be defined per index, got [join_field, another_join_field]"));
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

    public void testSubFields() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> b
            .startObject("join_field")
                .field("type", "join")
                .startObject("relations")
                    .field("parent", "child")
                    .field("child", "grand_child")
                .endObject()
            .endObject()));
        ParentJoinFieldMapper mapper = (ParentJoinFieldMapper) mapperService.mappingLookup().getMapper("join_field");
        assertTrue(mapper.fieldType().isSearchable());
        assertTrue(mapper.fieldType().isAggregatable());

        Iterator<Mapper> it = mapper.iterator();
        FieldMapper next = (FieldMapper) it.next();
        assertThat(next.name(), equalTo("join_field#parent"));
        assertTrue(next.fieldType().isSearchable());
        assertTrue(next.fieldType().isAggregatable());

        assertTrue(it.hasNext());
        next = (FieldMapper) it.next();
        assertThat(next.name(), equalTo("join_field#child"));
        assertTrue(next.fieldType().isSearchable());
        assertTrue(next.fieldType().isAggregatable());

        assertFalse(it.hasNext());
    }
}

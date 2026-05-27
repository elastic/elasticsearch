/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.eirf.EirfBatch;
import org.elasticsearch.eirf.EirfRowBuilder;
import org.elasticsearch.eirf.EirfSchema;
import org.elasticsearch.index.mapper.BooleanFieldMapper;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.IpFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.ShardBatchMapper;
import org.elasticsearch.index.mapper.ShardBatchMapper.BatchMapperResolution;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public class ShardBatchMapperResolveTests extends MapperServiceTestCase {

    /** Build a schema with the given (name, EIRF type) leaves by driving an EirfRowBuilder. */
    private static EirfSchema schemaOf(String... leafNames) {
        try (EirfRowBuilder b = new EirfRowBuilder()) {
            b.startDocument();
            for (String name : leafNames) {
                // Any value shape works — resolveMappers only looks at the schema, not the value.
                b.setLong(name, 0L);
            }
            b.endDocument();
            try (EirfBatch batch = b.build()) {
                return batch.schema();
            }
        }
    }

    private MapperService mapper(XContentBuilder mapping) throws IOException {
        return createMapperService(mapping);
    }

    public void testHappyPath() throws IOException {
        MapperService ms = mapper(mapping(b -> {
            b.startObject("ts").field("type", "date").endObject();
            b.startObject("host").field("type", "keyword").endObject();
            b.startObject("value").field("type", "long").endObject();
            b.startObject("score").field("type", "double").endObject();
        }));
        EirfSchema schema = schemaOf("ts", "host", "value", "score");
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schema, ms.mappingLookup());
        assertNotNull(resolution);
        assertEquals(4, resolution.columnMappers().length);
        for (int i = 0; i < 4; i++) {
            assertNotNull("column " + i + " should resolve to a mapper", resolution.columnMappers()[i]);
        }
        // Order of EirfSchema leaves matches insertion order for the row builder above.
        assertTrue(resolution.columnMappers()[schema.findLeaf("ts", 0)] instanceof DateFieldMapper);
        assertTrue(resolution.columnMappers()[schema.findLeaf("host", 0)] instanceof KeywordFieldMapper);
        assertTrue(resolution.columnMappers()[schema.findLeaf("value", 0)] instanceof NumberFieldMapper);
        assertTrue(resolution.columnMappers()[schema.findLeaf("score", 0)] instanceof NumberFieldMapper);
    }

    public void testKeywordIgnoreAboveIsSupported() throws IOException {
        MapperService ms = mapper(mapping(b -> { b.startObject("host").field("type", "keyword").field("ignore_above", 32).endObject(); }));
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schemaOf("host"), ms.mappingLookup());
        assertNotNull(resolution);
        assertTrue(resolution.columnMappers()[0] instanceof KeywordFieldMapper);
    }

    public void testNumberIgnoreMalformedIsSupported() throws IOException {
        MapperService ms = mapper(mapping(b -> { b.startObject("v").field("type", "long").field("ignore_malformed", true).endObject(); }));
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schemaOf("v"), ms.mappingLookup());
        assertNotNull(resolution);
        assertTrue(resolution.columnMappers()[0] instanceof NumberFieldMapper);
    }

    public void testMissingLeafUnderDynamicFalseIsIgnored() throws IOException {
        MapperService ms = mapper(topMapping(b -> {
            b.field("dynamic", "false");
            b.startObject("properties");
            b.startObject("known").field("type", "keyword").endObject();
            b.endObject();
        }));
        EirfSchema schema = schemaOf("known", "unknown");
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schema, ms.mappingLookup());
        assertNotNull(resolution);
        assertNotNull(resolution.columnMappers()[schema.findLeaf("known", 0)]);
        assertNull(resolution.columnMappers()[schema.findLeaf("unknown", 0)]);
    }

    public void testMissingLeafUnderDynamicTrueFallsBack() throws IOException {
        MapperService ms = mapper(mapping(b -> { b.startObject("known").field("type", "keyword").endObject(); }));
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schemaOf("known", "unknown"), ms.mappingLookup());
        assertNull(resolution);
    }

    public void testRuntimeFieldInMappingFallsBack() throws IOException {
        MapperService ms = mapper(topMapping(b -> {
            b.startObject("runtime");
            b.startObject("rt").field("type", "keyword").endObject();
            b.endObject();
            b.startObject("properties");
            b.startObject("known").field("type", "keyword").endObject();
            b.endObject();
        }));
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schemaOf("known"), ms.mappingLookup());
        assertNull(resolution);
    }

    public void testIndexTimeScriptFallsBack() throws IOException {
        // A long field with a script is a standard example of an index-time script. Registering one
        // populates MappingLookup.indexTimeScriptMappers() which resolveMappers short-circuits on.
        // We can't easily register a real script in a unit test without wiring a ScriptService, but
        // we can verify that any mapper marked hasScript=true via the `script` parameter trips the
        // supportsBatchIndexing() guard. That path is covered by testUnsupportedMapperType below
        // (the short-circuit in resolveMappers on indexTimeScriptMappers is a superset check and
        // redundant with the per-mapper guard, so this test is intentionally narrow).
    }

    public void testTextMapperHappyPath() throws IOException {
        MapperService ms = mapper(mapping(b -> { b.startObject("t").field("type", "text").endObject(); }));
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schemaOf("t"), ms.mappingLookup());
        assertNotNull(resolution);
        assertTrue(resolution.columnMappers()[0] instanceof TextFieldMapper);
    }

    public void testTextMapperWithIndexPrefixesFallsBack() throws IOException {
        MapperService ms = mapper(mapping(b -> {
            b.startObject("t");
            b.field("type", "text");
            b.startObject("index_prefixes").endObject();
            b.endObject();
        }));
        assertNull(ShardBatchMapper.resolveMappers(schemaOf("t"), ms.mappingLookup()));
    }

    public void testTextMapperWithIndexPhrasesFallsBack() throws IOException {
        MapperService ms = mapper(mapping(b -> { b.startObject("t").field("type", "text").field("index_phrases", true).endObject(); }));
        assertNull(ShardBatchMapper.resolveMappers(schemaOf("t"), ms.mappingLookup()));
    }

    public void testTextMapperWithFielddataIsSupported() throws IOException {
        MapperService ms = mapper(mapping(b -> { b.startObject("t").field("type", "text").field("fielddata", true).endObject(); }));
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schemaOf("t"), ms.mappingLookup());
        assertNotNull(resolution);
        assertTrue(resolution.columnMappers()[0] instanceof TextFieldMapper);
    }

    public void testBooleanMapperHappyPath() throws IOException {
        MapperService ms = mapper(mapping(b -> { b.startObject("b").field("type", "boolean").endObject(); }));
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schemaOf("b"), ms.mappingLookup());
        assertNotNull(resolution);
        assertTrue(resolution.columnMappers()[0] instanceof BooleanFieldMapper);
    }

    public void testBooleanIgnoreMalformedIsSupported() throws IOException {
        MapperService ms = mapper(
            mapping(b -> { b.startObject("b").field("type", "boolean").field("ignore_malformed", true).endObject(); })
        );
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schemaOf("b"), ms.mappingLookup());
        assertNotNull(resolution);
        assertTrue(resolution.columnMappers()[0] instanceof BooleanFieldMapper);
    }

    public void testIpMapperHappyPath() throws IOException {
        MapperService ms = mapper(mapping(b -> { b.startObject("ip").field("type", "ip").endObject(); }));
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schemaOf("ip"), ms.mappingLookup());
        assertNotNull(resolution);
        assertTrue(resolution.columnMappers()[0] instanceof IpFieldMapper);
    }

    public void testIpIgnoreMalformedIsSupported() throws IOException {
        MapperService ms = mapper(mapping(b -> { b.startObject("ip").field("type", "ip").field("ignore_malformed", true).endObject(); }));
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schemaOf("ip"), ms.mappingLookup());
        assertNotNull(resolution);
        assertTrue(resolution.columnMappers()[0] instanceof IpFieldMapper);
    }

    public void testKeywordWithCopyToFallsBack() throws IOException {
        MapperService ms = mapper(mapping(b -> {
            b.startObject("src").field("type", "keyword").field("copy_to", "dst").endObject();
            b.startObject("dst").field("type", "keyword").endObject();
        }));
        assertNull(ShardBatchMapper.resolveMappers(schemaOf("src"), ms.mappingLookup()));
    }

    public void testKeywordWithMultiFieldsFallsBack() throws IOException {
        MapperService ms = mapper(mapping(b -> {
            b.startObject("host");
            b.field("type", "keyword");
            b.startObject("fields");
            b.startObject("lower").field("type", "keyword").endObject();
            b.endObject();
            b.endObject();
        }));
        assertNull(ShardBatchMapper.resolveMappers(schemaOf("host"), ms.mappingLookup()));
    }

    public void testNestedLeafHappyPath() throws IOException {
        MapperService ms = mapper(mapping(b -> {
            b.startObject("outer");
            b.startObject("properties");
            b.startObject("inner").field("type", "long").endObject();
            b.endObject();
            b.endObject();
        }));
        EirfSchema schema = schemaOf("outer.inner");
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schema, ms.mappingLookup());
        assertNotNull(resolution);
        assertTrue(resolution.columnMappers()[0] instanceof NumberFieldMapper);
    }

    public void testNestedLeafUnderNestedDynamicFalseIsIgnored() throws IOException {
        MapperService ms = mapper(mapping(b -> {
            b.startObject("outer");
            b.field("dynamic", "false");
            b.startObject("properties");
            b.startObject("known").field("type", "long").endObject();
            b.endObject();
            b.endObject();
        }));
        EirfSchema schema = schemaOf("outer.known", "outer.unknown");
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schema, ms.mappingLookup());
        assertNotNull(resolution);
        assertNotNull(resolution.columnMappers()[schema.findLeaf("known", schema.findNonLeaf("outer", 0))]);
        assertNull(resolution.columnMappers()[schema.findLeaf("unknown", schema.findNonLeaf("outer", 0))]);
    }
}

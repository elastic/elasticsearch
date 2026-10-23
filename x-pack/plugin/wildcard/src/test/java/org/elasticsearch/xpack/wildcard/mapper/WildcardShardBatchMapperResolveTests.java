/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.wildcard.mapper;

import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.AbstractShardBatchMapperResolveTestCase;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ShardBatchMapper;
import org.elasticsearch.index.mapper.ShardBatchMapper.BatchMapperResolution;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.wildcard.Wildcard;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * Verifies that {@link ShardBatchMapper#resolveMappers} correctly recognizes {@link WildcardFieldMapper}
 * as columnar-batch eligible (or not).
 *
 * @see AbstractShardBatchMapperResolveTestCase
 */
public class WildcardShardBatchMapperResolveTests extends AbstractShardBatchMapperResolveTestCase {

    @Override
    protected Collection<Plugin> getPlugins() {
        return List.of(new Wildcard());
    }

    public void testWildcardMapperIsSupported() throws IOException {
        MapperService ms = columnarMapperService(mapping(b -> { b.startObject("f").field("type", "wildcard").endObject(); }));
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schemaOf("f"), ms.mappingLookup(), indexSettings);
        assertNotNull(resolution);
        assertTrue(resolution.columnMappers()[0] instanceof WildcardFieldMapper);
    }

    public void testWildcardWithKeywordSubFieldResolves() throws IOException {
        MapperService ms = columnarMapperService(mapping(b -> {
            b.startObject("f");
            b.field("type", "wildcard");
            b.startObject("fields");
            b.startObject("raw").field("type", "keyword").endObject();
            b.endObject();
            b.endObject();
        }));
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schemaOf("f"), ms.mappingLookup(), indexSettings);
        assertNotNull("wildcard with a keyword sub-field must take the columnar fast path", resolution);
        assertTrue(resolution.columnMappers()[0] instanceof WildcardFieldMapper);
    }

    public void testKeywordParentWithWildcardSubFieldResolves() throws IOException {
        MapperService ms = columnarMapperService(mapping(b -> {
            b.startObject("f");
            b.field("type", "keyword");
            b.startObject("fields");
            b.startObject("wc").field("type", "wildcard").endObject();
            b.endObject();
            b.endObject();
        }));
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schemaOf("f"), ms.mappingLookup(), indexSettings);
        assertNotNull("keyword parent with wildcard sub-field must take the columnar fast path", resolution);
    }

    public void testWildcardNonColumnarModeFallsBack() throws IOException {
        MapperService ms = createMapperService(mapping(b -> b.startObject("f").field("type", "wildcard").endObject()));
        IndexSettings nonColumnar = ms.getIndexSettings();
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schemaOf("f"), ms.mappingLookup(), nonColumnar);
        assertNull(resolution);
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.extras;

import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.AbstractShardBatchMapperResolveTestCase;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ShardBatchMapper;
import org.elasticsearch.index.mapper.ShardBatchMapper.BatchMapperResolution;
import org.elasticsearch.plugins.Plugin;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * Verifies that {@link ShardBatchMapper#resolveMappers} correctly recognizes {@link MatchOnlyTextFieldMapper}
 * as columnar-batch eligible (or not), mirroring the keyword coverage in
 * {@code org.elasticsearch.action.bulk.ShardBatchMapperResolveTests}. That test class lives in {@code server}'s
 * unit test source set, which has no dependency on {@code mapper-extras}, so match_only_text needs its own
 * equivalent here instead.
 *
 * @see AbstractShardBatchMapperResolveTestCase
 */
public class MatchOnlyTextShardBatchMapperResolveTests extends AbstractShardBatchMapperResolveTestCase {

    @Override
    protected Collection<Plugin> getPlugins() {
        return List.of(new MapperExtrasPlugin());
    }

    public void testMatchOnlyTextMapperIsSupported() throws IOException {
        MapperService ms = columnarMapperService(mapping(b -> { b.startObject("f").field("type", "match_only_text").endObject(); }));
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schemaOf("f"), ms.mappingLookup(), indexSettings);
        assertNotNull(resolution);
        assertTrue(resolution.columnMappers()[0] instanceof MatchOnlyTextFieldMapper);
    }

    public void testMatchOnlyTextMultiValueFalseIsSupported() throws IOException {
        MapperService ms = columnarMapperService(mapping(b -> {
            b.startObject("f").field("type", "match_only_text");
            b.startObject("doc_values").field("multi_value", false).endObject();
            b.endObject();
        }));
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schemaOf("f"), ms.mappingLookup(), indexSettings);
        assertNotNull(resolution);
        assertTrue(resolution.columnMappers()[0] instanceof MatchOnlyTextFieldMapper);
    }

    /** A keyword sub-field on a match_only_text parent: the base class fans out to sub-mappers, so both take the fast path. */
    public void testMatchOnlyTextWithMultiFieldsResolves() throws IOException {
        MapperService ms = columnarMapperService(mapping(b -> {
            b.startObject("f");
            b.field("type", "match_only_text");
            b.startObject("fields");
            b.startObject("raw").field("type", "keyword").endObject();
            b.endObject();
            b.endObject();
        }));
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schemaOf("f"), ms.mappingLookup(), indexSettings);
        assertNotNull("match_only_text with a keyword sub-field must take the columnar fast path", resolution);
        assertTrue(resolution.columnMappers()[0] instanceof MatchOnlyTextFieldMapper);
    }

    public void testMatchOnlyTextNonColumnarDocValuesFallsBack() throws IOException {
        MapperService ms = createMapperService(
            mapping(b -> b.startObject("f").field("type", "match_only_text").field("doc_values", true).endObject())
        );
        var rawMapper = ms.mappingLookup().getMapper("f");
        assertTrue(rawMapper instanceof MatchOnlyTextFieldMapper);
        assertFalse(((MatchOnlyTextFieldMapper) rawMapper).supportsColumnarParse(ms.getIndexSettings()));
    }

    public void testMatchOnlyTextDocValuesDisabledFallsBack() throws IOException {
        MapperService ms = columnarMapperService(mapping(b -> {
            b.startObject("f").field("type", "match_only_text").field("doc_values", false).endObject();
        }));
        assertNull(ShardBatchMapper.resolveMappers(schemaOf("f"), ms.mappingLookup(), indexSettings));
    }

    public void testMatchOnlyTextNonColumnarModeFallsBack() throws IOException {
        MapperService ms = createMapperService(mapping(b -> b.startObject("f").field("type", "match_only_text").endObject()));
        IndexSettings nonColumnar = ms.getIndexSettings();
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schemaOf("f"), ms.mappingLookup(), nonColumnar);
        assertNull(resolution);
    }
}

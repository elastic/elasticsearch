/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.search.IndexSearcher;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.MappedFieldType.Relation;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentFactory;

import static java.util.Collections.emptyMap;

// The purpose of this test case is to test RangeQueryBuilder.getRelation()
// Whether it should return INTERSECT/DISJOINT/WITHIN is already tested in
// RangeQueryBuilderTests
public class RangeQueryRewriteTests extends ESSingleNodeTestCase {

    public void testRewriteMissingField() throws Exception {
        IndexService indexService = createIndex("test");
        IndexReader reader = new MultiReader();
        SearchExecutionContext context = new SearchExecutionContext(
            0,
            0,
            indexService.getIndexSettings(),
            null,
            null,
            null,
            indexService.mapperService(),
            indexService.mapperService().mappingLookup(),
            null,
            null,
            parserConfig(),
            writableRegistry(),
            null,
            new IndexSearcher(reader),
            null,
            null,
            null,
            () -> true,
            null,
            emptyMap()
        );
        RangeQueryBuilder range = new RangeQueryBuilder("foo");
        assertEquals(Relation.DISJOINT, range.getRelation(context));
    }

    public void testRewriteMissingReader() throws Exception {
        IndexService indexService = createIndex("test");
        String mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("type")
                .startObject("properties")
                .startObject("foo")
                .field("type", "date")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );
        indexService.mapperService().merge("type", new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE);
        SearchExecutionContext context = new SearchExecutionContext(
            0,
            0,
            indexService.getIndexSettings(),
            null,
            null,
            null,
            indexService.mapperService(),
            indexService.mapperService().mappingLookup(),
            null,
            null,
            parserConfig(),
            writableRegistry(),
            null,
            null,
            null,
            null,
            null,
            () -> true,
            null,
            emptyMap()
        );
        RangeQueryBuilder range = new RangeQueryBuilder("foo");
        // can't make assumptions on a missing reader, so it must return INTERSECT
        assertEquals(Relation.INTERSECTS, range.getRelation(context));
    }

    public void testRewriteEmptyReader() throws Exception {
        IndexService indexService = createIndex("test");
        String mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("type")
                .startObject("properties")
                .startObject("foo")
                .field("type", "date")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );
        indexService.mapperService().merge("type", new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE);
        IndexReader reader = new MultiReader();
        SearchExecutionContext context = new SearchExecutionContext(
            0,
            0,
            indexService.getIndexSettings(),
            null,
            null,
            null,
            indexService.mapperService(),
            indexService.mapperService().mappingLookup(),
            null,
            null,
            parserConfig(),
            writableRegistry(),
            null,
            new IndexSearcher(reader),
            null,
            null,
            null,
            () -> true,
            null,
            emptyMap()
        );
        RangeQueryBuilder range = new RangeQueryBuilder("foo");
        // no values -> DISJOINT
        assertEquals(Relation.DISJOINT, range.getRelation(context));
    }
}

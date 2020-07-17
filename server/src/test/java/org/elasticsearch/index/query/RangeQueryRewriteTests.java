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

package org.elasticsearch.index.query;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.search.IndexSearcher;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.MappedFieldType.Relation;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.test.ESSingleNodeTestCase;

// The purpose of this test case is to test RangeQueryBuilder.getRelation()
// Whether it should return INTERSECT/DISJOINT/WITHIN is already tested in
// RangeQueryBuilderTests
public class RangeQueryRewriteTests extends ESSingleNodeTestCase {

    public void testRewriteMissingField() throws Exception {
        IndexService indexService = createIndex("test");
        IndexReader reader = new MultiReader();
        QueryRewriteContext context = new QueryShardContext(0, indexService.getIndexSettings(), BigArrays.NON_RECYCLING_INSTANCE,
            null, null, indexService.mapperService(), null, null, xContentRegistry(), writableRegistry(),
            null, new IndexSearcher(reader), null, null, null, () -> true, null);
        RangeQueryBuilder range = new RangeQueryBuilder("foo");
        assertEquals(Relation.DISJOINT, range.getRelation(context));
    }

    public void testRewriteMissingReader() throws Exception {
        IndexService indexService = createIndex("test");
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                    .startObject("foo")
                        .field("type", "date")
                    .endObject()
                .endObject()
            .endObject().endObject());
        indexService.mapperService().merge("type",
                new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE);
        QueryRewriteContext context = new QueryShardContext(0, indexService.getIndexSettings(), null, null, null,
                indexService.mapperService(), null, null, xContentRegistry(), writableRegistry(),
                null, null, null, null, null, () -> true, null);
        RangeQueryBuilder range = new RangeQueryBuilder("foo");
        // can't make assumptions on a missing reader, so it must return INTERSECT
        assertEquals(Relation.INTERSECTS, range.getRelation(context));
    }

    public void testRewriteEmptyReader() throws Exception {
        IndexService indexService = createIndex("test");
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                    .startObject("foo")
                        .field("type", "date")
                    .endObject()
                .endObject()
            .endObject().endObject());
        indexService.mapperService().merge("type",
                new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE);
        IndexReader reader = new MultiReader();
        QueryRewriteContext context = new QueryShardContext(0, indexService.getIndexSettings(), BigArrays.NON_RECYCLING_INSTANCE,
            null, null, indexService.mapperService(), null, null, xContentRegistry(), writableRegistry(),
                null, new IndexSearcher(reader), null, null, null, () -> true, null);
        RangeQueryBuilder range = new RangeQueryBuilder("foo");
        // no values -> DISJOINT
        assertEquals(Relation.DISJOINT, range.getRelation(context));
    }
}

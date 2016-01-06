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

package org.elasticsearch.index.search;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;

import static org.elasticsearch.index.query.QueryBuilders.multiMatchQuery;

public class MultiMatchQueryTests extends ESSingleNodeTestCase {

    private IndexService indexService;

    @Before
    public void setup() throws IOException {
        IndexService indexService = createIndex("test");
        MapperService mapperService = indexService.mapperService();
        String mapping = "{\n" +
                "    \"person\":{\n" +
                "        \"properties\":{\n" +
                "            \"name\":{\n" +
                "                  \"properties\":{\n" +
                "                        \"first\": {\n" +
                "                            \"type\":\"string\"\n" +
                "                        }," +
                "                        \"last\": {\n" +
                "                            \"type\":\"string\"\n" +
                "                        }" +
                "                   }" +
                "            }\n" +
                "        }\n" +
                "    }\n" +
                "}";
        mapperService.merge("person", new CompressedXContent(mapping), true, false);
        this.indexService = indexService;
    }

    public void testCrossFieldMultiMatchQuery() throws IOException {
        QueryShardContext queryShardContext = indexService.getShard(0).getQueryShardContext();
        queryShardContext.setAllowUnmappedFields(true);
        Query parsedQuery = multiMatchQuery("banon").field("name.first", 2).field("name.last", 3).field("foobar").type(MultiMatchQueryBuilder.Type.CROSS_FIELDS).toQuery(queryShardContext);
        try (Engine.Searcher searcher = indexService.getShard(0).acquireSearcher("test")) {
            Query rewrittenQuery = searcher.searcher().rewrite(parsedQuery);

            BooleanQuery.Builder expected = new BooleanQuery.Builder();
            expected.add(new TermQuery(new Term("foobar", "banon")), BooleanClause.Occur.SHOULD);
            Query tq1 = new BoostQuery(new TermQuery(new Term("name.first", "banon")), 2);
            Query tq2 = new BoostQuery(new TermQuery(new Term("name.last", "banon")), 3);
            expected.add(new DisjunctionMaxQuery(Arrays.<Query>asList(tq1, tq2), 0f), BooleanClause.Occur.SHOULD);
            assertEquals(expected.build(), rewrittenQuery);
        }
    }
}

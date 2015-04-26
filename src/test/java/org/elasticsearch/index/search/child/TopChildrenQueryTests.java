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

package org.elasticsearch.index.search.child;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryUtils;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.fielddata.plain.ParentChildIndexFieldData;
import org.elasticsearch.index.mapper.internal.ParentFieldMapper;
import org.elasticsearch.search.internal.SearchContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

/**
 */
public class TopChildrenQueryTests extends AbstractChildTests {

    @BeforeClass
    public static void before() throws IOException {
        SearchContext.setCurrent(ChildrenConstantScoreQueryTests.createSearchContext("test", "parent", "child"));
    }

    @AfterClass
    public static void after() throws IOException {
        SearchContext current = SearchContext.current();
        SearchContext.removeCurrent();
        Releasables.close(current);
    }

    @Test
    public void testBasicQuerySanities() {
        Query childQuery = new TermQuery(new Term("field", "value"));
        ScoreType scoreType = ScoreType.values()[random().nextInt(ScoreType.values().length)];
        ParentFieldMapper parentFieldMapper = SearchContext.current().mapperService().documentMapper("child").parentFieldMapper();
        ParentChildIndexFieldData parentChildIndexFieldData = SearchContext.current().fieldData().getForField(parentFieldMapper);
        Query query = new TopChildrenQuery(parentChildIndexFieldData, childQuery, "child", "parent", scoreType, 1, 1, wrapWithBitSetFilter(Queries.newNonNestedFilter()));
        QueryUtils.check(query);
    }

}

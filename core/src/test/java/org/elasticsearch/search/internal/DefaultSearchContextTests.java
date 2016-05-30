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

package org.elasticsearch.search.internal;

import org.apache.lucene.queries.TermsQuery;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.mapper.internal.TypeFieldMapper;
import org.elasticsearch.test.ESTestCase;

import static org.apache.lucene.search.BooleanClause.Occur.FILTER;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class DefaultSearchContextTests extends ESTestCase {

    public void testCreateSearchFilter() {
        Query searchFilter = DefaultSearchContext.createSearchFilter(new String[]{"type1", "type2"}, null, randomBoolean());
        Query expectedQuery = new BooleanQuery.Builder()
            .add(new TermsQuery(TypeFieldMapper.NAME, new BytesRef("type1"), new BytesRef("type2")), FILTER)
            .build();
        assertThat(searchFilter, equalTo(expectedQuery));

        searchFilter = DefaultSearchContext.createSearchFilter(new String[]{"type1", "type2"}, new MatchAllDocsQuery(), randomBoolean());
        expectedQuery = new BooleanQuery.Builder()
            .add(new TermsQuery(TypeFieldMapper.NAME, new BytesRef("type1"), new BytesRef("type2")), FILTER)
            .add(new MatchAllDocsQuery(), FILTER)
            .build();
        assertThat(searchFilter, equalTo(expectedQuery));

        searchFilter = DefaultSearchContext.createSearchFilter(null, null, false);
        assertThat(searchFilter, nullValue());

        searchFilter = DefaultSearchContext.createSearchFilter(null, null, true);
        expectedQuery = new BooleanQuery.Builder().add(Queries.newNonNestedFilter(), FILTER).build();
        assertThat(searchFilter, equalTo(expectedQuery));

        searchFilter = DefaultSearchContext.createSearchFilter(null, new MatchAllDocsQuery(), true);
        expectedQuery = new BooleanQuery.Builder()
            .add(new MatchAllDocsQuery(), FILTER)
            .add(Queries.newNonNestedFilter(), FILTER)
            .build();
        assertThat(searchFilter, equalTo(expectedQuery));

        searchFilter = DefaultSearchContext.createSearchFilter(null, new MatchAllDocsQuery(), false);
        expectedQuery = new BooleanQuery.Builder()
            .add(new MatchAllDocsQuery(), FILTER)
            .build();
        assertThat(searchFilter, equalTo(expectedQuery));
    }

}

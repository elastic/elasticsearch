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
package org.elasticsearch.index.mapper;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.test.ESTestCase;
import org.mockito.Mockito;

public class TypeFieldTypeTests extends ESTestCase {

    public void testTermsQuery() {
        QueryShardContext context = Mockito.mock(QueryShardContext.class);

        TypeFieldMapper.TypeFieldType ft = TypeFieldMapper.TypeFieldType.INSTANCE;

        Query query = ft.termQuery("_doc", context);
        assertEquals(new MatchAllDocsQuery(), query);

        query = ft.termQuery("other_type", context);
        assertEquals(new MatchNoDocsQuery(), query);
    }
}

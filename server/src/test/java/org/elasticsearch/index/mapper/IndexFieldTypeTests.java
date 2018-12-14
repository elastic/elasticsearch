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

import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.QueryShardContext;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IndexFieldTypeTests extends FieldTypeTestCase {

    @Override
    protected MappedFieldType createDefaultFieldType() {
        return new IndexFieldMapper.IndexFieldType();
    }

    public void testPrefixQuery() {
        MappedFieldType ft = createDefaultFieldType();
        ft.setName("field");
        ft.setIndexOptions(IndexOptions.DOCS);

        assertEquals(new MatchAllDocsQuery(), ft.prefixQuery("ind", null, createContext()));
        assertEquals(new MatchNoDocsQuery(), ft.prefixQuery("other_ind", null, createContext()));
    }

    public void testRegexpQuery() {
        MappedFieldType ft = createDefaultFieldType();
        ft.setName("field");
        ft.setIndexOptions(IndexOptions.DOCS);

        assertEquals(new MatchAllDocsQuery(), ft.regexpQuery("ind.x", 0, 10, null, createContext()));
        assertEquals(new MatchNoDocsQuery(), ft.regexpQuery("ind?x", 0, 10, null, createContext()));
    }

    public void testWildcardQuery() {
        MappedFieldType ft = createDefaultFieldType();
        ft.setName("field");
        ft.setIndexOptions(IndexOptions.DOCS);

        assertEquals(new MatchAllDocsQuery(), ft.wildcardQuery("ind*x", null, createContext()));
        assertEquals(new MatchNoDocsQuery(), ft.wildcardQuery("other_ind*x", null, createContext()));
    }

    private QueryShardContext createContext() {
        QueryShardContext context = mock(QueryShardContext.class);

        Index index = new Index("index", "123");
        when(context.getFullyQualifiedIndex()).thenReturn(index);
        when(context.index()).thenReturn(index);

        return context;
    }
}

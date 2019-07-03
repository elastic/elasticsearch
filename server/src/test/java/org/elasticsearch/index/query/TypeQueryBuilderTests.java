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

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.TypeFieldMapper;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.AbstractQueryTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;


public class TypeQueryBuilderTests extends AbstractQueryTestCase<TypeQueryBuilder> {

    @Override
    protected TypeQueryBuilder doCreateTestQueryBuilder() {
        return new TypeQueryBuilder("_doc");
    }

    @Override
    protected void doAssertLuceneQuery(TypeQueryBuilder queryBuilder, Query query, SearchContext context) throws IOException {
        if (createShardContext().getMapperService().documentMapper(queryBuilder.type()) == null) {
            assertEquals(new MatchNoDocsQuery(), query);
        } else {
            assertThat(query,
                anyOf(
                    equalTo(new TypeFieldMapper.TypesQuery(new BytesRef(queryBuilder.type()))),
                    equalTo(new MatchAllDocsQuery()))
            );
        }
    }

    public void testIllegalArgument() {
        expectThrows(IllegalArgumentException.class, () -> new TypeQueryBuilder((String) null));
    }

    public void testFromJson() throws IOException {
        String json =
                "{\n" +
                "  \"type\" : {\n" +
                "    \"value\" : \"my_type\",\n" +
                "    \"boost\" : 1.0\n" +
                "  }\n" +
                "}";

        TypeQueryBuilder parsed = (TypeQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);

        assertEquals(json, "my_type", parsed.type());
    }

    @Override
    public void testToQuery() throws IOException {
        super.testToQuery();
        assertWarnings(TypeQueryBuilder.TYPES_DEPRECATION_MESSAGE);
    }

    @Override
    public void testMustRewrite() throws IOException {
        super.testMustRewrite();
        assertWarnings(TypeQueryBuilder.TYPES_DEPRECATION_MESSAGE);
    }

    @Override
    public void testCacheability() throws IOException {
        super.testCacheability();
        assertWarnings(TypeQueryBuilder.TYPES_DEPRECATION_MESSAGE);
    }
}

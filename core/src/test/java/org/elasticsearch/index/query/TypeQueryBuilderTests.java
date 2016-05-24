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

import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.internal.TypeFieldMapper;
import org.elasticsearch.test.AbstractQueryTestCase;

import java.io.IOException;

public class TypeQueryBuilderTests extends AbstractQueryTestCase<TypeQueryBuilder> {

    @Override
    protected TypeQueryBuilder doCreateTestQueryBuilder() {
        return new TypeQueryBuilder(getRandomType());
    }

    @Override
    protected void doAssertLuceneQuery(TypeQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        if (createShardContext().getMapperService().documentMapper(queryBuilder.type()) == null) {
            assertEquals(new MatchNoDocsQuery(), query);
        } else {
            assertEquals(new TypeFieldMapper.TypeQuery(new BytesRef(queryBuilder.type())), query);
        }
    }

    public void testIllegalArgument() {
        try {
            new TypeQueryBuilder((String) null);
            fail("cannot be null");
        } catch (IllegalArgumentException e) {
            // expected
        }
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
}

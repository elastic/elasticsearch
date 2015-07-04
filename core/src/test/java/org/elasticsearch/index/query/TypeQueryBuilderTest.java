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

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.internal.TypeFieldMapper;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.Matchers.is;

public class TypeQueryBuilderTest extends BaseQueryTestCase<TypeQueryBuilder> {

    @Override
    protected TypeQueryBuilder doCreateTestQueryBuilder() {
        return new TypeQueryBuilder(getRandomType());
    }

    @Override
    protected Query doCreateExpectedQuery(TypeQueryBuilder queryBuilder, QueryParseContext context) throws IOException {
        Query expectedQuery;
        //LUCENE 4 UPGRADE document mapper should use bytesref as well?
        DocumentMapper documentMapper = context.mapperService().documentMapper(queryBuilder.type().utf8ToString());
        if (documentMapper == null) {
            expectedQuery = new TermQuery(new Term(TypeFieldMapper.NAME, queryBuilder.type()));
        } else {
            expectedQuery = documentMapper.typeFilter();
        }
        return expectedQuery;
    }

    @Test
    public void testValidate() {
        TypeQueryBuilder typeQueryBuilder = new TypeQueryBuilder((String) null);
        assertThat(typeQueryBuilder.validate().validationErrors().size(), is(1));
    }
}

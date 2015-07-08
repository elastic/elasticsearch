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
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.support.QueryParsers;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.Matchers.is;

public class WildcardQueryBuilderTest extends BaseQueryTestCase<WildcardQueryBuilder> {

    @Override
    protected WildcardQueryBuilder doCreateTestQueryBuilder() {
        WildcardQueryBuilder query;

        // mapped or unmapped field
        String text = randomAsciiOfLengthBetween(1, 10);
        if (randomBoolean()) {
            query = new WildcardQueryBuilder(STRING_FIELD_NAME, text);
        } else {
            query = new WildcardQueryBuilder(randomAsciiOfLengthBetween(1, 10), text);
        }
        if (randomBoolean()) {
            query.rewrite(randomFrom(getRandomRewriteMethod()));
        }
        return query;
    }

    @Override
    protected Query doCreateExpectedQuery(WildcardQueryBuilder queryBuilder, QueryParseContext context) throws IOException {
        String indexFieldName;
        BytesRef valueBytes;

        MappedFieldType fieldType = context.fieldMapper(queryBuilder.fieldName());
        if (fieldType != null) {
            indexFieldName = fieldType.names().indexName();
            valueBytes = fieldType.indexedValueForSearch(queryBuilder.value());
        } else {
            indexFieldName = queryBuilder.fieldName();
            valueBytes = new BytesRef(queryBuilder.value());
        }

        WildcardQuery expectedQuery = new WildcardQuery(new Term(indexFieldName, valueBytes));

        //norelease fix to be removed to avoid NPE on unmapped fields
        context.parseFieldMatcher(randomBoolean() ? ParseFieldMatcher.EMPTY : ParseFieldMatcher.STRICT);
        MultiTermQuery.RewriteMethod rewriteMethod = QueryParsers.parseRewriteMethod(context.parseFieldMatcher(), queryBuilder.rewrite(), null);
        QueryParsers.setRewriteMethod(expectedQuery, rewriteMethod);
        return expectedQuery;
    }

    @Test
    public void testValidate() {
        WildcardQueryBuilder wildcardQueryBuilder = new WildcardQueryBuilder("", "text");
        assertThat(wildcardQueryBuilder.validate().validationErrors().size(), is(1));

        wildcardQueryBuilder = new WildcardQueryBuilder("field", null);
        assertThat(wildcardQueryBuilder.validate().validationErrors().size(), is(1));

        wildcardQueryBuilder = new WildcardQueryBuilder(null, null);
        assertThat(wildcardQueryBuilder.validate().validationErrors().size(), is(2));

        wildcardQueryBuilder = new WildcardQueryBuilder("field", "text");
        assertNull(wildcardQueryBuilder.validate());
    }

    @Test
    public void testEmptyValue() throws IOException {
        QueryParseContext context = createContext();
        context.setAllowUnmappedFields(true);

        WildcardQueryBuilder wildcardQueryBuilder = new WildcardQueryBuilder(getRandomType(), "");
        assertEquals(wildcardQueryBuilder.toQuery(context).getClass(), WildcardQuery.class);
    }
}

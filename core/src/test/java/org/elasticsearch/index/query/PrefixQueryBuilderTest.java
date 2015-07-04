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
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.support.QueryParsers;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.Matchers.is;

public class PrefixQueryBuilderTest extends BaseQueryTestCase<PrefixQueryBuilder> {

    @Override
    protected PrefixQueryBuilder doCreateTestQueryBuilder() {
        String fieldName = randomBoolean() ? STRING_FIELD_NAME : randomAsciiOfLengthBetween(1, 10);
        String value = randomAsciiOfLengthBetween(1, 10);
        PrefixQueryBuilder query = new PrefixQueryBuilder(fieldName, value);

        if (randomBoolean()) {
            query.rewrite(getRandomRewriteMethod());
        }
        return query;
    }

    @Override
    protected Query doCreateExpectedQuery(PrefixQueryBuilder queryBuilder, QueryParseContext context) throws IOException {
        //norelease fix to be removed to avoid NPE on unmapped fields (Dtests.seed=BF5D7566DECBC5B1)
        context.parseFieldMatcher(randomBoolean() ? ParseFieldMatcher.EMPTY : ParseFieldMatcher.STRICT);
        
        MultiTermQuery.RewriteMethod method = QueryParsers.parseRewriteMethod(context.parseFieldMatcher(), queryBuilder.rewrite(), null);

        Query query = null;
        MappedFieldType fieldType = context.fieldMapper(queryBuilder.fieldName());
        if (fieldType != null) {
            query = fieldType.prefixQuery(queryBuilder.value(), method, context);
        }
        if (query == null) {
            PrefixQuery prefixQuery = new PrefixQuery(new Term(queryBuilder.fieldName(), BytesRefs.toBytesRef(queryBuilder.value())));
            if (method != null) {
                prefixQuery.setRewriteMethod(method);
            }
            query = prefixQuery;
        }

        return query;
    }

    @Test
    public void testValidate() {
        PrefixQueryBuilder prefixQueryBuilder = new PrefixQueryBuilder("", "prefix");
        assertThat(prefixQueryBuilder.validate().validationErrors().size(), is(1));

        prefixQueryBuilder = new PrefixQueryBuilder("field", null);
        assertThat(prefixQueryBuilder.validate().validationErrors().size(), is(1));

        prefixQueryBuilder = new PrefixQueryBuilder("field", "prefix");
        assertNull(prefixQueryBuilder.validate());

        prefixQueryBuilder = new PrefixQueryBuilder(null, null);
        assertThat(prefixQueryBuilder.validate().validationErrors().size(), is(2));
    }
}
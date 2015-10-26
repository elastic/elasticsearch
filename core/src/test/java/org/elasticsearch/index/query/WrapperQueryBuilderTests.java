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

import org.apache.lucene.search.Query;
import org.elasticsearch.action.support.ToXContentToBytes;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class WrapperQueryBuilderTests extends AbstractQueryTestCase<WrapperQueryBuilder> {

    @Override
    protected boolean supportsBoostAndQueryName() {
        return false;
    }

    @Override
    protected WrapperQueryBuilder doCreateTestQueryBuilder() {
        QueryBuilder wrappedQuery = RandomQueryBuilder.createQuery(random());
        switch (randomInt(2)) {
            case 0:
                return new WrapperQueryBuilder(wrappedQuery.toString());
            case 1:
                return new WrapperQueryBuilder(((ToXContentToBytes)wrappedQuery).buildAsBytes().toBytes());
            case 2:
                return new WrapperQueryBuilder(((ToXContentToBytes)wrappedQuery).buildAsBytes());
            default:
                throw new UnsupportedOperationException();
        }
    }

    @Override
    protected void doAssertLuceneQuery(WrapperQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        try (XContentParser qSourceParser = XContentFactory.xContent(queryBuilder.source()).createParser(queryBuilder.source())) {
            final QueryShardContext contextCopy = new QueryShardContext(context.indexQueryParserService());
            contextCopy.reset(qSourceParser);
            QueryBuilder<?> innerQuery = contextCopy.parseContext().parseInnerQueryBuilder();
            Query expected = innerQuery.toQuery(context);
            assertThat(query, equalTo(expected));
        }
    }

    @Override
    protected void assertBoost(WrapperQueryBuilder queryBuilder, Query query) throws IOException {
        //no-op boost is checked already above as part of doAssertLuceneQuery as we rely on lucene equals impl
    }

    public void testIllegalArgument() {
        try {
            if (randomBoolean()) {
                new WrapperQueryBuilder((byte[]) null);
            } else {
                new WrapperQueryBuilder(new byte[0]);
            }
            fail("cannot be null or empty");
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            if (randomBoolean()) {
                new WrapperQueryBuilder((String) null);
            } else {
                new WrapperQueryBuilder("");
            }
            fail("cannot be null or empty");
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            if (randomBoolean()) {
                new WrapperQueryBuilder((BytesReference) null);
            } else {
                new WrapperQueryBuilder(new BytesArray(new byte[0]));
            }
            fail("cannot be null or empty");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }
}

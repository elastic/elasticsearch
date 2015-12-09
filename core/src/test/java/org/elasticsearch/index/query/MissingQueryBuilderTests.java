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

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;

public class MissingQueryBuilderTests extends AbstractQueryTestCase<MissingQueryBuilder> {

    @Override
    protected MissingQueryBuilder doCreateTestQueryBuilder() {
        String fieldName = randomBoolean() ? randomFrom(MAPPED_FIELD_NAMES) : randomAsciiOfLengthBetween(1, 10);
        Boolean existence = randomBoolean();
        Boolean nullValue = randomBoolean();
        if (existence == false && nullValue == false) {
            if (randomBoolean()) {
                existence = true;
            } else {
                nullValue = true;
            }
        }
        return new MissingQueryBuilder(fieldName, nullValue, existence);
    }

    @Override
    protected void doAssertLuceneQuery(MissingQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        // too many mapping dependent cases to test, we don't want to end up
        // duplication the toQuery method
    }

    public void testIllegalArguments() {
        try {
            if (randomBoolean()) {
                new MissingQueryBuilder("", true, true);
            } else {
                new MissingQueryBuilder(null, true, true);
            }
            fail("must not be null or empty");
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            new MissingQueryBuilder("fieldname", false, false);
            fail("existence and nullValue cannot both be false");
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            new MissingQueryBuilder("fieldname", MissingQueryBuilder.DEFAULT_NULL_VALUE, false);
            fail("existence and nullValue cannot both be false");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    public void testBothNullValueAndExistenceFalse() throws IOException {
        QueryShardContext context = createShardContext();
        context.setAllowUnmappedFields(true);
        try {
            MissingQueryBuilder.newFilter(context, "field", false, false);
            fail("Expected QueryShardException");
        } catch (QueryShardException e) {
            assertThat(e.getMessage(), containsString("missing must have either existence, or null_value"));
        }
    }

    public void testFromJson() throws IOException {
        String json =
            "{\n" + 
                "  \"missing\" : {\n" + 
                "    \"field\" : \"user\",\n" + 
                "    \"null_value\" : false,\n" + 
                "    \"existence\" : true,\n" + 
                "    \"boost\" : 1.0\n" + 
                "  }\n" + 
                "}";

        MissingQueryBuilder parsed = (MissingQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);

        assertEquals(json, false, parsed.nullValue());
        assertEquals(json, true, parsed.existence());
        assertEquals(json, "user", parsed.fieldPattern());
    }
}

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

import org.apache.lucene.search.*;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.internal.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.object.ObjectMapper;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;

import static org.hamcrest.Matchers.is;

public class MissingQueryBuilderTest extends BaseQueryTestCase<MissingQueryBuilder> {

    @Override
    protected MissingQueryBuilder doCreateTestQueryBuilder() {
        MissingQueryBuilder query  = new MissingQueryBuilder(getRandomFieldName());
        if (randomBoolean()) {
            query.nullValue(randomBoolean());
        }
        if (randomBoolean()) {
            query.existence(randomBoolean());
        }
        // cannot set both to false
        if ((query.nullValue() == false) && (query.existence() == false)) {
            query.existence(!query.existence());
        }
        return query;
    }

    private String getRandomFieldName() {
        if (randomBoolean()) {
            return randomAsciiOfLengthBetween(1, 10);
        }
        switch (randomIntBetween(0, 3)) {
            case 0:
                return BOOLEAN_FIELD_NAME;
            case 1:
                return STRING_FIELD_NAME;
            case 2:
                return INT_FIELD_NAME;
            case 3:
                return DOUBLE_FIELD_NAME;
            default:
                throw new UnsupportedOperationException();
        }
    }

    @Override
    protected Query doCreateExpectedQuery(MissingQueryBuilder queryBuilder, QueryParseContext context) throws IOException {
        final boolean existence = queryBuilder.existence();
        final boolean nullValue = queryBuilder.nullValue();
        String fieldPattern = queryBuilder.fieldPattern();

        if (!existence && !nullValue) {
            throw new QueryParsingException(context, "missing must have either existence, or null_value, or both set to true");
        }

        final FieldNamesFieldMapper.FieldNamesFieldType fieldNamesFieldType = (FieldNamesFieldMapper.FieldNamesFieldType) context.mapperService().fullName(FieldNamesFieldMapper.NAME);
        if (fieldNamesFieldType == null) {
            // can only happen when no types exist, so no docs exist either
            return Queries.newMatchNoDocsQuery();
        }

        ObjectMapper objectMapper = context.getObjectMapper(fieldPattern);
        if (objectMapper != null) {
            // automatic make the object mapper pattern
            fieldPattern = fieldPattern + ".*";
        }

        Collection<String> fields = context.simpleMatchToIndexNames(fieldPattern);
        if (fields.isEmpty()) {
            if (existence) {
                // if we ask for existence of fields, and we found none, then we should match on all
                return Queries.newMatchAllQuery();
            }
            return null;
        }

        Query existenceFilter = null;
        Query nullFilter = null;

        if (existence) {
            BooleanQuery boolFilter = new BooleanQuery();
            for (String field : fields) {
                MappedFieldType fieldType = context.fieldMapper(field);
                Query filter = null;
                if (fieldNamesFieldType.isEnabled()) {
                    final String f;
                    if (fieldType != null) {
                        f = fieldType.names().indexName();
                    } else {
                        f = field;
                    }
                    filter = fieldNamesFieldType.termQuery(f, context);
                }
                // if _field_names are not indexed, we need to go the slow way
                if (filter == null && fieldType != null) {
                    filter = fieldType.rangeQuery(null, null, true, true);
                }
                if (filter == null) {
                    filter = new TermRangeQuery(field, null, null, true, true);
                }
                boolFilter.add(filter, BooleanClause.Occur.SHOULD);
            }

            existenceFilter = boolFilter;
            existenceFilter = Queries.not(existenceFilter);;
        }

        if (nullValue) {
            for (String field : fields) {
                MappedFieldType fieldType = context.fieldMapper(field);
                if (fieldType != null) {
                    nullFilter = fieldType.nullValueQuery();
                }
            }
        }

        Query filter;
        if (nullFilter != null) {
            if (existenceFilter != null) {
                BooleanQuery combined = new BooleanQuery();
                combined.add(existenceFilter, BooleanClause.Occur.SHOULD);
                combined.add(nullFilter, BooleanClause.Occur.SHOULD);
                // cache the not filter as well, so it will be faster
                filter = combined;
            } else {
                filter = nullFilter;
            }
        } else {
            filter = existenceFilter;
        }

        if (filter == null) {
            return null;
        }

        return new ConstantScoreQuery(filter);
    }

    @Test
    public void testValidate() {
        MissingQueryBuilder missingQueryBuilder = new MissingQueryBuilder("");
        assertThat(missingQueryBuilder.validate().validationErrors().size(), is(1));

        missingQueryBuilder = new MissingQueryBuilder(null);
        assertThat(missingQueryBuilder.validate().validationErrors().size(), is(1));

        missingQueryBuilder = new MissingQueryBuilder("field").existence(false).nullValue(false);
        assertThat(missingQueryBuilder.validate().validationErrors().size(), is(1));

        missingQueryBuilder = new MissingQueryBuilder("field");
        assertNull(missingQueryBuilder.validate());
    }

    @Test(expected = QueryParsingException.class)
    public void testBothNullValueAndExistenceFalse() throws IOException {
        QueryParseContext context = createContext();
        context.setAllowUnmappedFields(true);
        MissingQueryBuilder.newFilter(context, "field", false, false);
    }
}

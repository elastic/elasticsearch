/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.index.Term;
import org.apache.lucene.queries.spans.FieldMaskingSpanQuery;
import org.apache.lucene.queries.spans.SpanTermQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.test.AbstractQueryTestCase;

import java.io.IOException;

import static org.elasticsearch.index.query.FieldMaskingSpanQueryBuilder.NAME;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;

public class FieldMaskingSpanQueryBuilderTests extends AbstractQueryTestCase<FieldMaskingSpanQueryBuilder> {
    @Override
    protected FieldMaskingSpanQueryBuilder doCreateTestQueryBuilder() {
        String fieldName;
        if (randomBoolean()) {
            fieldName = randomFrom(MAPPED_FIELD_NAMES);
        } else {
            fieldName = randomAlphaOfLengthBetween(1, 10);
        }
        SpanTermQueryBuilder innerQuery = new SpanTermQueryBuilderTests().createTestQueryBuilder();
        innerQuery.boost(1.0f);
        return new FieldMaskingSpanQueryBuilder(innerQuery, fieldName);
    }

    @Override
    protected void doAssertLuceneQuery(FieldMaskingSpanQueryBuilder queryBuilder,
                                       Query query,
                                       SearchExecutionContext context) throws IOException {
        String fieldInQuery = expectedFieldName(queryBuilder.fieldName());
        assertThat(query, instanceOf(FieldMaskingSpanQuery.class));
        FieldMaskingSpanQuery fieldMaskingSpanQuery = (FieldMaskingSpanQuery) query;
        assertThat(fieldMaskingSpanQuery.getField(), equalTo(fieldInQuery));
        Query subQuery = queryBuilder.innerQuery().toQuery(context);
        assertThat(fieldMaskingSpanQuery.getMaskedQuery(), equalTo(subQuery));
    }

    public void testIllegalArguments() {
        expectThrows(IllegalArgumentException.class, () -> new FieldMaskingSpanQueryBuilder(null, "maskedField"));
        SpanQueryBuilder span = new SpanTermQueryBuilder("name", "value");
        expectThrows(IllegalArgumentException.class, () -> new FieldMaskingSpanQueryBuilder(span, null));
        expectThrows(IllegalArgumentException.class, () -> new FieldMaskingSpanQueryBuilder(span, ""));
    }

    public void testFromJson() throws IOException {
        String json =
                "{\n" +
                "  \"" + NAME.getPreferredName() + "\" : {\n" +
                "    \"query\" : {\n" +
                "      \"span_term\" : {\n" +
                "        \"value\" : {\n" +
                "          \"value\" : 0.5,\n" +
                "          \"boost\" : 0.23\n" +
                "        }\n" +
                "      }\n" +
                "    },\n" +
                "    \"field\" : \"mapped_geo_shape\",\n" +
                "    \"boost\" : 42.0,\n" +
                "    \"_name\" : \"KPI\"\n" +
                "  }\n" +
                "}";
        Exception exception = expectThrows(ParsingException.class, () -> parseQuery(json));
        assertThat(exception.getMessage(),
            equalTo(NAME.getPreferredName() + " [query] as a nested span clause can't have non-default boost value [0.23]"));
    }

    public void testJsonWithTopLevelBoost() throws IOException {
        String json =
            "{\n" +
                "  \"" + NAME.getPreferredName() + "\" : {\n" +
                "    \"query\" : {\n" +
                "      \"span_term\" : {\n" +
                "        \"value\" : {\n" +
                "          \"value\" : \"foo\"\n" +
                "        }\n" +
                "      }\n" +
                "    },\n" +
                "    \"field\" : \"mapped_geo_shape\",\n" +
                "    \"boost\" : 42.0,\n" +
                "    \"_name\" : \"KPI\"\n" +
                "  }\n" +
                "}";
        Query q = parseQuery(json).toQuery(createSearchExecutionContext());
        assertEquals(
            new BoostQuery(
                new FieldMaskingSpanQuery(new SpanTermQuery(new Term("value", "foo")), "mapped_geo_shape"),
                42.0f),
            q
        );
    }

    public void testJsonWithDeprecatedName() throws IOException {
        String json =
            "{\n" +
                "  \"field_masking_span\" : {\n" +
                "    \"query\" : {\n" +
                "      \"span_term\" : {\n" +
                "        \"value\" : {\n" +
                "          \"value\" : \"foo\"\n" +
                "        }\n" +
                "      }\n" +
                "    },\n" +
                "    \"field\" : \"mapped_geo_shape\",\n" +
                "    \"boost\" : 42.0,\n" +
                "    \"_name\" : \"KPI\"\n" +
                "  }\n" +
                "}";
        Query q = parseQuery(json).toQuery(createSearchExecutionContext());
        assertWarnings("Deprecated field [field_masking_span] used, expected [" + NAME.getPreferredName() + "] instead");
    }
}

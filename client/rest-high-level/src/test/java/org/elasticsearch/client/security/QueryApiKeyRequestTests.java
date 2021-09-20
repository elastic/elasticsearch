/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.client.ValidationException;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.searchafter.SearchAfterBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import java.util.List;
import java.util.Optional;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class QueryApiKeyRequestTests extends ESTestCase {

    public void testNewInstance() {
        final QueryBuilder queryBuilder = randomQueryBuilder();
        final int from = randomIntBetween(0, 100);
        final int size = randomIntBetween(0, 100);
        final List<FieldSortBuilder> fieldSortBuilders = randomFieldSortBuilders();
        final SearchAfterBuilder searchAfterBuilder = randomSearchAfterBuilder();
        final QueryApiKeyRequest request = new QueryApiKeyRequest(queryBuilder, from, size, fieldSortBuilders, searchAfterBuilder);

        assertThat(request.getQueryBuilder(), equalTo(queryBuilder));
        assertThat(request.getFrom(), equalTo(from));
        assertThat(request.getSize(), equalTo(size));
        assertThat(request.getFieldSortBuilders(), equalTo(fieldSortBuilders));
        assertThat(request.getSearchAfterBuilder(), equalTo(searchAfterBuilder));
    }

    public void testEqualsHashCode() {
        final QueryApiKeyRequest request = new QueryApiKeyRequest(randomQueryBuilder(),
            randomIntBetween(0, 100),
            randomIntBetween(0, 100),
            randomFieldSortBuilders(),
            randomSearchAfterBuilder());

        EqualsHashCodeTestUtils.checkEqualsAndHashCode(request, original -> new QueryApiKeyRequest(original.getQueryBuilder(),
            original.getFrom(),
            original.getSize(),
            original.getFieldSortBuilders(),
            original.getSearchAfterBuilder()), this::mutateInstance);
    }

    public void testValidation() {
        final QueryApiKeyRequest request1 = new QueryApiKeyRequest(null, randomIntBetween(0, 100), randomIntBetween(0, 100), null, null);
        final Optional<ValidationException> validationException1 = request1.validate();
        assertThat(validationException1.isPresent(), is(false));

        final QueryApiKeyRequest request2 = new QueryApiKeyRequest(null, randomIntBetween(-100, -1), randomIntBetween(0, 100), null, null);
        final Optional<ValidationException> validationException2 = request2.validate();
        assertThat(validationException2.get().getMessage(), containsString("from must be non-negative"));

        final QueryApiKeyRequest request3 = new QueryApiKeyRequest(null, randomIntBetween(0, 100), randomIntBetween(-100, -1), null, null);
        final Optional<ValidationException> validationException3 = request3.validate();
        assertThat(validationException3.get().getMessage(), containsString("size must be non-negative"));
    }

    private QueryApiKeyRequest mutateInstance(QueryApiKeyRequest request) {
        switch (randomIntBetween(0, 5)) {
            case 0:
                return new QueryApiKeyRequest(randomValueOtherThan(request.getQueryBuilder(), QueryApiKeyRequestTests::randomQueryBuilder),
                    request.getFrom(),
                    request.getSize(),
                    request.getFieldSortBuilders(),
                    request.getSearchAfterBuilder());
            case 1:
                return new QueryApiKeyRequest(request.getQueryBuilder(),
                    request.getFrom() + 1,
                    request.getSize(),
                    request.getFieldSortBuilders(),
                    request.getSearchAfterBuilder());
            case 2:
                return new QueryApiKeyRequest(request.getQueryBuilder(),
                    request.getFrom(),
                    request.getSize() + 1,
                    request.getFieldSortBuilders(),
                    request.getSearchAfterBuilder());
            case 3:
                return new QueryApiKeyRequest(request.getQueryBuilder(),
                    request.getFrom(),
                    request.getSize(),
                    randomValueOtherThan(request.getFieldSortBuilders(), QueryApiKeyRequestTests::randomFieldSortBuilders),
                    request.getSearchAfterBuilder());
            default:
                return new QueryApiKeyRequest(request.getQueryBuilder(),
                    request.getFrom(),
                    request.getSize(),
                    request.getFieldSortBuilders(),
                    randomValueOtherThan(request.getSearchAfterBuilder(), QueryApiKeyRequestTests::randomSearchAfterBuilder));

        }
    }

    public static QueryBuilder randomQueryBuilder() {
        switch (randomIntBetween(0, 5)) {
            case 0:
                return QueryBuilders.matchAllQuery();
            case 1:
                return QueryBuilders.termQuery(randomAlphaOfLengthBetween(3, 8),
                    randomFrom(randomAlphaOfLength(8), randomInt(), randomLong(), randomDouble(), randomFloat()));
            case 2:
                return QueryBuilders.idsQuery().addIds(randomArray(1, 5, String[]::new, () -> randomAlphaOfLength(20)));
            case 3:
                return QueryBuilders.prefixQuery(randomAlphaOfLengthBetween(3, 8), randomAlphaOfLengthBetween(3, 8));
            case 4:
                return QueryBuilders.wildcardQuery(randomAlphaOfLengthBetween(3, 8),
                    randomAlphaOfLengthBetween(0, 3) + "*" + randomAlphaOfLengthBetween(0, 3));
            case 5:
                return QueryBuilders.rangeQuery(randomAlphaOfLengthBetween(3, 8)).from(randomNonNegativeLong()).to(randomNonNegativeLong());
            default:
                return null;
        }
    }

    public static List<FieldSortBuilder> randomFieldSortBuilders() {
        if (randomBoolean()) {
            return randomList(1, 2, () -> new FieldSortBuilder(randomAlphaOfLengthBetween(3, 8)).order(randomFrom(SortOrder.values())));
        } else {
            return null;
        }
    }

    public static SearchAfterBuilder randomSearchAfterBuilder() {
        if (randomBoolean()) {
            return new SearchAfterBuilder().setSortValues(randomArray(1, 2, String[]::new, () -> randomAlphaOfLengthBetween(3, 8)));
        } else {
            return null;
        }
    }
}

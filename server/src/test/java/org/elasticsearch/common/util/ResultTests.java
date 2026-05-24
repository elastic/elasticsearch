/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.test.hamcrest.OptionalMatchers.isEmpty;
import static org.elasticsearch.test.hamcrest.OptionalMatchers.isPresentWith;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class ResultTests extends ESTestCase {

    public void testSuccess() {
        final String str = randomAlphaOfLengthBetween(3, 8);
        final Result<String, ElasticsearchException> result = Result.of(str);
        assertThat(result.isSuccessful(), is(true));
        assertThat(result.isFailure(), is(false));
        assertThat(result.get(), sameInstance(str));
        assertThat(result.failure(), isEmpty());
        assertThat(result.asOptional(), isPresentWith(str));
    }

    public void testFailure() {
        final ElasticsearchException exception = new ElasticsearchStatusException(
            randomAlphaOfLengthBetween(10, 30),
            RestStatus.INTERNAL_SERVER_ERROR
        );
        final Result<String, ElasticsearchException> result = Result.failure(exception);
        assertThat(result.isSuccessful(), is(false));
        assertThat(result.isFailure(), is(true));
        assertThat(expectThrows(Exception.class, result::get), sameInstance(exception));
        assertThat(result.failure(), isPresentWith(sameInstance(exception)));
        assertThat(result.asOptional(), isEmpty());
    }

}

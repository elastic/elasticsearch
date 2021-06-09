/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.asyncsearch;

import org.elasticsearch.client.ValidationException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.test.ESTestCase;

import java.util.Optional;

public class SubmitAsyncSearchRequestTests extends ESTestCase {

    public void testValidation() {
        {
            SearchSourceBuilder source = new SearchSourceBuilder();
            SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(source, "test");
            Optional<ValidationException> validation = request.validate();
            assertFalse(validation.isPresent());
        }
        {
            SearchSourceBuilder source = new SearchSourceBuilder().suggest(new SuggestBuilder());
            SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(source, "test");
            Optional<ValidationException> validation = request.validate();
            assertTrue(validation.isPresent());
            assertEquals(1, validation.get().validationErrors().size());
            assertEquals("suggest-only queries are not supported", validation.get().validationErrors().get(0));
        }
        {
            SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(new SearchSourceBuilder(), "test");
            request.setKeepAlive(new TimeValue(1));
            Optional<ValidationException> validation = request.validate();
            assertTrue(validation.isPresent());
            assertEquals(1, validation.get().validationErrors().size());
            assertEquals("[keep_alive] must be greater than 1 minute, got: 1ms", validation.get().validationErrors().get(0));
        }
    }

}

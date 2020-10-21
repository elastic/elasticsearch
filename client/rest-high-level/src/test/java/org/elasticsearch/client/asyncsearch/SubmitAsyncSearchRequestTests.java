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

package org.elasticsearch.client.asyncsearch;

import org.elasticsearch.client.ValidationException;
import org.elasticsearch.common.unit.TimeValue;
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

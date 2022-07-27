/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.apikey;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.hamcrest.Matchers.equalTo;

public class BulkUpdateApiKeyResponseTests extends ESTestCase {

    public void testToXContent() throws IOException {
        // Force ordered key set for deterministic comparison with raw JSON string below
        final SortedMap<String, Exception> errorDetails = new TreeMap<>();
        errorDetails.put("failed-api-key-id-1", new IllegalArgumentException("msg - 1"));
        errorDetails.put("failed-api-key-id-2", new ResourceNotFoundException("potato"));
        errorDetails.put("failed-api-key-id-3", new ElasticsearchException("error1", new IllegalArgumentException("msg - 1")));
        errorDetails.put("failed-api-key-id-4", new ElasticsearchException("error2", new IllegalArgumentException("msg - 2")));
        final var response = new BulkUpdateApiKeyResponse(List.of("api-key-id-1"), List.of("api-key-id-2", "api-key-id-3"), errorDetails);
        final XContentBuilder builder = XContentFactory.jsonBuilder();

        response.toXContent(builder, ToXContent.EMPTY_PARAMS);

        assertThat(Strings.toString(builder), equalTo(XContentHelper.stripWhitespace("""
            {
               "updated": [
                 "api-key-id-1"
               ],
               "noops": [
                 "api-key-id-2",
                 "api-key-id-3"
               ],
               "errors": {
                 "count": 4,
                 "details": {
                   "failed-api-key-id-1": {
                     "type": "illegal_argument_exception",
                     "reason": "msg - 1"
                   },
                   "failed-api-key-id-2": {
                     "type": "resource_not_found_exception",
                     "reason": "potato"
                   },
                   "failed-api-key-id-3": {
                     "type": "exception",
                     "reason": "error1",
                     "caused_by": {
                       "type": "illegal_argument_exception",
                       "reason": "msg - 1"
                     }
                   },
                   "failed-api-key-id-4": {
                     "type": "exception",
                     "reason": "error2",
                     "caused_by": {
                       "type": "illegal_argument_exception",
                       "reason": "msg - 2"
                     }
                   }
                 }
               }
             }""")));
    }

    public void testToXContentOmitsErrorDetailsIfNoErrors() throws IOException {
        final var response = new BulkUpdateApiKeyResponse(List.of("api-key-id-1"), List.of("api-key-id-2", "api-key-id-3"), Map.of());
        final XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertThat(Strings.toString(builder), equalTo(XContentHelper.stripWhitespace("""
            {
               "updated": [
                 "api-key-id-1"
               ],
               "noops": [
                 "api-key-id-2",
                 "api-key-id-3"
               ],
               "errors": {
                 "count": 0
               }
             }""")));
    }
}

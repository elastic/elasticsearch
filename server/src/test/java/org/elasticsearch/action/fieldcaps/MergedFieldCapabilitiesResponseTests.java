/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.mapper.TimeSeriesParams;
import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

public class MergedFieldCapabilitiesResponseTests extends AbstractChunkedSerializingTestCase<FieldCapabilitiesResponse> {

    @Override
    protected FieldCapabilitiesResponse doParseInstance(XContentParser parser) throws IOException {
        return FieldCapabilitiesResponse.fromXContent(parser);
    }

    @Override
    protected FieldCapabilitiesResponse createTestInstance() {
        // merged responses
        Map<String, Map<String, FieldCapabilities>> responses = new HashMap<>();

        String[] fields = generateRandomStringArray(5, 10, false, true);
        assertNotNull(fields);

        for (String field : fields) {
            Map<String, FieldCapabilities> typesToCapabilities = new HashMap<>();
            String[] types = generateRandomStringArray(5, 10, false, false);
            assertNotNull(types);

            for (String type : types) {
                typesToCapabilities.put(type, FieldCapabilitiesTests.randomFieldCaps(field));
            }
            responses.put(field, typesToCapabilities);
        }
        int numIndices = randomIntBetween(1, 10);
        String[] indices = new String[numIndices];
        for (int i = 0; i < numIndices; i++) {
            indices[i] = randomAlphaOfLengthBetween(5, 10);
        }
        return new FieldCapabilitiesResponse(indices, responses);
    }

    @Override
    protected Writeable.Reader<FieldCapabilitiesResponse> instanceReader() {
        return FieldCapabilitiesResponse::new;
    }

    @Override
    protected FieldCapabilitiesResponse mutateInstance(FieldCapabilitiesResponse response) {
        Map<String, Map<String, FieldCapabilities>> mutatedResponses = new HashMap<>(response.get());

        int mutation = response.get().isEmpty() ? 0 : randomIntBetween(0, 2);

        switch (mutation) {
            case 0 -> {
                String toAdd = randomAlphaOfLength(10);
                mutatedResponses.put(
                    toAdd,
                    Collections.singletonMap(randomAlphaOfLength(10), FieldCapabilitiesTests.randomFieldCaps(toAdd))
                );
            }
            case 1 -> {
                String toRemove = randomFrom(mutatedResponses.keySet());
                mutatedResponses.remove(toRemove);
            }
            case 2 -> {
                String toReplace = randomFrom(mutatedResponses.keySet());
                mutatedResponses.put(
                    toReplace,
                    Collections.singletonMap(randomAlphaOfLength(10), FieldCapabilitiesTests.randomFieldCaps(toReplace))
                );
            }
        }
        // TODO pass real list
        return new FieldCapabilitiesResponse(null, mutatedResponses, Collections.emptyList());
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        // Disallow random fields from being inserted under the 'fields' key, as this
        // map only contains field names, and also under 'fields.FIELD_NAME', as these
        // maps only contain type names.
        return field -> field.matches("fields(\\.\\w+)?");
    }

    public void testToXContent() throws IOException {
        FieldCapabilitiesResponse response = createSimpleResponse();
        assertEquals("""
            {
              "indices": [ "index1", "index2", "index3", "index4" ],
              "fields": {
                "rating": {
                  "keyword": {
                    "type": "keyword",
                    "metadata_field": false,
                    "searchable": false,
                    "aggregatable": true,
                    "time_series_dimension": true,
                    "indices": [ "index3", "index4" ],
                    "non_searchable_indices": [ "index4" ]
                  },
                  "long": {
                    "type": "long",
                    "metadata_field": false,
                    "searchable": true,
                    "aggregatable": false,
                    "time_series_metric": "counter",
                    "indices": [ "index1", "index2" ],
                    "non_aggregatable_indices": [ "index1" ],
                    "non_dimension_indices": [ "index4" ]
                  }
                },
                "title": {
                  "text": {
                    "type": "text",
                    "metadata_field": false,
                    "searchable": true,
                    "aggregatable": false
                  }
                }
              },
              "failed_indices": 2,
              "failures": [
                {
                  "indices": [ "errorindex", "errorindex2" ],
                  "failure": {
                    "error": {
                      "root_cause": [ { "type": "illegal_argument_exception", "reason": "test" } ],
                      "type": "illegal_argument_exception",
                      "reason": "test"
                    }
                  }
                }
              ]
            }""".replaceAll("\\s+", ""), Strings.toString(response));
    }

    private static FieldCapabilitiesResponse createSimpleResponse() {
        Map<String, FieldCapabilities> titleCapabilities = new HashMap<>();
        titleCapabilities.put(
            "text",
            new FieldCapabilities("title", "text", false, true, false, false, null, null, null, null, null, null, Collections.emptyMap())
        );

        Map<String, FieldCapabilities> ratingCapabilities = new HashMap<>();
        ratingCapabilities.put(
            "long",
            new FieldCapabilities(
                "rating",
                "long",
                false,
                true,
                false,
                false,
                TimeSeriesParams.MetricType.COUNTER,
                new String[] { "index1", "index2" },
                null,
                new String[] { "index1" },
                new String[] { "index4" },
                null,
                Collections.emptyMap()
            )
        );
        ratingCapabilities.put(
            "keyword",
            new FieldCapabilities(
                "rating",
                "keyword",
                false,
                false,
                true,
                true,
                null,
                new String[] { "index3", "index4" },
                new String[] { "index4" },
                null,
                null,
                null,
                Collections.emptyMap()
            )
        );

        Map<String, Map<String, FieldCapabilities>> responses = new HashMap<>();
        responses.put("title", titleCapabilities);
        responses.put("rating", ratingCapabilities);

        List<FieldCapabilitiesFailure> failureMap = List.of(
            new FieldCapabilitiesFailure(new String[] { "errorindex", "errorindex2" }, new IllegalArgumentException("test"))
        );
        return new FieldCapabilitiesResponse(new String[] { "index1", "index2", "index3", "index4" }, responses, failureMap);
    }

    public void testChunking() {
        AbstractChunkedSerializingTestCase.assertChunkCount(
            FieldCapabilitiesResponseTests.createResponseWithFailures(),
            instance -> instance.getFailures().isEmpty() ? 2 : (3 + instance.get().size() + instance.getFailures().size())
        );

        AbstractChunkedSerializingTestCase.assertChunkCount(createTestInstance(), instance -> 2 + instance.get().size());
    }
}

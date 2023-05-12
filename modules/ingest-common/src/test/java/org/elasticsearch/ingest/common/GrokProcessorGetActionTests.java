/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.grok.PatternBank;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.grok.GrokBuiltinPatterns.ECS_COMPATIBILITY_V1;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;

public class GrokProcessorGetActionTests extends ESTestCase {
    private static final PatternBank LEGACY_TEST_PATTERNS = new PatternBank(Map.of("PATTERN2", "foo2", "PATTERN1", "foo1"));
    private static final PatternBank ECS_TEST_PATTERNS = new PatternBank(Map.of("ECS_PATTERN2", "foo2", "ECS_PATTERN1", "foo1"));

    public void testRequest() throws Exception {
        GrokProcessorGetAction.Request request = new GrokProcessorGetAction.Request(false, GrokProcessor.DEFAULT_ECS_COMPATIBILITY_MODE);
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        GrokProcessorGetAction.Request otherRequest = new GrokProcessorGetAction.Request(streamInput);
        assertThat(otherRequest.validate(), nullValue());
    }

    public void testResponseSerialization() throws Exception {
        GrokProcessorGetAction.Response response = new GrokProcessorGetAction.Response(LEGACY_TEST_PATTERNS.bank());
        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        GrokProcessorGetAction.Response otherResponse = new GrokProcessorGetAction.Response(streamInput);
        assertThat(response.getGrokPatterns(), equalTo(LEGACY_TEST_PATTERNS.bank()));
        assertThat(response.getGrokPatterns(), equalTo(otherResponse.getGrokPatterns()));
    }

    public void testResponseSorting() {
        List<String> sortedKeys = new ArrayList<>(LEGACY_TEST_PATTERNS.bank().keySet());
        Collections.sort(sortedKeys);
        GrokProcessorGetAction.TransportAction transportAction = new GrokProcessorGetAction.TransportAction(
            mock(TransportService.class),
            mock(ActionFilters.class),
            LEGACY_TEST_PATTERNS,
            ECS_TEST_PATTERNS
        );
        GrokProcessorGetAction.Response[] receivedResponse = new GrokProcessorGetAction.Response[1];
        transportAction.doExecute(
            null,
            new GrokProcessorGetAction.Request(true, GrokProcessor.DEFAULT_ECS_COMPATIBILITY_MODE),
            new ActionListener<>() {
                @Override
                public void onResponse(GrokProcessorGetAction.Response response) {
                    receivedResponse[0] = response;
                }

                @Override
                public void onFailure(Exception e) {
                    fail();
                }
            }
        );
        assertThat(receivedResponse[0], notNullValue());
        assertThat(receivedResponse[0].getGrokPatterns().keySet().toArray(), equalTo(sortedKeys.toArray()));

        GrokProcessorGetAction.Response firstResponse = receivedResponse[0];
        transportAction.doExecute(
            null,
            new GrokProcessorGetAction.Request(true, GrokProcessor.DEFAULT_ECS_COMPATIBILITY_MODE),
            new ActionListener<>() {
                @Override
                public void onResponse(GrokProcessorGetAction.Response response) {
                    receivedResponse[0] = response;
                }

                @Override
                public void onFailure(Exception e) {
                    fail();
                }
            }
        );
        assertThat(receivedResponse[0], notNullValue());
        assertThat(receivedResponse[0], not(sameInstance(firstResponse)));
        assertThat(receivedResponse[0].getGrokPatterns(), sameInstance(firstResponse.getGrokPatterns()));
    }

    public void testEcsCompatibilityMode() {
        List<String> sortedKeys = new ArrayList<>(ECS_TEST_PATTERNS.bank().keySet());
        Collections.sort(sortedKeys);
        GrokProcessorGetAction.TransportAction transportAction = new GrokProcessorGetAction.TransportAction(
            mock(TransportService.class),
            mock(ActionFilters.class),
            LEGACY_TEST_PATTERNS,
            ECS_TEST_PATTERNS
        );
        GrokProcessorGetAction.Response[] receivedResponse = new GrokProcessorGetAction.Response[1];
        transportAction.doExecute(null, new GrokProcessorGetAction.Request(true, ECS_COMPATIBILITY_V1), new ActionListener<>() {
            @Override
            public void onResponse(GrokProcessorGetAction.Response response) {
                receivedResponse[0] = response;
            }

            @Override
            public void onFailure(Exception e) {
                fail();
            }
        });
        assertThat(receivedResponse[0], notNullValue());
        assertThat(receivedResponse[0].getGrokPatterns().keySet().toArray(), equalTo(sortedKeys.toArray()));
    }

    @SuppressWarnings("unchecked")
    public void testResponseToXContent() throws Exception {
        GrokProcessorGetAction.Response response = new GrokProcessorGetAction.Response(LEGACY_TEST_PATTERNS.bank());
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            response.toXContent(builder, ToXContent.EMPTY_PARAMS);
            Map<String, Object> converted = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();
            Map<String, String> patterns = (Map<String, String>) converted.get("patterns");
            assertThat(patterns.size(), equalTo(2));
            assertThat(patterns.get("PATTERN1"), equalTo("foo1"));
            assertThat(patterns.get("PATTERN2"), equalTo("foo2"));
        }
    }
}

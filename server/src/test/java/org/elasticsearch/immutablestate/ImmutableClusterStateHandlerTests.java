/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.immutablestate;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.indices.settings.InternalOrPrivateSettingsPlugin;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;

public class ImmutableClusterStateHandlerTests extends ESTestCase {
    public void testValidation() {
        ImmutableClusterStateHandler<ValidRequest> handler = new ImmutableClusterStateHandler<>() {
            @Override
            public String name() {
                return "handler";
            }

            @Override
            public TransformState transform(Object source, TransformState prevState) throws Exception {
                return prevState;
            }
        };

        handler.validate(new ValidRequest());
        assertEquals(
            "Validation error",
            expectThrows(IllegalStateException.class, () -> handler.validate(new InvalidRequest())).getMessage()
        );
    }

    public void testAsMapAndFromMap() throws IOException {
        String someJSON = """
            {
                "persistent": {
                    "indices.recovery.max_bytes_per_sec": "25mb",
                    "cluster": {
                         "remote": {
                             "cluster_one": {
                                 "seeds": [
                                     "127.0.0.1:9300"
                                 ]
                             }
                         }
                    }
                }
            }""";

        ImmutableClusterStateHandler<ValidRequest> persistentHandler = new ImmutableClusterStateHandler<>() {
            @Override
            public String name() {
                return "persistent";
            }

            @Override
            public TransformState transform(Object source, TransformState prevState) throws Exception {
                return prevState;
            }
        };

        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, someJSON)) {
            Map<String, Object> originalMap = parser.map();

            Map<String, ?> internalHandlerMap = Maps.asMap(originalMap.get(persistentHandler.name()));
            assertThat(internalHandlerMap.keySet(), containsInAnyOrder("indices.recovery.max_bytes_per_sec", "cluster"));
            assertEquals(
                "Unsupported input format",
                expectThrows(IllegalStateException.class, () -> Maps.asMap(Integer.valueOf(123))).getMessage()
            );

            try (XContentParser newParser = XContentHelper.mapToXContentParser(XContentParserConfiguration.EMPTY, originalMap)) {
                Map<String, Object> newMap = newParser.map();

                assertThat(newMap.keySet(), containsInAnyOrder("persistent"));
            }
        }
    }

    static class ValidRequest extends MasterNodeRequest<InternalOrPrivateSettingsPlugin.UpdateInternalOrPrivateAction.Request> {
        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    static class InvalidRequest extends ValidRequest {
        @Override
        public ActionRequestValidationException validate() {
            return new ActionRequestValidationException();
        }
    }
}

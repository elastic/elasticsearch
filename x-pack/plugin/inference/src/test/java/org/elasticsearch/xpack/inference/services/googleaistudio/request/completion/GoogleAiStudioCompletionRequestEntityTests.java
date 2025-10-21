/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googleaistudio.request.completion;

import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.googleaistudio.request.GoogleAiStudioCompletionRequestEntity;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.inference.MatchersUtils.equalToIgnoringWhitespaceInJsonString;

public class GoogleAiStudioCompletionRequestEntityTests extends ESTestCase {

    public void testToXContent_WritesSingleMessage() throws IOException {
        var entity = new GoogleAiStudioCompletionRequestEntity(List.of("input"));

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString("""
            {
                "contents": [
                                {
                                    "parts": [
                                        {
                                            "text":"input"
                                        }
                                    ],
                                    "role": "user"
                                }
                            ],
                "generationConfig": {
                    "candidateCount": 1
                }
            }"""));
    }

}

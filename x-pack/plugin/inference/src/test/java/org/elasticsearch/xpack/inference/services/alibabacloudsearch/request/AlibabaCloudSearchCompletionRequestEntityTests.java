/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.alibabacloudsearch.request;

import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.completion.AlibabaCloudSearchCompletionTaskSettings;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.request.completion.AlibabaCloudSearchCompletionRequestEntity;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.inference.MatchersUtils.equalToIgnoringWhitespaceInJsonString;

public class AlibabaCloudSearchCompletionRequestEntityTests extends ESTestCase {
    public void testToXContent_WritesSingleMessage() throws IOException {
        var entity = new AlibabaCloudSearchCompletionRequestEntity(
            List.of("input"),
            AlibabaCloudSearchCompletionTaskSettings.of(null),
            null
        );
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);
        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString("""
            {
                "messages": [
                    {
                        "role": "user",
                        "content": "input"
                    }
                ]
            }
            """));
    }

    public void testToXContent_WritesMultiMessages() throws IOException {
        var entity = new AlibabaCloudSearchCompletionRequestEntity(
            List.of("question1", "answer1", "question2", "answer2", "question3"),
            AlibabaCloudSearchCompletionTaskSettings.of(null),
            null
        );
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);
        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString("""
            {
                "messages": [
                    {
                        "role": "user",
                        "content": "question1"
                    },
                    {
                        "role": "assistant",
                        "content": "answer1"
                    },
                    {
                        "role": "user",
                        "content": "question2"
                    },
                    {
                        "role": "assistant",
                        "content": "answer2"
                    },
                    {
                        "role": "user",
                        "content": "question3"
                    }
                ]
            }
            """));
    }
}

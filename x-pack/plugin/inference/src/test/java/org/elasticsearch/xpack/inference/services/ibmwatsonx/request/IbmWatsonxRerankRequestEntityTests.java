/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ibmwatsonx.request;

import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.rerank.IbmWatsonxRerankTaskSettings;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.inference.MatchersUtils.equalToIgnoringWhitespaceInJsonString;

public class IbmWatsonxRerankRequestEntityTests extends ESTestCase {
    public void testXContent_Request() throws IOException {
        IbmWatsonxRerankTaskSettings taskSettings = new IbmWatsonxRerankTaskSettings(5, true, 100);
        var entity = new IbmWatsonxRerankRequestEntity(
            "database",
            List.of("greenland", "google", "john", "mysql", "potter", "grammar"),
            taskSettings,
            "model",
            "project_id"
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString("""
                   {"model_id":"model",
                    "query":"database",
                    "inputs":[
                      {"text":"greenland"},
                      {"text":"google"},
                      {"text":"john"},
                      {"text":"mysql"},
                      {"text":"potter"},
                      {"text":"grammar"}
                    ],
                   "project_id":"project_id",
                   "parameters":{
                     "truncate_input_tokens":100,
                     "return_options":{
                       "inputs":true,
                       "top_n":5
                       }
                     }
                   }
            """));
    }
}

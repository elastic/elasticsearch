/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;

public class ConnectorSchedulingTests extends ESTestCase {

    public void testToXContent() throws IOException {
        String content = XContentHelper.stripWhitespace("""
            {
                "access_control": {
                    "enabled": false,
                    "interval": "0 0 0 * * ?"
                },
                "full": {
                    "enabled": false,
                    "interval": "0 0 0 * * ?"
                },
                "incremental": {
                    "enabled": false,
                    "interval": "0 0 0 * * ?"
                }
            }""");

        ConnectorScheduling scheduling = ConnectorScheduling.fromXContentBytes(new BytesArray(content), XContentType.JSON);
        boolean humanReadable = true;
        BytesReference originalBytes = toShuffledXContent(scheduling, XContentType.JSON, ToXContent.EMPTY_PARAMS, humanReadable);
        ConnectorScheduling parsed;
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), originalBytes)) {
            parsed = ConnectorScheduling.fromXContent(parser);
        }
        assertToXContentEquivalent(originalBytes, toXContent(parsed, XContentType.JSON, humanReadable), XContentType.JSON);
    }
}

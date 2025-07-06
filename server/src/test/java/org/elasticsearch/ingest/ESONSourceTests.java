/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

public class ESONSourceTests extends ESTestCase {

    public void testParse() throws Exception {
        String jsonString = XContentHelper.convertToJson(
            new BytesArray("{\"field\": false, \"field2\": [922337203685477580, 92233720368547758, 2, 3, 4.3], \"field3\": null}"),
            true,
            XContentType.JSON
        );

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, jsonString)) {
            ESONSource.Builder builder = new ESONSource.Builder();
            builder.parse(parser);
        }
    }
}

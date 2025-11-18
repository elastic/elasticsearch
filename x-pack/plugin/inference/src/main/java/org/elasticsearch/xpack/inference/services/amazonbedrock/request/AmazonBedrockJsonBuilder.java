/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.request;

import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

public class AmazonBedrockJsonBuilder {

    private final ToXContent jsonWriter;

    public AmazonBedrockJsonBuilder(ToXContent jsonWriter) {
        this.jsonWriter = jsonWriter;
    }

    public String getStringContent() throws IOException {
        try (var builder = jsonBuilder()) {
            return Strings.toString(jsonWriter.toXContent(builder, null));
        }
    }
}

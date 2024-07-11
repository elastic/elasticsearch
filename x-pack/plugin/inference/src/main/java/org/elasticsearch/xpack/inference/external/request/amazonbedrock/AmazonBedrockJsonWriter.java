/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.amazonbedrock;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;

/**
 * This is needed as the input for the Amazon Bedrock SDK does not like
 * the formatting of XContent JSON output
 */
public interface AmazonBedrockJsonWriter {
    JsonGenerator writeJson(JsonGenerator generator) throws IOException;
}

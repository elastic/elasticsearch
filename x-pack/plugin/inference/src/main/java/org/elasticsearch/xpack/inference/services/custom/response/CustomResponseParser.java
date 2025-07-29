/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.custom.response;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xpack.inference.external.http.HttpResult;

import java.io.IOException;

public interface CustomResponseParser extends ToXContentFragment, NamedWriteable {
    InferenceServiceResults parse(HttpResult response) throws IOException;
}

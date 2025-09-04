/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class UpdateIndexMigrationVersionResponse extends ActionResponse {
    public UpdateIndexMigrationVersionResponse(StreamInput in) throws IOException {}

    public UpdateIndexMigrationVersionResponse() {}

    @Override
    public void writeTo(StreamOutput out) throws IOException {}
}

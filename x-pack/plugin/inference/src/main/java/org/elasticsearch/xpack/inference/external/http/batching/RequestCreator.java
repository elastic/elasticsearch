/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.batching;

import org.apache.http.client.protocol.HttpClientContext;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.inference.external.http.HttpResult;

import java.util.List;

/**
 * Provides a contract for creating a {@link Runnable} to execute.
 * @param <GroupingKey> the criteria to group requests together to be sent as a single batch
 */
public interface RequestCreator<GroupingKey> {
    Runnable createRequest(
        List<String> input,
        BatchingComponents components,
        HttpClientContext context,
        ActionListener<HttpResult> listener
    );

    GroupingKey key();
}

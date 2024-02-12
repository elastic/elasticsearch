/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;

import java.io.Closeable;

public interface Sender extends Closeable {
    void start();

    void send(HttpRequest request, ActionListener<HttpResult> listener);

    void send(HttpRequest request, @Nullable TimeValue timeout, ActionListener<HttpResult> listener);
}

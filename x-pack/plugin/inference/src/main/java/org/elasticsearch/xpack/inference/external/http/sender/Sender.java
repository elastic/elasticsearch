/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.inference.external.http.batching.RequestCreator;

import java.io.Closeable;
import java.util.List;

public interface Sender<K> extends Closeable {
    void start();

    void send(RequestCreator<K> requestCreator, List<String> input, ActionListener<InferenceServiceResults> listener);

    void send(RequestCreator<K> requestCreator, List<String> input, TimeValue timeout, ActionListener<InferenceServiceResults> listener);
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.batching;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.inference.external.http.HttpResult;

import java.util.List;

public interface BatchableRequest<K> {
    TransactionHandler<K> handler();

    List<String> input();

    ActionListener<HttpResult> listener();
}

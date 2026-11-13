/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.streaming;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.compute.data.Page;

import java.util.List;

public interface StreamStorage {

    void create(String streamId, long expirationMillis, ActionListener<Void> listener);

    void extendTtl(String streamId, long expirationMillis, ActionListener<Void> listener);

    void appendPage(String streamingId, int offset, Page page, ActionListener<Void> listener);

    void markComplete(String streamId, int totalRows, int totalPages, ActionListener<Void> listener);

    void markFailed(String streamId, ActionListener<Void> listener);

    void getMetadata(String streamId, ActionListener<StreamMetadata> listener);

    void getPages(String streamId, int offset, int rows, ActionListener<List<Page>> listener);

    void delete(String streamId, ActionListener<Void> listener);
}

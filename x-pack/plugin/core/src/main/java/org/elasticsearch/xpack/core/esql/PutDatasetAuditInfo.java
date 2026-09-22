/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.esql;

/**
 * Implemented by {@code PutDatasetAction.Request} so the audit trail can record a dataset change
 * event without depending on the ES|QL plugin's request class.
 */
public interface PutDatasetAuditInfo {
    String datasetName();

    String datasetDataSource();

    String datasetResource();
}

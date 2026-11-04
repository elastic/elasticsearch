/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.mount;

import org.elasticsearch.action.ActionType;

/** Reads the footers of Parquet files and says what a mount would create, without creating anything. */
public final class PreviewParquetAction extends ActionType<ParquetMountResponse> {

    public static final PreviewParquetAction INSTANCE = new PreviewParquetAction();
    public static final String NAME = "cluster:admin/parquet/preview";

    private PreviewParquetAction() {
        super(NAME);
    }
}

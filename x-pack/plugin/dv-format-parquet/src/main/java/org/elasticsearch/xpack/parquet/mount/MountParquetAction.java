/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.mount;

import org.elasticsearch.action.ActionType;

/** Mounts Parquet files as the backing indices of a data stream. */
public final class MountParquetAction extends ActionType<ParquetMountResponse> {

    public static final MountParquetAction INSTANCE = new MountParquetAction();
    public static final String NAME = "cluster:admin/parquet/mount";

    private MountParquetAction() {
        super(NAME);
    }
}

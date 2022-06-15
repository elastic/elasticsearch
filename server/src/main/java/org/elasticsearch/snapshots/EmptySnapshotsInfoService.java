/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.snapshots;

import java.util.Map;

public class EmptySnapshotsInfoService implements SnapshotsInfoService {
    public static final EmptySnapshotsInfoService INSTANCE = new EmptySnapshotsInfoService();

    @Override
    public SnapshotShardSizeInfo snapshotShardSizes() {
        return new SnapshotShardSizeInfo(Map.of());
    }
}

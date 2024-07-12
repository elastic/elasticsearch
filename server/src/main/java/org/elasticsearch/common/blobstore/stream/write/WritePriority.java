/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.blobstore.stream.write;

public enum WritePriority {
    // Used for segment transfers during refresh, flush or merges
    NORMAL,
    // Used for transfer of translog or ckp files.
    HIGH,
    // Used for transfer of remote cluster state
    URGENT,
    // All other background transfers such as in snapshot recovery, recovery from local store or index etc.
    LOW
}

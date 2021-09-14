/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.watcher;

import java.util.Objects;

public class WatcherMetadata {

    private final boolean manuallyStopped;

    public WatcherMetadata(boolean manuallyStopped) {
        this.manuallyStopped = manuallyStopped;
    }

    public boolean manuallyStopped() {
        return manuallyStopped;
    }
    @Override
    public String toString() {
        return "manuallyStopped["+ manuallyStopped +"]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        WatcherMetadata action = (WatcherMetadata) o;

        return manuallyStopped == action.manuallyStopped;
    }

    @Override
    public int hashCode() {
        return Objects.hash(manuallyStopped);
    }
}

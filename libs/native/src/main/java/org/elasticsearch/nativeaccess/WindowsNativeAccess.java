/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess;

class WindowsNativeAccess extends AbstractNativeAccess {

    @Override
    public boolean definitelyRunningAsRoot() {
        return false; // don't know
    }

    @Override
    public void tryLockMemory() {

    }

    @Override
    public void trySetMaxNumberOfThreads() {
        // no way to set limit for number of threads in Windows
    }

    @Override
    public void trySetMaxVirtualMemorySize() {
        // no way to set limit for virtual memory size in Windows
    }

    @Override
    public void trySetMaxFileSize() {
        // no way to set limit for max file size in Windows
    }
}

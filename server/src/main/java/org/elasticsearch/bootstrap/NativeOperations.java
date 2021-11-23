/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.bootstrap;

import java.nio.file.Path;
import java.util.OptionalLong;

public interface NativeOperations {

    boolean tryLockMemory();

    boolean definitelyRunningAsRoot();

    String getShortPathName(String path);

    void addConsoleCtrlHandler(ConsoleCtrlHandler handler);

    int tryInstallSystemCallFilter(Path tmpFile);

    OptionalLong tryRetrieveMaxNumberOfThreads();

    OptionalLong tryRetrieveMaxVirtualMemorySize();

    OptionalLong tryRetrieveMaxFileSize();

}

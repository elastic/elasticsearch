/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.process;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * Interface for the controller that ML uses to start native processes.
 */
public interface MlController {

    /**
     * Get build/version information about the native code being used by the ML plugin.
     * @return A map containing native code build/version information.
     *         This map will be empty in the case where no native code exists.
     * @throws TimeoutException If native code information cannot be obtained
     *                          within a reasonable amount of time.
     */
    Map<String, Object> getNativeCodeInfo() throws TimeoutException;

    /**
     * Stop the controller.  For implementations where the controller is an external
     * process this will instruct that external process to exit, thus preventing any
     * subsequent controller operations from working.  Stopping the controller is
     * irreversible; the only way to restart a controller is to restart the
     * Elasticsearch node.
     * @throws IOException If it is not possible to communicate the stop request to
     *                     the controller.
     */
    void stop() throws IOException;
}

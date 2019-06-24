/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.process;

import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.core.ml.MachineLearningField;
import org.elasticsearch.xpack.ml.utils.NamedPipeHelper;

import java.io.IOException;

/**
 * Manages a singleton NativeController so that both the MachineLearningInfoTransportAction and MachineLearning classes can
 * get access to the same one.
 */
public class NativeControllerHolder {

    private static final Object lock = new Object();
    private static NativeController nativeController;

    private NativeControllerHolder() {
    }

    /**
     * Get a reference to the singleton native process controller.
     *
     * The NativeController is created lazily to allow time for the C++ process to be started before connection is attempted.
     *
     * <code>null</code> is returned to tests where xpack.ml.autodetect_process=false.
     *
     * Calls may throw an exception if initial connection to the C++ process fails.
     */
    public static NativeController getNativeController(String localNodeName, Environment environment) throws IOException {

        if (MachineLearningField.AUTODETECT_PROCESS.get(environment.settings())) {
            synchronized (lock) {
                if (nativeController == null) {
                    nativeController = new NativeController(localNodeName, environment, new NamedPipeHelper());
                    nativeController.tailLogsInThread();
                }
            }
            return nativeController;
        }
        return null;
    }

    /**
     * Get a reference to the singleton native process controller.
     *
     * Assumes that if it is possible for a native controller to exist that it will already have been created.
     * Designed for use by objects that don't have access to the environment but know a native controller must exist
     * for the object calling this method to exist.
     */
    public static NativeController getNativeController() {
        return nativeController;
    }

}

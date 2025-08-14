/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.gpu;

import com.nvidia.cuvs.CuVSResources;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

public class GPUSupport {

    private static final Logger LOG = LogManager.getLogger(GPUSupport.class);

    /** Tells whether the platform supports cuvs. */
    public static boolean isSupported(boolean logError) {
        try (var resources = cuVSResourcesOrNull(logError)) {
            if (resources != null) {
                return true;
            }
        }
        return false;
    }

    /** Returns a resources if supported, otherwise null. */
    public static CuVSResources cuVSResourcesOrNull(boolean logError) {
        try {
            var resources = CuVSResources.create();
            return resources;
        } catch (UnsupportedOperationException uoe) {
            if (logError) {
                String msg = "";
                if (uoe.getMessage() == null) {
                    msg = "Runtime Java version: " + Runtime.version().feature();
                } else {
                    msg = ": " + uoe.getMessage();
                }
                LOG.warn("GPU based vector indexing is not supported on this platform or java version; " + msg);
            }
        } catch (Throwable t) {
            if (logError) {
                if (t instanceof ExceptionInInitializerError ex) {
                    t = ex.getCause();
                }
                LOG.warn("Exception occurred during creation of cuvs resources", t);
            }
        }
        return null;
    }
}

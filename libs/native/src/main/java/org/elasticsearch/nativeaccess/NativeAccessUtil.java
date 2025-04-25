/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.nativeaccess;

public class NativeAccessUtil {
    /**
     * Enables native access for the provided module.
     * We need to have this adapter even if the method is available in JDK 21, as it was in preview.
     * Available to JDK 22+, required for JDK 24+ when using --illegal-native-access=deny
     */
    public static void enableNativeAccess(ModuleLayer.Controller controller, Module module) {
        controller.enableNativeAccess(module);
    }

    public static boolean isNativeAccessEnabled(Module module) {
        return module.isNativeAccessEnabled();
    }
}

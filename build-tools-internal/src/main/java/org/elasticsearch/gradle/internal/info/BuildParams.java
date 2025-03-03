/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.gradle.internal.info;

import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.function.Consumer;

@Deprecated
public class BuildParams {
    private static Boolean isCi;

    /**
     * Initialize global build parameters. This method accepts and a initialization function which in turn accepts a
     * {@link MutableBuildParams}. Initialization can be done in "stages", therefore changes override existing values, and values from
     * previous calls to {@link #init(Consumer)} carry forward. In cases where you want to clear existing values
     * {@link MutableBuildParams#reset()} may be used.
     *
     * @param initializer Build parameter initializer
     */
    public static void init(Consumer<MutableBuildParams> initializer) {
        initializer.accept(MutableBuildParams.INSTANCE);
    }

    public static Boolean isCi() {
        return value(isCi);
    }

    private static <T> T value(T object) {
        if (object == null) {
            String callingMethod = Thread.currentThread().getStackTrace()[2].getMethodName();

            throw new IllegalStateException(
                "Build parameter '"
                    + propertyName(callingMethod)
                    + "' has not been initialized.\n"
                    + "Perhaps the plugin responsible for initializing this property has not been applied."
            );
        }

        return object;
    }

    private static String propertyName(String methodName) {
        String propertyName = methodName.startsWith("is") ? methodName.substring("is".length()) : methodName.substring("get".length());
        return propertyName.substring(0, 1).toLowerCase() + propertyName.substring(1);
    }

    public static class MutableBuildParams {
        private static MutableBuildParams INSTANCE = new MutableBuildParams();

        private MutableBuildParams() {}

        /**
         * Resets any existing values from previous initializations.
         */
        public void reset() {
            Arrays.stream(BuildParams.class.getDeclaredFields()).filter(f -> Modifier.isStatic(f.getModifiers())).forEach(f -> {
                try {
                    // Since we are mutating private static fields from a public static inner class we need to suppress
                    // accessibility controls here.
                    f.setAccessible(true);
                    f.set(null, null);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        public void setIsCi(boolean isCi) {
            BuildParams.isCi = isCi;
        }
    }
}

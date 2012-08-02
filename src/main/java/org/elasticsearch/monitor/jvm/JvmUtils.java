/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.monitor.jvm;

import java.lang.reflect.Field;

/**
 */
public class JvmUtils {

    /**
     * True iff running on a 64bit JVM
     */
    public static final boolean JRE_IS_64BIT;

    public static final boolean JRE_IS_MINIMUM_JAVA6;
    public static final boolean JRE_IS_MINIMUM_JAVA7;

    static {
        boolean is64Bit = false;
        try {
            final Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
            final Field unsafeField = unsafeClass.getDeclaredField("theUnsafe");
            unsafeField.setAccessible(true);
            final Object unsafe = unsafeField.get(null);
            final int addressSize = ((Number) unsafeClass.getMethod("addressSize")
                    .invoke(unsafe)).intValue();
            //System.out.println("Address size: " + addressSize);
            is64Bit = addressSize >= 8;
        } catch (Throwable e) {
            final String x = System.getProperty("sun.arch.data.model");
            if (x != null) {
                is64Bit = x.indexOf("64") != -1;
            } else {
                String OS_ARCH = System.getProperty("os.arch");
                if (OS_ARCH != null && OS_ARCH.indexOf("64") != -1) {
                    is64Bit = true;
                } else {
                    is64Bit = false;
                }
            }
        }
        JRE_IS_64BIT = is64Bit;

        // this method only exists in Java 6:
        boolean v6 = true;
        try {
            String.class.getMethod("isEmpty");
        } catch (NoSuchMethodException nsme) {
            v6 = false;
        }
        JRE_IS_MINIMUM_JAVA6 = v6;

        // this method only exists in Java 7:
        boolean v7 = true;
        try {
            Throwable.class.getMethod("getSuppressed");
        } catch (NoSuchMethodException nsme) {
            v7 = false;
        }
        JRE_IS_MINIMUM_JAVA7 = v7;
    }

}

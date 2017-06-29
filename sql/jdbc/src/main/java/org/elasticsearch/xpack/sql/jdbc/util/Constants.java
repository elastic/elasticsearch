/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.util;

import java.util.StringTokenizer;

//taken from Apache Lucene
public abstract class Constants {

    /** JVM vendor info. */
    public static final String JVM_VENDOR = System.getProperty("java.vm.vendor");
    public static final String JVM_VERSION = System.getProperty("java.vm.version");
    public static final String JVM_NAME = System.getProperty("java.vm.name");
    public static final String JVM_SPEC_VERSION = System.getProperty("java.specification.version");

    /** The value of <tt>System.getProperty("java.version")</tt>. **/
    public static final String JAVA_VERSION = System.getProperty("java.version");

    public static final String OS_ARCH = System.getProperty("os.arch");
    public static final String OS_VERSION = System.getProperty("os.version");
    public static final String JAVA_VENDOR = System.getProperty("java.vendor");

    private static final int JVM_MAJOR_VERSION;
    private static final int JVM_MINOR_VERSION;

    /** True iff running on a 64bit JVM */
    public static final boolean JRE_IS_64BIT;

    static {
        final StringTokenizer st = new StringTokenizer(JVM_SPEC_VERSION, ".");
        JVM_MAJOR_VERSION = Integer.parseInt(st.nextToken());
        if (st.hasMoreTokens()) {
            JVM_MINOR_VERSION = Integer.parseInt(st.nextToken());
        }
        else {
            JVM_MINOR_VERSION = 0;
        }
        boolean is64Bit = false;
        final String x = System.getProperty("sun.arch.data.model");
        if (x != null) {
            is64Bit = x.contains("64");
        }
        else {
            if (OS_ARCH != null && OS_ARCH.contains("64")) {
                is64Bit = true;
            }
            else {
                is64Bit = false;
            }
        }
        JRE_IS_64BIT = is64Bit;
    }

    public static final boolean JRE_IS_MINIMUM_JAVA7 = JVM_MAJOR_VERSION > 1 || (JVM_MAJOR_VERSION == 1 && JVM_MINOR_VERSION >= 7);
    public static final boolean JRE_IS_MINIMUM_JAVA8 = JVM_MAJOR_VERSION > 1 || (JVM_MAJOR_VERSION == 1 && JVM_MINOR_VERSION >= 8);
    public static final boolean JRE_IS_MINIMUM_JAVA9 = JVM_MAJOR_VERSION > 1 || (JVM_MAJOR_VERSION == 1 && JVM_MINOR_VERSION >= 9);

}
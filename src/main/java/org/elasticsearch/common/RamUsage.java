/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common;

/**
 *
 */
public class RamUsage {

    private static final String OS_ARCH = System.getProperty("os.arch");
    private static final boolean JRE_IS_64BIT;

    static {
        String x = System.getProperty("sun.arch.data.model");
        if (x != null) {
            JRE_IS_64BIT = x.indexOf("64") != -1;
        } else {
            if (OS_ARCH != null && OS_ARCH.indexOf("64") != -1) {
                JRE_IS_64BIT = true;
            } else {
                JRE_IS_64BIT = false;
            }
        }
    }

    public final static int NUM_BYTES_SHORT = 2;
    public final static int NUM_BYTES_INT = 4;
    public final static int NUM_BYTES_LONG = 8;
    public final static int NUM_BYTES_FLOAT = 4;
    public final static int NUM_BYTES_DOUBLE = 8;
    public final static int NUM_BYTES_CHAR = 2;
    public final static int NUM_BYTES_OBJECT_HEADER = 8;
    public final static int NUM_BYTES_OBJECT_REF = JRE_IS_64BIT ? 8 : 4;
    public final static int NUM_BYTES_ARRAY_HEADER = NUM_BYTES_OBJECT_HEADER + NUM_BYTES_INT + NUM_BYTES_OBJECT_REF;

}

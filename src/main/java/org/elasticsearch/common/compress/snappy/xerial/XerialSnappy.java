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

package org.elasticsearch.common.compress.snappy.xerial;

import org.xerial.snappy.Snappy;

import java.io.PrintStream;

/**
 */
public class XerialSnappy {

    public static final boolean available;
    public static final Throwable failure;

    static {
        Throwable failureX = null;
        boolean availableX;
        // Yuck!, we need to do this since snappy 1.0.4.1 does e.printStackTrace
        // when failing to load the snappy library, and we don't want it displayed...
        PrintStream err = System.err;
        try {
            System.setErr(null);
            byte[] tests = Snappy.compress("test");
            Snappy.uncompressString(tests);
            availableX = true;
        } catch (Throwable e) {
            availableX = false;
            failureX = e;
        } finally {
            System.setErr(err);
        }
        available = availableX;
        failure = failureX;
    }
}

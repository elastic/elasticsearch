/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package org.elasticsearch.plugin.mapper.attachments.tika;

import org.apache.lucene.util.Constants;

import java.util.StringTokenizer;

import static java.lang.Integer.parseInt;

public class LocaleChecker {
    public static int JVM_MAJOR_VERSION = 0;
    public static int JVM_MINOR_VERSION = 0;
    public static int JVM_PATCH_MAJOR_VERSION = 0;
    public static int JVM_PATCH_MINOR_VERSION = 0;

    static {
        StringTokenizer st = new StringTokenizer(Constants.JVM_SPEC_VERSION, ".");
        JVM_MAJOR_VERSION = parseInt(st.nextToken());
        if(st.hasMoreTokens()) {
            JVM_MINOR_VERSION = parseInt(st.nextToken());
        }
        if(st.hasMoreTokens()) {
            StringTokenizer stPatch = new StringTokenizer(st.nextToken(), "_");
            JVM_PATCH_MAJOR_VERSION = parseInt(stPatch.nextToken());
            JVM_PATCH_MINOR_VERSION = parseInt(stPatch.nextToken());
        }
    }

    /**
     * Tika 1.8 fixed currently known Locale issues with some JVMs
     */
    public static boolean isLocaleCompatible() {
        return true;
    }
}

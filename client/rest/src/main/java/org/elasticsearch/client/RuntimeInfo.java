/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

class RuntimeInfo {

    /**
     * Returns runtime info to append to the metadata header. Currently looks up classes identifying non-Java JVM
     * languages and appends their major/minor version patch.
     */
    public static String getRuntimeMetadata() {
        StringBuilder s = new StringBuilder();
        String version;

        version = HlrcKind();
        if (version != null) {
            s.append(",hl=").append(version);
        }

        version= kotlinVersion();
        if (version != null) {
            s.append(",kt=").append(version);
        }

        version = scalaVersion();
        if (version != null) {
            s.append(",sc=").append(version);
        }

        version = groovyVersion();
        if (version != null) {
            s.append(",clj=").append(version);
        }

        version = groovyVersion();
        if (version != null) {
            s.append(",gy=").append(version);
        }

        return s.toString();
    }

    public static String HlrcKind() {
        try {
            Class.forName("org.elasticsearch.client.RestHighLevelClient");
        } catch (Exception t) {
            return null;
        }
        // 1 is the HLRC based on ES server request/response classes
        // 2 will be the HLRC based on code generation from API specs
        return "1";
    }

    public static String kotlinVersion() {
        try {
            //KotlinVersion.CURRENT.toString()
            Class<?> clazz = Class.forName("kotlin.KotlinVersion");
            Field field = clazz.getField("CURRENT");
            String version = field.get(null).toString();
            return stripPatchRevision(version);

        } catch (Exception t) {
            // ignore
        }
        return null;
    }

    public static String scalaVersion() {
        try {
            // scala.util.Properties.versionNumberString()
            Class<?> clazz = Class.forName("scala.util.Properties");
            Method m = clazz.getMethod("versionNumberString");
            String version = (String) m.invoke(null);
            return stripPatchRevision(version);

        } catch (Exception t) {
            // ignore
        }
        return null;
    }

    public static String clojureVersion() {
        try {
            // (clojure-version) which translates to
            // clojure.core$clojure_version.invokeStatic()
            Class<?> clazz = Class.forName("clojure.core$clojure_version");
            Method m = clazz.getMethod("invokeStatic");
            String version = (String) m.invoke(null);
            return stripPatchRevision(version);

        } catch (Exception t) {
            // ignore
        }
        return null;
    }

    public static String groovyVersion() {
        try {
            // groovy.lang.GroovySystem.getVersion()
            // There's also getShortVersion(), but only since Groovy 3.0.1
            Class<?> clazz = Class.forName("groovy.lang.GroovySystem");
            Method m = clazz.getMethod("getVersion");
            String version = (String) m.invoke(null);
            return stripPatchRevision(version);

        } catch (Exception t) {
            // ignore
        }
        return null;
    }

    static String stripPatchRevision(String version) {
        if (version == null) {
            return null;
        }

        int firstDot = version.indexOf('.');
        int secondDot = version.indexOf('.', firstDot + 1);
        if (secondDot < 0) {
            return version;
        } else {
            return version.substring(0, secondDot);
        }
    }
}

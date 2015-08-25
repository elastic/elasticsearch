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

package org.elasticsearch.bootstrap;

import org.elasticsearch.common.Strings;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class JavaVersion implements Comparable<JavaVersion> {
    private final List<Integer> version;

    public List<Integer> getVersion() {
        return Collections.unmodifiableList(version);
    }

    private JavaVersion(List<Integer> version) {
        this.version = version;
    }

    public static JavaVersion parse(String value) {
        if (value == null) {
            throw new NullPointerException("value");
        }
        if ("".equals(value)) {
            throw new IllegalArgumentException("value");
        }

        List<Integer> version = new ArrayList<>();
        String[] components = value.split("\\.");
        for (String component : components) {
            version.add(Integer.valueOf(component));
        }

        return new JavaVersion(version);
    }

    public static boolean isValid(String value) {
        return value.matches("^0*[0-9]+(\\.[0-9]+)*$");
    }

    private final static JavaVersion CURRENT = parse(System.getProperty("java.specification.version"));

    public static JavaVersion current() {
        return CURRENT;
    }

    @Override
    public int compareTo(JavaVersion o) {
        int len = Math.max(version.size(), o.version.size());
        for (int i = 0; i < len; i++) {
            int d = (i < version.size() ? version.get(i) : 0);
            int s = (i < o.version.size() ? o.version.get(i) : 0);
            if (s < d)
                return 1;
            if (s > d)
                return -1;
        }
        return 0;
    }

    @Override
    public String toString() {
        return Strings.collectionToDelimitedString(version, ".");
    }
}

/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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
package org.elasticsearch.util.yaml.snakeyaml.nodes;

import org.elasticsearch.util.yaml.snakeyaml.error.YAMLException;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.*;

public final class Tag implements Comparable<Tag> {
    public static final String PREFIX = "tag:yaml.org,2002:";
    public static final Tag YAML = new Tag(PREFIX + "yaml");
    public static final Tag VALUE = new Tag(PREFIX + "value");
    public static final Tag MERGE = new Tag(PREFIX + "merge");
    public static final Tag SET = new Tag(PREFIX + "set");
    public static final Tag PAIRS = new Tag(PREFIX + "pairs");
    public static final Tag OMAP = new Tag(PREFIX + "omap");
    public static final Tag BINARY = new Tag(PREFIX + "binary");
    public static final Tag INT = new Tag(PREFIX + "int");
    public static final Tag FLOAT = new Tag(PREFIX + "float");
    public static final Tag TIMESTAMP = new Tag(PREFIX + "timestamp");
    public static final Tag BOOL = new Tag(PREFIX + "bool");
    public static final Tag NULL = new Tag(PREFIX + "null");
    public static final Tag STR = new Tag(PREFIX + "str");
    public static final Tag SEQ = new Tag(PREFIX + "seq");
    public static final Tag MAP = new Tag(PREFIX + "map");
    public static final Map<Tag, Set<Class<?>>> COMPATIBILITY_MAP;

    static {
        COMPATIBILITY_MAP = new HashMap<Tag, Set<Class<?>>>();
        Set<Class<?>> floatSet = new HashSet<Class<?>>();
        floatSet.add(Double.class);
        floatSet.add(Float.class);
        floatSet.add(BigDecimal.class);
        COMPATIBILITY_MAP.put(FLOAT, floatSet);
        //
        Set<Class<?>> intSet = new HashSet<Class<?>>();
        intSet.add(Integer.class);
        intSet.add(Long.class);
        intSet.add(BigInteger.class);
        COMPATIBILITY_MAP.put(INT, intSet);
        //
        Set<Class<?>> timestampSet = new HashSet<Class<?>>();
        timestampSet.add(Date.class);
        timestampSet.add(java.sql.Date.class);
        timestampSet.add(Timestamp.class);
        COMPATIBILITY_MAP.put(TIMESTAMP, timestampSet);
    }

    private final String value;

    public Tag(String tag) {
        if (tag == null) {
            throw new NullPointerException("Tag must be provided.");
        } else if (tag.length() == 0) {
            throw new IllegalArgumentException("Tag must not be empty.");
        } else if (tag.trim().length() != tag.length()) {
            throw new IllegalArgumentException("Tag must not contain leading or trailing spaces.");
        }
        this.value = tag;
    }

    public Tag(Class<? extends Object> clazz) {
        if (clazz == null) {
            throw new NullPointerException("Class for tag must be provided.");
        }
        this.value = Tag.PREFIX + clazz.getName();
    }

    public String getValue() {
        return value;
    }

    public boolean startsWith(String prefix) {
        return value.startsWith(prefix);
    }

    public String getClassName() {
        if (!value.startsWith(Tag.PREFIX)) {
            throw new YAMLException("Unknown tag: " + value);
        }
        return value.substring(Tag.PREFIX.length());
    }

    public int getLength() {
        return value.length();
    }

    @Override
    public String toString() {
        return value;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj instanceof Tag) {
            return value.equals(((Tag) obj).getValue());
        } else if (obj instanceof String) {
            if (value.equals(obj.toString())) {
                // TODO to be removed later (version 2.0?)
                System.err.println("Comparing Tag and String is deprecated.");
                return true;
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    /**
     * Java has more then 1 class compatible with a language-independent tag
     * (!!int, !!float, !!timestamp etc)
     *
     * @param clazz - Class to check compatibility
     * @return true when the Class can be represented by this
     *         language-independent tag
     */
    public boolean isCompatible(Class<?> clazz) {
        Set<Class<?>> set = COMPATIBILITY_MAP.get(this);
        if (set != null) {
            return set.contains(clazz);
        } else {
            return false;
        }
    }

    /**
     * Check whether this tag matches the global tag for the Class
     *
     * @param clazz - Class to check
     * @return true when the this tag can be used as a global tag for the Class
     */
    public boolean matches(Class<? extends Object> clazz) {
        return value.equals(Tag.PREFIX + clazz.getName());
    }

    public int compareTo(Tag o) {
        return value.compareTo(o.getValue());
    }
}

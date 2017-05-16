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

package org.elasticsearch.test.rest.yaml;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Allows to cache the last obtained test response and or part of it within variables
 * that can be used as input values in following requests and assertions.
 */
public class Stash implements ToXContent {
    private static final Pattern EXTENDED_KEY = Pattern.compile("\\$\\{([^}]+)\\}");

    private static final Logger logger = Loggers.getLogger(Stash.class);

    public static final Stash EMPTY = new Stash();

    private final Map<String, Object> stash = new HashMap<>();
    private final ObjectPath stashObjectPath = new ObjectPath(stash);

    /**
     * Allows to saved a specific field in the stash as key-value pair
     */
    public void stashValue(String key, Object value) {
        logger.trace("stashing [{}]=[{}]", key, value);
        Object old = stash.put(key, value);
        if (old != null && old != value) {
            logger.trace("replaced stashed value [{}] with same key [{}]", old, key);
        }
    }

    /**
     * Clears the previously stashed values
     */
    public void clear() {
        stash.clear();
    }

    /**
     * Tells whether a particular key needs to be looked up in the stash based on its name.
     * Returns true if the string representation of the key starts with "$", false otherwise
     * The stash contains fields eventually extracted from previous responses that can be reused
     * as arguments for following requests (e.g. scroll_id)
     */
    public boolean containsStashedValue(Object key) {
        if (key == null || false == key instanceof CharSequence) {
            return false;
        }
        String stashKey = key.toString();
        if (false == Strings.hasLength(stashKey)) {
            return false;
        }
        if (stashKey.startsWith("$")) {
            return true;
        }
        return EXTENDED_KEY.matcher(stashKey).find();
    }

    /**
     * Retrieves a value from the current stash.
     * The stash contains fields eventually extracted from previous responses that can be reused
     * as arguments for following requests (e.g. scroll_id)
     */
    public Object getValue(String key) throws IOException {
        if (key.charAt(0) == '$' && key.charAt(1) != '{') {
            return unstash(key.substring(1));
        }
        Matcher matcher = EXTENDED_KEY.matcher(key);
        /*
         * String*Buffer* because that is what the Matcher API takes. In modern versions of java the uncontended synchronization is very,
         * very cheap so that should not be a problem.
         */
        StringBuffer result = new StringBuffer(key.length());
        if (false == matcher.find()) {
            throw new IllegalArgumentException("Doesn't contain any stash keys [" + key + "]");
        }
        do {
            matcher.appendReplacement(result, Matcher.quoteReplacement(unstash(matcher.group(1)).toString()));
        } while (matcher.find());
        matcher.appendTail(result);
        return result.toString();
    }

    private Object unstash(String key) throws IOException {
        Object stashedValue = stashObjectPath.evaluate(key);
        if (stashedValue == null) {
            throw new IllegalArgumentException("stashed value not found for key [" + key + "]");
        }
        return stashedValue;
    }

    /**
     * Goes recursively against each map entry and replaces any string value starting with "$" with its
     * corresponding value retrieved from the stash
     */
    @SuppressWarnings("unchecked") // Safe because we check that all the map keys are string in unstashObject
    public Map<String, Object> replaceStashedValues(Map<String, Object> map) throws IOException {
        return (Map<String, Object>) unstashObject(map);
    }

    private Object unstashObject(Object obj) throws IOException {
        if (obj instanceof List) {
            List<?> list = (List<?>) obj;
            List<Object> result = new ArrayList<>();
            for (Object o : list) {
                if (containsStashedValue(o)) {
                    result.add(getValue(o.toString()));
                } else {
                    result.add(unstashObject(o));
                }
            }
            return result;
        }
        if (obj instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) obj;
            Map<String, Object> result = new HashMap<>();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                String key = (String) entry.getKey();
                Object value = entry.getValue();
                if (containsStashedValue(key)) {
                    key = getValue(key).toString();
                }
                if (containsStashedValue(value)) {
                    value = getValue(value.toString());
                } else {
                    value = unstashObject(value);
                }
                if (null != result.putIfAbsent(key, value)) {
                    throw new IllegalArgumentException("Unstashing has caused a key conflict! The map is [" + result + "] and the key is ["
                            + entry.getKey() + "] which unstashes to [" + key + "]");
                }
            }
            return result;
        }
        return obj;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("stash", stash);
        return builder;
    }
}

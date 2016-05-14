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

package org.elasticsearch.test.rest;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.rest.client.RestResponse;

/**
 * Allows to cache the last obtained test response and or part of it within variables
 * that can be used as input values in following requests and assertions.
 */
public class Stash implements ToXContent {

    private static final ESLogger logger = Loggers.getLogger(Stash.class);

    public static final Stash EMPTY = new Stash();

    private final Map<String, Object> stash = new HashMap<>();
    private RestResponse response;

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

    public void stashResponse(RestResponse response) throws IOException {
        // TODO we can almost certainly save time by lazily evaluating the body
        stashValue("body", response.getBody());
        this.response = response;
    }

    /**
     * Clears the previously stashed values
     */
    public void clear() {
        stash.clear();
    }

    /**
     * Tells whether a particular value needs to be looked up in the stash
     * The stash contains fields eventually extracted from previous responses that can be reused
     * as arguments for following requests (e.g. scroll_id)
     */
    public boolean isStashedValue(Object key) {
        if (key == null) {
            return false;
        }
        String stashKey = key.toString();
        return Strings.hasLength(stashKey) && stashKey.startsWith("$");
    }

    /**
     * Extracts a value from the current stash
     * The stash contains fields eventually extracted from previous responses that can be reused
     * as arguments for following requests (e.g. scroll_id)
     */
    public Object unstashValue(String value) throws IOException {
        if (value.startsWith("$body.")) {
            if (response == null) {
                return null;
            }
            return response.evaluate(value.substring("$body".length()), this);
        }
        Object stashedValue = stash.get(value.substring(1));
        if (stashedValue == null) {
            throw new IllegalArgumentException("stashed value not found for key [" + value + "]");
        }
        return stashedValue;
    }

    /**
     * Recursively unstashes map values if needed
     */
    public Map<String, Object> unstashMap(Map<String, Object> map) throws IOException {
        Map<String, Object> copy = new HashMap<>(map);
        unstashObject(copy);
        return copy;
    }

    @SuppressWarnings("unchecked")
    private void unstashObject(Object obj) throws IOException {
        if (obj instanceof List) {
            List list = (List) obj;
            for (int i = 0; i < list.size(); i++) {
                Object o = list.get(i);
                if (isStashedValue(o)) {
                    list.set(i, unstashValue(o.toString()));
                } else {
                    unstashObject(o);
                }
            }
        }
        if (obj instanceof Map) {
            Map<String, Object> map = (Map) obj;
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                if (isStashedValue(entry.getValue())) {
                    entry.setValue(unstashValue(entry.getValue().toString()));
                } else {
                    unstashObject(entry.getValue());
                }
            }
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("stash", stash);
        return builder;
    }
}

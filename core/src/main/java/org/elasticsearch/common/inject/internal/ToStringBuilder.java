/*
 * Copyright (C) 2006 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsearch.common.inject.internal;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Helps with {@code toString()} methods.
 *
 * @author crazybob@google.com (Bob Lee)
 */
public class ToStringBuilder {

    // Linked hash map ensures ordering.
    final Map<String, Object> map = new LinkedHashMap<>();

    final String name;

    public ToStringBuilder(String name) {
        this.name = name;
    }

    public ToStringBuilder(Class type) {
        this.name = type.getSimpleName();
    }

    public ToStringBuilder add(String name, Object value) {
        if (map.put(name, value) != null) {
            throw new RuntimeException("Duplicate names: " + name);
        }
        return this;
    }

    @Override
    public String toString() {
        return name + map.toString().replace('{', '[').replace('}', ']');
    }
}

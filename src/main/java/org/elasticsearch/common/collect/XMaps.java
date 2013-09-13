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

package org.elasticsearch.common.collect;

import java.util.Collections;
import java.util.Map;

/**
 * This class provides factory methods for Maps. The returned {@link Map}
 * instances are general purpose maps and non of the method guarantees a
 * concrete implementation unless the return type is a concrete type. The
 * implementations used might change over time, if you rely on a specific
 * Implementation you should use a concrete constructor.
 */
public final class XMaps {

    /**
     * Wraps the given map into a read only implementation.
     */
    public static <K, V> Map<K, V> makeReadOnly(Map<K, V> map) {
        return Collections.unmodifiableMap(map);
    }

}

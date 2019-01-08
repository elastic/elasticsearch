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
package org.elasticsearch.search.aggregations.bucket.geogrid2;

import java.util.Map;
import java.util.HashMap;

/**
 * Store for types - needed in multiple classes
 */
public class GeoGridTypes {

    public static GeoGridTypes DEFAULT = new GeoGridTypes();

    private Map<String, GeoGridType> types;

    private GeoGridTypes() {
        // TODO: we need to decide how types map is instantiated/stored
        // TODO: especially this is important to allow type plugins
        types = new HashMap<>();
        final GeoGridType type = new GeoHashType();
        types.put(type.getName(), type);
    }

    public GeoGridType get(String typeStr, String name) {
        final GeoGridType type = types.get(typeStr);
        if (type != null) {
            return type;
        }
        throw new IllegalArgumentException(
            "[type] is not valid. Allowed values: " +
                String.join(", ", types.keySet()) +
                ". Found [" + typeStr + "] in [" + name + "]");
    }
}

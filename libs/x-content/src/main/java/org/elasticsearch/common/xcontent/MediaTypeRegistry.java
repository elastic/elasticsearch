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

package org.elasticsearch.common.xcontent;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

public class MediaTypeRegistry {
    private static final MediaTypeRegistry INSTANCE = new MediaTypeRegistry();
    private Map<String, MediaType> formatToMediaType = new ConcurrentHashMap<>();
    private Map<String, MediaType> typeWithSubtypeToMediaType = new ConcurrentHashMap<>();
    private Map<String, Map<String, Pattern>> parametersMap= new ConcurrentHashMap<>();

    public static MediaTypeRegistry getInstance() {
        return INSTANCE;
    }

    public <T extends MediaType> void register(Map<String, T> formatToMediaType, Map<String, T> typeWithSubtypeToMediaType, Map<String, Map<String, Pattern>> parametersMap) {
        this.formatToMediaType.putAll(formatToMediaType);
        this.typeWithSubtypeToMediaType.putAll(typeWithSubtypeToMediaType);
        this.parametersMap.putAll(parametersMap);
    }


    public MediaType formatToMediaType(String format) {
        return formatToMediaType.get(format);
    }

    public MediaType typeWithSubtypeToMediaType(String typeWithSubtype) {
        return typeWithSubtypeToMediaType.get(typeWithSubtype);
    }

    public Map<String, Pattern> parametersFor(String typeWithSubtype) {
        return parametersMap.get(typeWithSubtype);
    }
}

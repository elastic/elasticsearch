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

import java.util.Collection;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

public class MediaTypeRegistry<T extends MediaType> {

    private Map<String, T> formatToMediaType = new ConcurrentHashMap<>();
    private Map<String, T> typeWithSubtypeToMediaType = new ConcurrentHashMap<>();
    private Map<String, Map<String, Pattern>> parametersMap = new ConcurrentHashMap<>();

    public  MediaTypeRegistry<T> register(Map<String, T> formatToMediaType,
                                       Map<String, T> typeWithSubtypeToMediaType,
                                       Map<String, Map<String, Pattern>> parametersMap) {
        this.formatToMediaType.putAll(formatToMediaType);
        this.typeWithSubtypeToMediaType.putAll(typeWithSubtypeToMediaType);
        this.parametersMap.putAll(parametersMap);
        return this;
    }

    public MediaTypeRegistry<T> register(String typeWithSubtype, T mediaType, String format, Map<String, String> parametersMap) {
        if (format != null) {
            this.formatToMediaType.put(format, mediaType);
        }
        this.typeWithSubtypeToMediaType.put(typeWithSubtype,mediaType);
        Map<String, Pattern> parametersForMediaType = new HashMap<>(parametersMap.size());
        for (Map.Entry<String, String> params : parametersMap.entrySet()) {
            String parameterName = params.getKey().toLowerCase(Locale.ROOT);
            String parameterRegex = params.getValue();
            Pattern pattern = Pattern.compile(parameterRegex, Pattern.CASE_INSENSITIVE);
            parametersForMediaType.put(parameterName, pattern);
        }
        this.parametersMap.put(typeWithSubtype,parametersForMediaType);
        return this;
    }

    public T formatToMediaType(String format) {
        if(format == null) {
            return null;
        }
        return formatToMediaType.get(format.toLowerCase(Locale.ROOT));
    }

    public T typeWithSubtypeToMediaType(String typeWithSubtype) {
        return typeWithSubtypeToMediaType.get(typeWithSubtype);
    }

    public Map<String, Pattern> parametersFor(String typeWithSubtype) {
        return parametersMap.get(typeWithSubtype);
    }

    public MediaTypeRegistry<T> register(String alternativeMediaType, T mediaType, Map<String, String> paramNameAndValueRegex) {
        typeWithSubtypeToMediaType.put(alternativeMediaType.toLowerCase(Locale.ROOT), mediaType);
        formatToMediaType.put(mediaType.format(), mediaType);

        Map<String, Pattern> parametersForMediaType = new HashMap<>(paramNameAndValueRegex.size());
        for (Map.Entry<String, String> params : paramNameAndValueRegex.entrySet()) {
            String parameterName = params.getKey().toLowerCase(Locale.ROOT);
            String parameterRegex = params.getValue();
            Pattern pattern = Pattern.compile(parameterRegex, Pattern.CASE_INSENSITIVE);
            parametersForMediaType.put(parameterName, pattern);
        }
        parametersMap.put(alternativeMediaType, parametersForMediaType);
        return this;
    }

    public MediaTypeRegistry<T> register(MediaTypeRegistry<? extends T> xContentTypeRegistry) {
        formatToMediaType.putAll(xContentTypeRegistry.formatToMediaType);
        typeWithSubtypeToMediaType.putAll(xContentTypeRegistry.typeWithSubtypeToMediaType);
        parametersMap.putAll(xContentTypeRegistry.parametersMap);
        return this;
    }
    public MediaTypeRegistry<T> register(Collection<MediaTypeRegistry<T>> mediaTypeRegistries ) {
        for (MediaTypeRegistry mediaTypeRegistry : mediaTypeRegistries) {
            register(mediaTypeRegistry);
        }
        return this;
    }

}

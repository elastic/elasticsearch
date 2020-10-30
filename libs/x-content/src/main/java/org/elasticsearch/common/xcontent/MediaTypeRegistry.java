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

import org.elasticsearch.common.collect.Tuple;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class MediaTypeRegistry<T extends MediaType> {

    private Map<String, T> formatToMediaType = new HashMap<>();
    private Map<String, T> typeWithSubtypeToMediaType = new HashMap<>();
    private Map<String, Map<String, Pattern>> parametersMap = new HashMap<>();

    public T formatToMediaType(String format) {
        if(format == null) {
            return null;
        }
        return formatToMediaType.get(format.toLowerCase(Locale.ROOT));
    }

    public T typeWithSubtypeToMediaType(String typeWithSubtype) {
        return typeWithSubtypeToMediaType.get(typeWithSubtype.toLowerCase(Locale.ROOT));
    }

    public Map<String, Pattern> parametersFor(String typeWithSubtype) {
        return parametersMap.get(typeWithSubtype);
    }

    public MediaTypeRegistry<T> register(T[] mediaTypes ) {
        for (T mediaType : mediaTypes) {
            Set<Tuple<String, Map<String, String>>> tuples = mediaType.mediaTypeMappings();
            for (Tuple<String, Map<String, String>> tuple : tuples) {
                formatToMediaType.put(mediaType.formatPathParameter(),mediaType);
                typeWithSubtypeToMediaType.put(tuple.v1(), mediaType);
                parametersMap.put(tuple.v1(), convertPatterns(tuple.v2()));
            }
        }
        return this;
    }

    private Map<String,Pattern> convertPatterns(Map<String,String> paramNameAndValueRegex){
        Map<String, Pattern> parametersForMediaType = new HashMap<>(paramNameAndValueRegex.size());
        for (Map.Entry<String, String> params : paramNameAndValueRegex.entrySet()) {
            String parameterName = params.getKey().toLowerCase(Locale.ROOT);
            String parameterRegex = params.getValue();
            Pattern pattern = Pattern.compile(parameterRegex, Pattern.CASE_INSENSITIVE);
            parametersForMediaType.put(parameterName, pattern);
        }
        return parametersForMediaType;
    }
}

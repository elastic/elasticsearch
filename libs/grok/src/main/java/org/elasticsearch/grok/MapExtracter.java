/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.grok;

import org.joni.Region;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;

/**
 * Extract {@link java.util.Map} results. This implementation of {@link org.elasticsearch.grok.GrokCaptureExtracter}
 * is mutable and should be discarded after collecting a single result.
 */
public class MapExtracter implements GrokCaptureExtracter {
    private final Map<String, Object> result;
    private final List<GrokCaptureExtracter> fieldExtracters;

    @SuppressWarnings("unchecked")
    MapExtracter(List<GrokCaptureConfig> captureConfig) {
        result = captureConfig.isEmpty() ? emptyMap() : new HashMap<>();
        fieldExtracters = new ArrayList<>(captureConfig.size());
        for (GrokCaptureConfig config : captureConfig) {
            fieldExtracters.add(config.objectExtracter(value -> {
                String key = config.name();
                if (config.hasMultipleBackReferences()) {
                    if (result.containsKey(key) == false) {
                        result.put(key, new ArrayList<>());
                    }

                    if (result.get(key)instanceof List<?> list) {
                        ((ArrayList<Object>) list).add(value);
                    }
                } else {
                    result.put(key, value);
                }
            }));
        }
    }

    @Override
    public void extract(byte[] utf8Bytes, int offset, Region region) {
        fieldExtracters.forEach(extracter -> extracter.extract(utf8Bytes, offset, region));
    }

    Map<String, Object> result() {
        // Maintain compatibility with Logstash's Grok:
        // Even though we could have a pattern with 2 or more groups with the same name (`%{NUMBER:name}%{WORD:name}%`) and only one
        // match, logstash still flatten the result returning just 1 element.
        return result.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> {
            if (e.getValue()instanceof List<?> list && list.size() == 1) {
                return list.get(0);
            } else {
                return e.getValue();
            }
        }));
    }
}

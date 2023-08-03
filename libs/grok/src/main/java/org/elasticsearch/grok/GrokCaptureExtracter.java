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

import static java.util.Collections.emptyMap;

/**
 * How to extract matches.
 */
public interface GrokCaptureExtracter {

    /**
     * Extract {@link Map} results. This implementation of {@link GrokCaptureExtracter}
     * is mutable and should be discarded after collecting a single result.
     */
    class MapExtracter implements GrokCaptureExtracter {
        private final Map<String, Object> result;
        private final List<GrokCaptureExtracter> fieldExtracters;

        @SuppressWarnings("unchecked")
        MapExtracter(List<GrokCaptureConfig> captureConfig) {
            result = captureConfig.isEmpty() ? emptyMap() : new HashMap<>();
            fieldExtracters = new ArrayList<>(captureConfig.size());
            for (GrokCaptureConfig config : captureConfig) {
                fieldExtracters.add(config.objectExtracter(value -> {
                    var key = config.name();

                    // Logstash's Grok processor flattens the list of values to a single value in case there's only 1 match,
                    // so we have to do the same to be compatible.
                    // e.g.:
                    // pattern = `%{SINGLEDIGIT:name}(%{SINGLEDIGIT:name})?`
                    // - GROK(pattern, "1") => { name: 1 }
                    // - GROK(pattern, "12") => { name: [1, 2] }
                    if (result.containsKey(key)) {
                        if (result.get(key) instanceof List<?> values) {
                            ((ArrayList<Object>) values).add(value);
                        } else {
                            var values = new ArrayList<>();
                            values.add(result.get(key));
                            values.add(value);
                            result.put(key, values);
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
            return result;
        }
    }

    void extract(byte[] utf8Bytes, int offset, Region region);
}

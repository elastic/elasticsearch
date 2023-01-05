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
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;

/**
 * Extract {@link java.util.Map} results. This implementation of {@link org.elasticsearch.grok.GrokCaptureExtracter}
 * is mutable and should be discarded after collecting a single result.
 */
public class MapExtracter implements GrokCaptureExtracter {
    private final Map<String, ValueHolder> result;
    private final List<GrokCaptureExtracter> fieldExtracters;

    MapExtracter(List<GrokCaptureConfig> captureConfig) {
        result = captureConfig.isEmpty() ? emptyMap() : new HashMap<>();
        fieldExtracters = new ArrayList<>(captureConfig.size());
        for (GrokCaptureConfig config : captureConfig) {
            var valueHolder = config.hasMultipleBackReferences() ? new MultiValueHolder() : new SingleValueHolder();
            fieldExtracters.add(config.objectExtracter(value -> {
                valueHolder.put(value);
                result.put(config.name(), valueHolder);
            }));
        }
    }

    @Override
    public void extract(byte[] utf8Bytes, int offset, Region region) {
        fieldExtracters.forEach(extracter -> extracter.extract(utf8Bytes, offset, region));
    }

    Map<String, Object> result() {
        return result.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().value()));
    }

    interface ValueHolder {
        Object value();

        void put(Object value);
    }

    private static class SingleValueHolder implements ValueHolder {

        private Object value;

        @Override
        public Object value() {
            return value;
        }

        @Override
        public void put(Object v) {
            value = v;
        }
    }

    private static class MultiValueHolder implements ValueHolder {
        private ArrayList<Object> value;

        @Override
        public Object value() {
            // Just to be compatible with Logstash's Grok:
            // Even though we could have a pattern with 2 or more groups with the same name (`%{NUMBER:name}%{WORD:name}%`) and only one
            // match, logstash still flatten the result returning just 1 element.
            return value.size() == 1 ? value.get(0) : value;
        }

        @Override
        public void put(Object v) {
            value = Objects.requireNonNullElseGet(value, ArrayList::new);
            value.add(v);
        }
    }
}

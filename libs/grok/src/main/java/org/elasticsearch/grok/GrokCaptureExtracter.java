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
public abstract class GrokCaptureExtracter {
    /**
     * Extract {@link Map} results. This implementation of {@link GrokCaptureExtracter}
     * is mutable and should be discarded after collecting a single result.
     */
    static class MapExtracter extends GrokCaptureExtracter {
        private final Map<String, Object> result;
        private final List<GrokCaptureExtracter> fieldExtracters;

        MapExtracter(List<GrokCaptureConfig> captureConfig) {
            result = captureConfig.isEmpty() ? emptyMap() : new HashMap<>();
            fieldExtracters = new ArrayList<>(captureConfig.size());
            for (GrokCaptureConfig config : captureConfig) {
                fieldExtracters.add(config.objectExtracter(v -> result.put(config.name(), v)));
            }
        }

        @Override
        void extract(byte[] utf8Bytes, int offset, Region region) {
            for (GrokCaptureExtracter extracter : fieldExtracters) {
                extracter.extract(utf8Bytes, offset, region);
            }
        }

        Map<String, Object> result() {
            return result;
        }
    }

    abstract void extract(byte[] utf8Bytes, int offset, Region region);
}

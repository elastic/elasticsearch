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

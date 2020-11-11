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

package org.elasticsearch.rollup;

import org.elasticsearch.Build;

public class RollupV2 {
    public static final Boolean ROLLUPV2_FEATURE_FLAG_REGISTERED;

    static {
        final String property = System.getProperty("es.rollupv2_feature_flag_registered");
        if (Build.CURRENT.isSnapshot() && property != null) {
            throw new IllegalArgumentException("es.rollupv2_feature_flag_registered is only supported in non-snapshot builds");
        }
        if ("true".equals(property)) {
            ROLLUPV2_FEATURE_FLAG_REGISTERED = true;
        } else if ("false".equals(property)) {
            ROLLUPV2_FEATURE_FLAG_REGISTERED = false;
        } else if (property == null) {
            ROLLUPV2_FEATURE_FLAG_REGISTERED = null;
        } else {
            throw new IllegalArgumentException(
                "expected es.rollupv2_feature_flag_registered to be unset or [true|false] but was [" + property + "]"
            );
        }
    }
}

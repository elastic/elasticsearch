/*
 * Copyright Elasticsearch B.V., and/or licensed to Elasticsearch B.V.
 * under one or more license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
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
 *
 * This file is based on a modification of https://github.com/open-telemetry/opentelemetry-java which is licensed under the Apache 2.0 License.
 */

package org.elasticsearch.exponentialhistogram;

import org.elasticsearch.core.Releasable;

/**
 * A histogram which participates in the {@link ExponentialHistogramCircuitBreaker} and therefore requires proper releasing.
 */
public interface ReleasableExponentialHistogram extends ExponentialHistogram, Releasable {

    /**
     * @return an empty singleton, which does not allocate any memory and therefore {@link #close()} is a no-op.
     */
    static ReleasableExponentialHistogram empty() {
        return EmptyExponentialHistogram.INSTANCE;
    }
}

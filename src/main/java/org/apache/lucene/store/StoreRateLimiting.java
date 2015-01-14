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
package org.apache.lucene.store;

import org.apache.lucene.store.RateLimiter.SimpleRateLimiter;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.unit.ByteSizeValue;

/**
 */
public class StoreRateLimiting {

    public static interface Provider {

        StoreRateLimiting rateLimiting();
    }

    public interface Listener {

        void onPause(long nanos);
    }

    public static enum Type {
        NONE,
        MERGE,
        ALL;

        public static Type fromString(String type) throws ElasticsearchIllegalArgumentException {
            if ("none".equalsIgnoreCase(type)) {
                return NONE;
            } else if ("merge".equalsIgnoreCase(type)) {
                return MERGE;
            } else if ("all".equalsIgnoreCase(type)) {
                return ALL;
            }
            throw new ElasticsearchIllegalArgumentException("rate limiting type [" + type + "] not valid, can be one of [all|merge|none]");
        }
    }

    private final SimpleRateLimiter rateLimiter = new SimpleRateLimiter(0);
    private volatile SimpleRateLimiter actualRateLimiter;

    private volatile Type type;

    public StoreRateLimiting() {

    }

    @Nullable
    public RateLimiter getRateLimiter() {
        return actualRateLimiter;
    }

    public void setMaxRate(ByteSizeValue rate) {
        if (rate.bytes() <= 0) {
            actualRateLimiter = null;
        } else if (actualRateLimiter == null) {
            actualRateLimiter = rateLimiter;
            actualRateLimiter.setMBPerSec(rate.mbFrac());
        } else {
            assert rateLimiter == actualRateLimiter;
            rateLimiter.setMBPerSec(rate.mbFrac());
        }
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public void setType(String type) throws ElasticsearchIllegalArgumentException {
        this.type = Type.fromString(type);
    }
}

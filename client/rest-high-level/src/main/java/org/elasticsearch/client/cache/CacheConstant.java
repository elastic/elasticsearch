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

package org.elasticsearch.client.cache;

import java.util.function.Function;

/**
 *  缓存相关常量
 */
class CacheConstant {

    static class CommonUsed{
        final static Number UNSET_NUMBER = -1;
        final static Integer DEFAULT_THREAD_POOL_CORE_SIZE_ASYNC_CACHE = Runtime.getRuntime().availableProcessors() / 4 + 1;
    }

    static class RedisRelevant{
        final static Integer DEFAULT_READ_TIMEOUT_MILLS = 1000;
        final static Integer DEFAULT_CONNECTION_TIMEOUT_MILLS = 2000;
        final static Function<Object, Object[]> DEFAULT_KEY_PARSER_TEMPLATE =  t -> new Object[]{t};
    }

}

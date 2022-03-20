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

/**
 *  cache接口
 * @param <K>   key
 * @param <V>   value
 */
public interface BasisCache<K, V> {

    /**
     * 向缓存中插入一条数据
     * @param key       key
     * @param value     value
     */
    void put(K key, V value);

    /**
     * 读取一条缓存中的数据
     * @param key       key
     * @return          缓存中的一条数据
     */
    V get(K key);

    /**
     * 使目标key对应的值失效
     * @param key       目标key
     */
    void inValidate(K key);

}

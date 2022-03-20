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

public interface AsyncCache<K, V> extends BasisCache<K, V> {

    /**
     * 判断是否可以执行异步的更新操作
     * 该方法主要是用来避免因缺乏执行异步操作必要条件进行导致调用失败情况
     * @return     是否可以执行异步的更新操作
     */
    boolean isAsync();

    /**
     *  异步向缓存中添加元素
     * @param key           key
     * @param value         value
     */
    void addAsync(K key, V value);

    /**
     * 异步的使缓存中对应的键值失效
     * @param key          目标键值
     */
    void inValidateAsync(K key);

}

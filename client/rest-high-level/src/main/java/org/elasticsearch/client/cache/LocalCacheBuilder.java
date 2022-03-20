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

import org.apache.commons.lang3.Validate;

import java.util.concurrent.TimeUnit;

public class LocalCacheBuilder<K, V> extends MultiCacheBuilder<LocalCacheBuilder<K, V>, K, V> {

    //写入键值后目标值失效的时间
    private Long expireAfterWriteMills = CacheConstant.CommonUsed.UNSET_NUMBER.longValue();

    // 获取键值后目标值失效时间
    private Long expireAfterAccessMills = CacheConstant.CommonUsed.UNSET_NUMBER.longValue();

    // 不使用maxmumWeighti,避免权重计算不统一或者不符合业务场景的问题发生
    private Long maximumSize = CacheConstant.CommonUsed.UNSET_NUMBER.longValue();

    LocalCacheBuilder(){}

   public <Key extends K, Value extends V> LocalCache<K, V> build(){
        return new LocalCache<>(this);
   }

   public LocalCacheBuilder<K, V> expireAfterWrite(Long duration, TimeUnit timeUnit){
       Validate.isTrue(expireAfterWriteMills == CacheConstant.CommonUsed.UNSET_NUMBER.longValue(), "time of expire after write has already been set");
       Validate.isTrue(duration > 0, "live duration after write must be larger than zero, but the target param is %d", duration);
       expireAfterWriteMills = timeUnit.toMillis(duration);
       return this;
   }

   public LocalCacheBuilder<K, V> expireAfterAccess(Long duration, TimeUnit timeUnit){
        Validate.isTrue(expireAfterAccessMills == CacheConstant.CommonUsed.UNSET_NUMBER.longValue(), "time of expire after write has already been set");
        Validate.isTrue(duration > 0, "live duration after accesds must be larger than zero, but the target param is %d", duration);
        expireAfterAccessMills = timeUnit.toMillis(duration);
        return this;
   }

   Long getExpireAfterWrite(){
        return expireAfterWriteMills;
   }

   Long getExpireAfterAccess(){
        return expireAfterAccessMills;
   }

   Long getMaximumSize(){
        return maximumSize;
   }

}

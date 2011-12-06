/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.index.store;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.jmx.MBean;
import org.elasticsearch.jmx.ManagedAttribute;

import java.io.IOException;

/**
 *
 */
@MBean(objectName = "shardType=store", description = "The storage of the index shard")
public class StoreManagement extends AbstractIndexShardComponent {

    private final Store store;

    @Inject
    public StoreManagement(Store store) {
        super(store.shardId(), store.indexSettings());
        this.store = store;
    }

    @ManagedAttribute(description = "Size in bytes")
    public long getSizeInBytes() {
        try {
            return store.estimateSize().bytes();
        } catch (IOException e) {
            return -1;
        }
    }

    @ManagedAttribute(description = "Size")
    public String getSize() {
        try {
            return store.estimateSize().toString();
        } catch (IOException e) {
            return "NA";
        }
    }
}

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

package org.elasticsearch.common.xcontent;

/**
 * An interface allowing to transfer an object to "XContent" using an
 * {@link XContentBuilder}. The difference between {@link ToXContentFragment}
 * and {@link ToXContentObject} is that the former outputs a fragment that
 * requires to start and end a new anonymous object externally, while the latter
 * guarantees that what gets printed out is fully valid syntax without any
 * external addition.
 */
public interface ToXContentFragment extends ToXContent {

    @Override
    default boolean isFragment() {
        return true;
    }
}

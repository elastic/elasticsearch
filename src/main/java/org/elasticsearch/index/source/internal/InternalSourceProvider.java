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

package org.elasticsearch.index.source.internal;

import org.elasticsearch.common.BytesHolder;
import org.elasticsearch.index.source.ExternalSourceProvider;

import java.io.IOException;

/**
 *
 */
public class InternalSourceProvider implements ExternalSourceProvider {
    @Override
    public BytesHolder dehydrateSource(String type, String id, byte[] source, int sourceOffset, int sourceLength) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public BytesHolder rehydrateSource(String type, String id, byte[] source, int sourceOffset, int sourceLength) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean enabled() {
        return false;
    }
}

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

package org.elasticsearch.index.query;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;

import java.util.function.LongSupplier;

public class CoordinatorRewriteContext extends QueryRewriteContext {
    private final Index index;
    private final long minTimestamp;
    private final long maxTimestamp;
    private final DateFieldMapper.DateFieldType timestampFieldTyped;

    public CoordinatorRewriteContext(NamedXContentRegistry xContentRegistry,
                                     NamedWriteableRegistry writeableRegistry,
                                     Client client,
                                     LongSupplier nowInMillis,
                                     Index index,
                                     long minTimestamp,
                                     long maxTimestamp,
                                     DateFieldMapper.DateFieldType timestampFieldTyped) {
        super(xContentRegistry, writeableRegistry, client, nowInMillis);
        this.index = index;
        this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
        this.timestampFieldTyped = timestampFieldTyped;
    }

    long getMinTimestamp() {
        return minTimestamp;
    }

    long getMaxTimestamp() {
        return maxTimestamp;
    }

    @Nullable
    public MappedFieldType getFieldType(String fieldName) {
        if (fieldName.equals(timestampFieldTyped.name()) == false) {
            return null;
        }

        return timestampFieldTyped;
    }

    @Override
    public CoordinatorRewriteContext convertToCoordinatorRewriteContext() {
        return this;
    }
}

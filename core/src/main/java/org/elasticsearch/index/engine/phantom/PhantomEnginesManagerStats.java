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

package org.elasticsearch.index.engine.phantom;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Statistics about usage of {@link PhantomEnginesManager}.
 */
public class PhantomEnginesManagerStats implements Streamable, ToXContent {

    private int loadedEnginesCount;
    private long maxHeapSize;
    private long usedHeapSize;
    private long addTime;
    private long addCount;
    private long removeTime;
    private long removeCount;

    public PhantomEnginesManagerStats() {
    }

    PhantomEnginesManagerStats(PhantomEnginesManager manager) {
        usedHeapSize = manager.usedHeapSizeInBytes();
        maxHeapSize = manager.maxHeapSizeInBytes();
        loadedEnginesCount = manager.loadedEnginesCount();
        addTime = manager.addTime.get();
        addCount = manager.addCount.get();
        removeTime = manager.removeTime.get();
        removeCount = manager.removeCount.get();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject("phantom_engines")
            .field("loaded_count", loadedEnginesCount)
            .field("max_heap_size_in_bytes", maxHeapSize)
            .field("max_heap_size", ByteSizeValue.of(maxHeapSize))
            .field("used_heap_size_in_bytes", usedHeapSize)
            .field("used_heap_size", ByteSizeValue.of(usedHeapSize))
            .field("used_heap_size_percent", usedHeapSize * 100 / maxHeapSize)
            .field("add_engine_time_in_millis", addTime)
            .field("add_engine_time", TimeValue.timeValueMillis(addTime))
            .field("add_engine_count", addCount)
            .field("remove_engine_time_in_millis", removeTime)
            .field("remove_engine_time", TimeValue.timeValueMillis(removeTime))
            .field("remove_engine_count", removeCount)
            .endObject();
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        loadedEnginesCount = in.readInt();
        maxHeapSize = in.readLong();
        usedHeapSize = in.readLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(loadedEnginesCount);
        out.writeLong(maxHeapSize);
        out.writeLong(usedHeapSize);
    }
}

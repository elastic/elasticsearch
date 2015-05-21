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

package org.elasticsearch.cluster;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * IndexClassification is used to classify indices by size into different
 * {@code ClusterInfo.IndexSize} categories.
 */
public class IndexClassification implements Streamable, ToXContent {

    private volatile ImmutableMap<String, ClusterInfo.IndexSize> indexClassifications;

    public IndexClassification(Map<String, ClusterInfo.IndexSize> indexClassifications) {
        this.indexClassifications = ImmutableMap.copyOf(indexClassifications);
    }

    // Used for serialization in ClusterInfo
    IndexClassification() {
    }

    public static IndexClassification classifyIndices(final Map<String, Long> indexSizes,
            ESLogger logger) {
        long maxSize = 0;
        long minSize = Long.MAX_VALUE;
        for (Map.Entry<String, Long> idx : indexSizes.entrySet()) {
            maxSize = Math.max(maxSize, idx.getValue());
            minSize = Math.min(minSize, idx.getValue());
        }
        Map<String, ClusterInfo.IndexSize> newIndexClassifications = new HashMap<>(indexSizes.size());

        long TINY = minSize;
        long HUGE = maxSize;
        long MEDIUM = HUGE - ((HUGE - TINY) / 2);
        long SMALL = MEDIUM - ((MEDIUM - TINY) / 2);
        long LARGE = HUGE - ((HUGE - MEDIUM) / 2);

        if (logger.isDebugEnabled()) {
            logger.debug("SMALLEST: [{}], SMALL: [{}], MEDIUM: [{}], LARGE: [{}], LARGEST: [{}]",
                    new ByteSizeValue(TINY),
                    new ByteSizeValue(SMALL),
                    new ByteSizeValue(MEDIUM),
                    new ByteSizeValue(LARGE),
                    new ByteSizeValue(HUGE));
        }

        for (Map.Entry<String, Long> idx : indexSizes.entrySet()) {
            if (TINY == HUGE) {
                // This means they're all the same size, or there is only one
                // index, so short-circuit to MEDIUM
                logger.debug("index [{}] is [{}]", idx.getKey(), ClusterInfo.IndexSize.MEDIUM);
                newIndexClassifications.put(idx.getKey(), ClusterInfo.IndexSize.MEDIUM);
                continue;
            }
            long size = idx.getValue();
            logger.debug("index size for [{}] is [{}]", idx.getKey(), new ByteSizeValue(size));
            ClusterInfo.IndexSize sizeEnum;
            // split the search set in half
            if (size <= MEDIUM) {
                // less than or equal to medium
                if (size > SMALL) {
                    // between SMALL and MEDIUM
                    if ((size - SMALL) < (MEDIUM - size)) {
                        sizeEnum = ClusterInfo.IndexSize.SMALL;
                    } else {
                        sizeEnum = ClusterInfo.IndexSize.MEDIUM;
                    }
                } else {
                    // between SMALLEST and SMALL
                    if ((size - TINY) < (SMALL - size)) {
                        sizeEnum = ClusterInfo.IndexSize.SMALLEST;
                    } else {
                        sizeEnum = ClusterInfo.IndexSize.SMALL;
                    }
                }
            } else {
                // greater than MEDIUM
                if (size > LARGE) {
                    // between LARGE and LARGEST
                    if ((size - LARGE) < (HUGE - size)) {
                        sizeEnum = ClusterInfo.IndexSize.LARGE;
                    } else {
                        sizeEnum = ClusterInfo.IndexSize.LARGEST;
                    }
                } else {
                    // between MEDIUM and LARGE
                    if ((size - MEDIUM) < (LARGE - size)) {
                        sizeEnum = ClusterInfo.IndexSize.MEDIUM;
                    } else {
                        sizeEnum = ClusterInfo.IndexSize.LARGE;
                    }
                }
            }
            logger.debug("index [{}] is [{}]", idx.getKey(), sizeEnum);
            newIndexClassifications.put(idx.getKey(), sizeEnum);
        }

        return new IndexClassification(newIndexClassifications);
    }

    public Map<String, ClusterInfo.IndexSize> getIndexClassifications() {
        return this.indexClassifications;
    }
    
    @Override
    public int hashCode() {
        return this.indexClassifications.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (o instanceof IndexClassification) {
            IndexClassification other = (IndexClassification) o;
            // Just compare the maps
            return this.indexClassifications.equals(other.getIndexClassifications());
        } else {
            return false;
        }
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, ClusterInfo.IndexSize> entry : indexClassifications.entrySet()) {
            sb.append(entry.getKey()).append(": ");
            sb.append(entry.getValue());
            sb.append("\n");
        }
        return sb.toString();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("classifications");
        for (Map.Entry<String, ClusterInfo.IndexSize> entry : indexClassifications.entrySet()) {
            builder.field(entry.getKey());
            builder.value(entry.getValue());
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        long mapSize = in.readVLong();
        Map<String, ClusterInfo.IndexSize> sizes = Maps.newHashMap();
        for (int i = 0; i < mapSize; i++) {
            String index = in.readString();
            String sizeStr = in.readString();
            ClusterInfo.IndexSize size = ClusterInfo.IndexSize.valueOf(sizeStr);
            sizes.put(index, size);
        }
        this.indexClassifications = ImmutableMap.copyOf(sizes);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(indexClassifications.size());
        for (Map.Entry<String, ClusterInfo.IndexSize> entry : indexClassifications.entrySet()) {
            String index = entry.getKey();
            ClusterInfo.IndexSize size = entry.getValue();
            out.writeString(index);
            out.writeString(size.toString());
        }
    }
}

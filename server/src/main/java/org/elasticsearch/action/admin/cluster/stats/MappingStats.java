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

package org.elasticsearch.action.admin.cluster.stats;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Usage statistics about mappings usage.
 */
public final class MappingStats implements ToXContentFragment, Writeable {

    /**
     * Create {@link MappingStats} from the given cluster state.
     */
    public static MappingStats of(ClusterState state) {
        Map<String, IndexFeatureStats> fieldTypes = new HashMap<>();
        for (IndexMetadata indexMetadata : state.metadata()) {
            Set<String> indexFieldTypes = new HashSet<>();
            MappingMetadata mappingMetadata = indexMetadata.mapping();
            if (mappingMetadata != null) {
                MappingVisitor.visitMapping(mappingMetadata.getSourceAsMap(), fieldMapping -> {
                    String type = null;
                    Object typeO = fieldMapping.get("type");
                    if (typeO != null) {
                        type = typeO.toString();
                    } else if (fieldMapping.containsKey("properties")) {
                        type = "object";
                    }

                    if (type != null) {
                        IndexFeatureStats stats = fieldTypes.computeIfAbsent(type, IndexFeatureStats::new);
                        stats.count++;
                        if (indexFieldTypes.add(type)) {
                            stats.indexCount++;
                        }
                    }
                });
            }
        }
        return new MappingStats(fieldTypes.values());
    }

    private final Set<IndexFeatureStats> fieldTypeStats;

    MappingStats(Collection<IndexFeatureStats> fieldTypeStats) {
        List<IndexFeatureStats> stats = new ArrayList<>(fieldTypeStats);
        stats.sort(Comparator.comparing(IndexFeatureStats::getName));
        this.fieldTypeStats = Collections.unmodifiableSet(new LinkedHashSet<IndexFeatureStats>(stats));
    }

    MappingStats(StreamInput in) throws IOException {
        fieldTypeStats = Collections.unmodifiableSet(new LinkedHashSet<>(in.readList(IndexFeatureStats::new)));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(fieldTypeStats);
    }

    /**
     * Return stats about field types.
     */
    public Set<IndexFeatureStats> getFieldTypeStats() {
        return fieldTypeStats;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("mappings");
        builder.startArray("field_types");
        for (IndexFeatureStats st : fieldTypeStats) {
            st.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof MappingStats == false) {
            return false;
        }
        MappingStats that = (MappingStats) o;
        return fieldTypeStats.equals(that.fieldTypeStats);
    }

    @Override
    public int hashCode() {
        return fieldTypeStats.hashCode();
    }
}

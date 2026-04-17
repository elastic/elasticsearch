/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

public class RequestStats implements Writeable, ToXContentFragment {

    public enum Category {
        TOTAL("total"),
        READ("read"),
        WRITE("write");

        private final String fieldName;

        Category(String fieldName) {
            this.fieldName = fieldName;
        }

        public String fieldName() {
            return fieldName;
        }
    }

    private final EnumMap<Category, TypeStats> statsByCategory;

    public RequestStats(EnumMap<Category, TypeStats> statsByCategory) {
        this.statsByCategory = statsByCategory;
    }

    public RequestStats(StreamInput in) throws IOException {
        int size = in.readVInt();
        this.statsByCategory = new EnumMap<>(Category.class);
        for (int i = 0; i < size; i++) {
            Category category = in.readEnum(Category.class);
            statsByCategory.put(category, new TypeStats(in));
        }
    }

    public static RequestStats empty() {
        EnumMap<Category, TypeStats> map = new EnumMap<>(Category.class);
        for (Category category : Category.values()) {
            map.put(category, TypeStats.empty());
        }
        return new RequestStats(map);
    }

    public TypeStats get(Category category) {
        return statsByCategory.get(category);
    }

    public void add(RequestStats other) {
        for (Category category : Category.values()) {
            TypeStats current = statsByCategory.get(category);
            TypeStats addition = other.statsByCategory.get(category);
            if (current == null) {
                statsByCategory.put(category, new TypeStats(addition));
            } else if (addition != null) {
                current.add(addition);
            }
        }
    }

    public static RequestStats sum(List<RequestStats> statsList) {
        if (statsList == null || statsList.isEmpty()) {
            return empty();
        }
        if (statsList.size() == 1) {
            return statsList.get(0);
        }

        EnumMap<Category, TypeStats> summed = new EnumMap<>(Category.class);
        for (Category category : Category.values()) {
            TypeStats acc = TypeStats.empty();
            for (RequestStats stats : statsList) {
                TypeStats typeStats = stats.get(category);
                if (typeStats != null) {
                    acc.add(typeStats);
                }
            }
            summed.put(category, acc);
        }
        return new RequestStats(summed);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(statsByCategory.size());
        for (Map.Entry<Category, TypeStats> entry : statsByCategory.entrySet()) {
            out.writeEnum(entry.getKey());
            entry.getValue().writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        for (Category category : Category.values()) {
            TypeStats stats = statsByCategory.get(category);
            if (stats == null) {
                continue;
            }
            builder.startObject(category.fieldName());
            stats.toXContent(builder, params);
            builder.endObject();
        }
        return builder;
    }

    public static final class TypeStats implements Writeable, ToXContentFragment {
        private long success;
        private long failed;
        private long total;

        public TypeStats(long success, long failed, long total) {
            this.success = success;
            this.failed = failed;
            this.total = total;
        }

        public TypeStats(TypeStats other) {
            this.success = other.success;
            this.failed = other.failed;
            this.total = other.total;
        }

        public TypeStats(StreamInput in) throws IOException {
            this.success = in.readVLong();
            this.failed = in.readVLong();
            this.total = in.readVLong();
        }

        static TypeStats empty() {
            return new TypeStats(0, 0, 0);
        }

        public long getSuccess() {
            return success;
        }

        public long getFailed() {
            return failed;
        }

        public long getTotal() {
            return total;
        }

        public void add(TypeStats other) {
            if (other == null) {
                return;
            }
            this.success += other.success;
            this.failed += other.failed;
            this.total += other.total;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(success);
            out.writeVLong(failed);
            out.writeVLong(total);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field("success", success);
            builder.field("failed", failed);
            builder.field("total", total);

            return builder;
        }
    }
}


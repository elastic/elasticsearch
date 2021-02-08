/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.snapshots.features;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Response to a feature state reset request. */
public class ResetFeatureStateResponse extends ActionResponse implements ToXContentObject {

    List<Item> itemList;

    public ResetFeatureStateResponse(Map<String, String> statusMap) {
        // TODO[wrb] - we should return a list or map of feature states and their responses
        itemList = new ArrayList<>();
        for (Map.Entry<String, String> entry : statusMap.entrySet()) {
            itemList.add(new Item(entry.getKey(), entry.getValue()));
        }
    }

    public ResetFeatureStateResponse(List<Item> statuslist) {
        // TODO[wrb] - we should return a list or map of feature states and their responses
        itemList = new ArrayList<>();
        itemList.addAll(statuslist);
    }

    public ResetFeatureStateResponse(StreamInput in) throws IOException {
        super(in);
        this.itemList = in.readList(Item::new);
    }

    public List<Item> getItemList() {
        return this.itemList;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.startArray("features");
            for (Item item: this.itemList) {
                builder.value(item);
            }
            builder.endArray();
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(this.itemList);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ResetFeatureStateResponse that = (ResetFeatureStateResponse) o;
        return Objects.equals(itemList, that.itemList);
    }

    @Override
    public int hashCode() {
        return Objects.hash(itemList);
    }

    @Override
    public String toString() {
        return "ResetFeatureStateResponse{" +
            "itemList=" + itemList +
            '}';
    }

    public static class Item implements Writeable, ToXContentObject {
        private String featureName;
        private String status;

        public Item(String featureName, String status) {
            this.featureName = featureName;
            this.status = status;
        }

        Item(StreamInput in) throws IOException {
            this.featureName = in.readString();
            this.status = in.readString();
        }

        public String getFeatureName() {
            return this.featureName;
        }

        public String getStatus() {
            return this.status;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("feature_name", this.featureName);
            builder.field("status", this.status);
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(this.featureName);
            out.writeString(this.status);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Item item = (Item) o;
            return Objects.equals(featureName, item.featureName) && Objects.equals(status, item.status);
        }

        @Override
        public int hashCode() {
            return Objects.hash(featureName, status);
        }

        @Override
        public String toString() {
            return "Item{" +
                "featureName='" + featureName + '\'' +
                ", status='" + status + '\'' +
                '}';
        }
    }
}

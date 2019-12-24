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
package org.elasticsearch.client.ml;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.client.ml.job.config.MlFilter;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collection;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Updates an existing {@link MlFilter} configuration
 */
public class UpdateFilterRequest implements Validatable, ToXContentObject {

    public static final ParseField ADD_ITEMS = new ParseField("add_items");
    public static final ParseField REMOVE_ITEMS = new ParseField("remove_items");

    public static final ConstructingObjectParser<UpdateFilterRequest, Void> PARSER =
        new ConstructingObjectParser<>("update_filter_request", (a) -> new UpdateFilterRequest((String)a[0]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), MlFilter.ID);
        PARSER.declareStringOrNull(UpdateFilterRequest::setDescription, MlFilter.DESCRIPTION);
        PARSER.declareStringArray(UpdateFilterRequest::setAddItems, ADD_ITEMS);
        PARSER.declareStringArray(UpdateFilterRequest::setRemoveItems, REMOVE_ITEMS);
    }

    private String filterId;
    private String description;
    private SortedSet<String> addItems;
    private SortedSet<String> removeItems;

    /**
     * Construct a new request referencing a non-null, existing filter_id
     * @param filterId Id referencing the filter to update
     */
    public UpdateFilterRequest(String filterId) {
        this.filterId = Objects.requireNonNull(filterId, "[" + MlFilter.ID.getPreferredName() + "] must not be null");
    }

    public String getFilterId() {
        return filterId;
    }

    public String getDescription() {
        return description;
    }

    /**
     * The new description of the filter
     * @param description the updated filter description
     */
    public void setDescription(String description) {
        this.description = description;
    }

    public SortedSet<String> getAddItems() {
        return addItems;
    }

    /**
     * The collection of items to add to the filter
     * @param addItems non-null items to add to the filter, defaults to empty array
     */
    public void setAddItems(Collection<String> addItems) {
        this.addItems = new TreeSet<>(Objects.requireNonNull(addItems,
            "[" + ADD_ITEMS.getPreferredName()+"] must not be null"));
    }

    public SortedSet<String> getRemoveItems() {
        return removeItems;
    }

    /**
     * The collection of items to remove from the filter
     * @param removeItems non-null items to remove from the filter, defaults to empty array
     */
    public void setRemoveItems(Collection<String> removeItems) {
        this.removeItems = new TreeSet<>(Objects.requireNonNull(removeItems,
            "[" + REMOVE_ITEMS.getPreferredName()+"] must not be null"));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(MlFilter.ID.getPreferredName(), filterId);
        if (description != null) {
            builder.field(MlFilter.DESCRIPTION.getPreferredName(), description);
        }
        if (addItems != null) {
            builder.field(ADD_ITEMS.getPreferredName(), addItems);
        }
        if (removeItems != null) {
            builder.field(REMOVE_ITEMS.getPreferredName(), removeItems);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(filterId, description, addItems, removeItems);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        UpdateFilterRequest other = (UpdateFilterRequest) obj;
        return Objects.equals(filterId, other.filterId)
            && Objects.equals(description, other.description)
            && Objects.equals(addItems, other.addItems)
            && Objects.equals(removeItems, other.removeItems);
    }

    @Override
    public final String toString() {
        return Strings.toString(this);
    }

}

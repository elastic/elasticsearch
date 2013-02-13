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

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.ElasticSearchGenerationException;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.FilterBuilder;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class AliasAction implements Streamable {

    public static enum Type {
        ADD((byte) 0),
        REMOVE((byte) 1);

        private final byte value;

        Type(byte value) {
            this.value = value;
        }

        public byte value() {
            return value;
        }

        public static Type fromValue(byte value) {
            if (value == 0) {
                return ADD;
            } else if (value == 1) {
                return REMOVE;
            } else {
                throw new ElasticSearchIllegalArgumentException("No type for action [" + value + "]");
            }
        }
    }

    private Type actionType;

    private String index;

    private String alias;

    @Nullable
    private String filter;

    @Nullable
    private String indexRouting;

    @Nullable
    private String searchRouting;

    private AliasAction() {

    }

    public AliasAction(Type actionType, String index, String alias) {
        this.actionType = actionType;
        this.index = index;
        this.alias = alias;
    }

    public AliasAction(Type actionType, String index, String alias, String filter) {
        this.actionType = actionType;
        this.index = index;
        this.alias = alias;
        this.filter = filter;
    }

    public Type actionType() {
        return actionType;
    }

    public String index() {
        return index;
    }

    public String alias() {
        return alias;
    }

    public String filter() {
        return filter;
    }

    public AliasAction filter(String filter) {
        this.filter = filter;
        return this;
    }

    public AliasAction filter(Map<String, Object> filter) {
        if (filter == null || filter.isEmpty()) {
            this.filter = null;
            return this;
        }
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            builder.map(filter);
            this.filter = builder.string();
            return this;
        } catch (IOException e) {
            throw new ElasticSearchGenerationException("Failed to generate [" + filter + "]", e);
        }
    }

    public AliasAction filter(FilterBuilder filterBuilder) {
        if (filterBuilder == null) {
            this.filter = null;
            return this;
        }
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            filterBuilder.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.close();
            this.filter = builder.string();
            return this;
        } catch (IOException e) {
            throw new ElasticSearchGenerationException("Failed to build json for alias request", e);
        }
    }

    public AliasAction routing(String routing) {
        this.indexRouting = routing;
        this.searchRouting = routing;
        return this;
    }

    public String indexRouting() {
        return indexRouting;
    }

    public AliasAction indexRouting(String indexRouting) {
        this.indexRouting = indexRouting;
        return this;
    }

    public String searchRouting() {
        return searchRouting;
    }

    public AliasAction searchRouting(String searchRouting) {
        this.searchRouting = searchRouting;
        return this;
    }

    public static AliasAction readAliasAction(StreamInput in) throws IOException {
        AliasAction aliasAction = new AliasAction();
        aliasAction.readFrom(in);
        return aliasAction;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        actionType = Type.fromValue(in.readByte());
        index = in.readString();
        alias = in.readString();
        if (in.readBoolean()) {
            filter = in.readString();
        }
        if (in.readBoolean()) {
            indexRouting = in.readString();
        }
        if (in.readBoolean()) {
            searchRouting = in.readString();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeByte(actionType.value());
        out.writeString(index);
        out.writeString(alias);
        if (filter == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeString(filter);
        }
        if (indexRouting == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeString(indexRouting);
        }
        if (searchRouting == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeString(searchRouting);
        }
    }

    public static AliasAction newAddAliasAction(String index, String alias) {
        return new AliasAction(Type.ADD, index, alias);
    }

    public static AliasAction newRemoveAliasAction(String index, String alias) {
        return new AliasAction(Type.REMOVE, index, alias);
    }

}

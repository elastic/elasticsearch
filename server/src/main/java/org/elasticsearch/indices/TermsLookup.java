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

package org.elasticsearch.indices;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.TermsQueryBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Encapsulates the parameters needed to fetch terms.
 */
public class TermsLookup implements Writeable, ToXContentFragment {
    private final String index;
    private @Nullable String type;
    private final String id;
    private final String path;
    private String routing;


    public TermsLookup(String index, String id, String path) {
        this(index, null, id, path);
    }

    /**
     * @deprecated Types are in the process of being removed, use {@link TermsLookup(String, String, String)} instead.
     */
    @Deprecated
    public TermsLookup(String index, String type, String id, String path) {
        if (id == null) {
            throw new IllegalArgumentException("[" + TermsQueryBuilder.NAME + "] query lookup element requires specifying the id.");
        }
        if (path == null) {
            throw new IllegalArgumentException("[" + TermsQueryBuilder.NAME + "] query lookup element requires specifying the path.");
        }
        if (index == null) {
            throw new IllegalArgumentException("[" + TermsQueryBuilder.NAME + "] query lookup element requires specifying the index.");
        }
        this.index = index;
        this.type = type;
        this.id = id;
        this.path = path;
    }

    /**
     * Read from a stream.
     */
    public TermsLookup(StreamInput in) throws IOException {
        type = in.readOptionalString();
        id = in.readString();
        path = in.readString();
        index = in.readString();
        routing = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(type);
        out.writeString(id);
        out.writeString(path);
        out.writeString(index);
        out.writeOptionalString(routing);
    }

    public String index() {
        return index;
    }

    /**
     * @deprecated Types are in the process of being removed.
     */
    @Deprecated
    public String type() {
        return type;
    }

    public String id() {
        return id;
    }

    public String path() {
        return path;
    }

    public String routing() {
        return routing;
    }

    public TermsLookup routing(String routing) {
        this.routing = routing;
        return this;
    }

    public static TermsLookup parseTermsLookup(XContentParser parser) throws IOException {
        String index = null;
        String type = null;
        String id = null;
        String path = null;
        String routing = null;
        XContentParser.Token token;
        String currentFieldName = "";
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                switch (currentFieldName) {
                case "index":
                    index = parser.text();
                    break;
                case "type":
                    type = parser.text();
                    break;
                case "id":
                    id = parser.text();
                    break;
                case "routing":
                    routing = parser.textOrNull();
                    break;
                case "path":
                    path = parser.text();
                    break;
                default:
                    throw new ParsingException(parser.getTokenLocation(), "[" + TermsQueryBuilder.NAME +
                        "] query does not support [" + currentFieldName + "] within lookup element");
                }
            } else {
                throw new ParsingException(parser.getTokenLocation(), "[" + TermsQueryBuilder.NAME + "] unknown token ["
                    + token + "] after [" + currentFieldName + "]");
            }
        }
        if (type == null) {
            return new TermsLookup(index, id, path).routing(routing);
        } else {
            return new TermsLookup(index, type, id, path).routing(routing);
        }
    }

    @Override
    public String toString() {
        if (type == null) {
            return index + "/" + id + "/" + path;
        } else {
            return index + "/" + type + "/" + id + "/" + path;
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("index", index);
        if (type != null) {
            builder.field("type", type);
        }
        builder.field("id", id);
        builder.field("path", path);
        if (routing != null) {
            builder.field("routing", routing);
        }
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, type, id, path, routing);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        TermsLookup other = (TermsLookup) obj;
        return Objects.equals(index, other.index) &&
                Objects.equals(type, other.type) &&
                Objects.equals(id, other.id) &&
                Objects.equals(path, other.path) &&
                Objects.equals(routing, other.routing);
    }
}

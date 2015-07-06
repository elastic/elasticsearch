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

package org.elasticsearch.indices.cache.query.terms;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryValidationException;

import java.io.IOException;
import java.util.Objects;

/**
 * Encapsulates the parameters needed to fetch terms.
 */
public class TermsLookup implements Writeable<TermsLookup>, ToXContent {
    static final TermsLookup PROTOTYPE = new TermsLookup();

    private String index;
    private String type;
    private String id;
    private String path;
    private String routing;

    public TermsLookup() {
    }

    public TermsLookup(String index, String type, String id, String path) {
        this.index = index;
        this.type = type;
        this.id = id;
        this.path = path;
    }

    public String index() {
        return index;
    }

    public TermsLookup index(String index) {
        this.index = index;
        return this;
    }

    public String type() {
        return type;
    }

    public TermsLookup type(String type) {
        this.type = type;
        return this;
    }

    public String id() {
        return id;
    }

    public TermsLookup id(String id) {
        this.id = id;
        return this;
    }

    public String path() {
        return path;
    }

    public TermsLookup path(String path) {
        this.path = path;
        return this;
    }

    public String routing() {
        return routing;
    }

    public TermsLookup routing(String routing) {
        this.routing = routing;
        return this;
    }

    @Override
    public String toString() {
        return index + "/" + type + "/" + id + "/" + path;
    }

    @Override
    public TermsLookup readFrom(StreamInput in) throws IOException {
        TermsLookup termsLookup = new TermsLookup();
        termsLookup.index = in.readOptionalString();
        termsLookup.type = in.readString();
        termsLookup.id = in.readString();
        termsLookup.path = in.readString();
        termsLookup.routing = in.readOptionalString();
        return termsLookup;
    }

    public static TermsLookup readTermsLookupFrom(StreamInput in) throws IOException {
        return PROTOTYPE.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(index);
        out.writeString(type);
        out.writeString(id);
        out.writeString(path);
        out.writeOptionalString(routing);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (index != null) {
            builder.field("index", index);
        }
        builder.field("type", type);
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

    public QueryValidationException validate() {
        QueryValidationException validationException = null;
        if (id == null) {
            validationException = addValidationError("[terms] query lookup element requires specifying the id.", validationException);
        }
        if (type == null) {
            validationException = addValidationError("[terms] query lookup element requires specifying the type.", validationException);
        }
        if (path == null) {
            validationException = addValidationError("[terms] query lookup element requires specifying the path.", validationException);
        }
        return validationException;
    }

    private static QueryValidationException addValidationError(String validationError, QueryValidationException validationException) {
        return QueryValidationException.addValidationError("terms_lookup", validationError, validationException);
    }
}

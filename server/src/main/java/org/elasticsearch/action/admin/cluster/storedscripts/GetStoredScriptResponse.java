/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.storedscripts;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.StoredScriptSource;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class GetStoredScriptResponse extends ActionResponse implements ToXContentObject {

    public static final ParseField _ID_PARSE_FIELD = new ParseField("_id");
    public static final ParseField FOUND_PARSE_FIELD = new ParseField("found");
    public static final ParseField SCRIPT = new ParseField("script");

    private final String id;
    private final StoredScriptSource source;

    public GetStoredScriptResponse(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            source = new StoredScriptSource(in);
        } else {
            source = null;
        }
        id = in.readString();
    }

    GetStoredScriptResponse(String id, StoredScriptSource source) {
        this.id = id;
        this.source = source;
    }

    public String getId() {
        return id;
    }

    /**
     * @return if a stored script and if not found <code>null</code>
     */
    public StoredScriptSource getSource() {
        return source;
    }

    public RestStatus status() {
        return source != null ? RestStatus.OK : RestStatus.NOT_FOUND;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.field(_ID_PARSE_FIELD.getPreferredName(), id);
        builder.field(FOUND_PARSE_FIELD.getPreferredName(), source != null);
        if (source != null) {
            builder.field(StoredScriptSource.SCRIPT_PARSE_FIELD.getPreferredName());
            source.toXContent(builder, params);
        }

        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (source == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            source.writeTo(out);
        }
        out.writeString(id);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GetStoredScriptResponse that = (GetStoredScriptResponse) o;
        return Objects.equals(id, that.id) && Objects.equals(source, that.source);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, source);
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.storedscripts;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.ScriptLanguagesInfo;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

public class GetScriptLanguageResponse extends ActionResponse implements StatusToXContentObject, Writeable {
    public final ScriptLanguagesInfo info;

    GetScriptLanguageResponse(ScriptLanguagesInfo info) {
        this.info = info;
    }

    GetScriptLanguageResponse(StreamInput in) throws IOException {
        super(in);
        info = new ScriptLanguagesInfo(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        info.writeTo(out);
    }

    @Override
    public RestStatus status() {
        return RestStatus.OK;
    }

    public static GetScriptLanguageResponse fromXContent(XContentParser parser) throws IOException {
        return new GetScriptLanguageResponse(ScriptLanguagesInfo.fromXContent(parser));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GetScriptLanguageResponse that = (GetScriptLanguageResponse) o;
        return info.equals(that.info);
    }

    @Override
    public int hashCode() {
        return Objects.hash(info);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return info.toXContent(builder, params);
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rank.script;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

public class ScriptRankHitData implements RankHitData {
    private final Map<String, Object> fieldData;

    public ScriptRankHitData(Map<String, Object> fieldData) {
        this.fieldData = fieldData;
    }

    public ScriptRankHitData(StreamInput in) throws IOException {
        fieldData = in.readGenericMap();
    }

    @Override
    public String getWriteableName() {
        return ScriptRankRetrieverBuilder.NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.SCRIPT_RANK_ADDED;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeGenericMap(fieldData);
    }

    public Map<String, Object> getFieldData() {
        return fieldData;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("field_data", fieldData);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean isFragment() {
        return false;
    }
}

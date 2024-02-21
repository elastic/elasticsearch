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
    private final float[] queryScores;

    public ScriptRankHitData(Map<String, Object> fieldData, float[] queryScores) {
        this.fieldData = fieldData;
        this.queryScores = queryScores;
    }

    public ScriptRankHitData(StreamInput in) throws IOException {
        fieldData = in.readGenericMap();
        queryScores = in.readFloatArray();
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
        out.writeFloatArray(queryScores);
    }

    public Map<String, Object> getFieldData() {
        return fieldData;
    }

    public float[] getQueryScores() {
        return queryScores;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("field_data", fieldData);
        builder.array("query_scores", queryScores);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean isFragment() {
        return false;
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rank.feature;

import org.apache.lucene.search.Explanation;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.rank.RankDoc;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * A {@link RankDoc} that contains field data to be used later by the reranker on the coordinator node.
 */
public class RankFeatureDoc extends RankDoc {

    public static final String NAME = "rank_feature_doc";

    // TODO: update to support more than 1 fields; and not restrict to string data
    public String featureData;

    public RankFeatureDoc(int doc, float score, int shardIndex) {
        super(doc, score, shardIndex);
    }

    public RankFeatureDoc(StreamInput in) throws IOException {
        super(in);
        featureData = in.readOptionalString();
    }

    @Override
    public Explanation explain() {
        throw new UnsupportedOperationException("explain is not supported for {" + getClass() + "}");
    }

    public void featureData(String featureData) {
        this.featureData = featureData;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeOptionalString(featureData);
    }

    @Override
    protected boolean doEquals(RankDoc rd) {
        RankFeatureDoc other = (RankFeatureDoc) rd;
        return Objects.equals(this.featureData, other.featureData);
    }

    @Override
    protected int doHashCode() {
        return Objects.hashCode(featureData);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected void doToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("featureData", featureData);
    }
}

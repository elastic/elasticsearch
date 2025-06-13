/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.rank.feature;

import org.apache.lucene.search.Explanation;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.rank.RankDoc;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * A {@link RankDoc} that contains field data to be used later by the reranker on the coordinator node.
 */
public class RankFeatureDoc extends RankDoc {

    public static final String NAME = "rank_feature_doc";

    // TODO: update to support more than 1 fields; and not restrict to string data
    public List<String> featureData;

    public RankFeatureDoc(int doc, float score, int shardIndex) {
        super(doc, score, shardIndex);
    }

    public RankFeatureDoc(StreamInput in) throws IOException {
        super(in);
        if (in.getTransportVersion().onOrAfter(TransportVersions.RERANK_SNIPPETS)) {
            featureData = in.readOptionalStringCollectionAsList();
        } else {
            String featureDataString = in.readOptionalString();
            featureData = featureDataString == null ? null : List.of(featureDataString);
        }
    }

    @Override
    public Explanation explain(Explanation[] sources, String[] queryNames) {
        throw new UnsupportedOperationException("explain is not supported for {" + getClass() + "}");
    }

    public void featureData(List<String> featureData) {
        this.featureData = featureData;
    }

    public void featureData(String featureData) {
        this.featureData = List.of(featureData);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().onOrAfter(TransportVersions.RERANK_SNIPPETS)) {
            out.writeOptionalStringCollection(featureData);
        } else {
            out.writeOptionalString(featureData.get(0));
        }
    }

    @Override
    protected boolean doEquals(RankDoc rd) {
        RankFeatureDoc other = (RankFeatureDoc) rd;
        return Objects.equals(this.featureData, other.featureData);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(featureData);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected void doToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.array("featureData", featureData);
    }
}

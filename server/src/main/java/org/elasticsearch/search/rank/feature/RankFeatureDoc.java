/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rank.feature;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.rank.RankDoc;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * A {@link RankDoc} that contains field data to be used later by the reranker on the coordinator node.
 */
public class RankFeatureDoc extends RankDoc {

    private final Map<String, Object> docFeatures;

    public RankFeatureDoc(int doc, float score, int shardIndex) {
        super(doc, score, shardIndex);
        this.docFeatures = new HashMap<>();

    }

    public RankFeatureDoc(StreamInput in) throws IOException {
        super(in);
        docFeatures = in.readMap(StreamInput::readString, StreamInput::readGenericValue);
    }

    public <T> void docFeatures(String key, T featureData) {
        this.docFeatures.put(key, featureData);
    }

    public Map<String, Object> docFeatures() {
        return this.docFeatures;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeMap(docFeatures, StreamOutput::writeString, StreamOutput::writeGenericValue);
    }

    @Override
    protected boolean doEquals(RankDoc rd) {
        RankFeatureDoc other = (RankFeatureDoc) rd;
        return Objects.equals(this.docFeatures, other.docFeatures);
    }

    @Override
    protected int doHashCode() {
        return Objects.hashCode(docFeatures);
    }
}

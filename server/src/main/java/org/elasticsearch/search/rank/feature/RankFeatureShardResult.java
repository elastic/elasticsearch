/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rank.feature;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.rank.RankShardResult;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * The result set of {@link RankFeatureDoc} docs for the shard.
 */
public class RankFeatureShardResult implements RankShardResult {

    public final RankFeatureDoc[] rankFeatureDocs;

    public RankFeatureShardResult(RankFeatureDoc[] rankFeatureDocs) {
        this.rankFeatureDocs = Objects.requireNonNull(rankFeatureDocs);
    }

    public RankFeatureShardResult(StreamInput in) throws IOException {
        rankFeatureDocs = in.readArray(RankFeatureDoc::new, RankFeatureDoc[]::new);
    }

    @Override
    public String getWriteableName() {
        return "rank_feature_shard";
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.RANK_FEATURE_PHASE_ADDED;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeArray(rankFeatureDocs);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RankFeatureShardResult that = (RankFeatureShardResult) o;
        return Arrays.equals(rankFeatureDocs, that.rankFeatureDocs);
    }

    @Override
    public int hashCode() {
        return 31 * Arrays.hashCode(rankFeatureDocs);
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "{rankFeatureDocs=" + Arrays.toString(rankFeatureDocs) + '}';
    }
}

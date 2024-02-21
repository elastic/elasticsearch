/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rank.script;

import org.apache.lucene.search.TopDocs;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.search.rank.RankShardResult;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class ScriptRankShardResult implements RankShardResult {

    private final List<TopDocs> topDocsList;

    public ScriptRankShardResult(List<TopDocs> topDocsList) {
        this.topDocsList = topDocsList;
    }

    public ScriptRankShardResult(StreamInput in) throws IOException {
        topDocsList = in.readCollectionAsList(Lucene::readTopDocsWithoutMaxScore);
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
        out.writeCollection(topDocsList, Lucene::writeTopDocsWithoutMaxScore);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ScriptRankShardResult that = (ScriptRankShardResult) o;
        return Objects.equals(topDocsList, that.topDocsList);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topDocsList);
    }

    @Override
    public String toString() {
        return "ScriptRankShardResult{" + "topDocsList=" + topDocsList + '}';
    }

    public List<TopDocs> getTopDocsList() {
        return topDocsList;
    }
}

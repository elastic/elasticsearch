/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.termvectors;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.xcontent.XContentBuilder;

import java.util.Map;

/**
 * The builder class for a term vector request.
 * Returns the term vector (doc frequency, positions, offsets) for a document.
 * <p>
 * Note, the {@code index}, {@code type} and {@code id} are
 * required.
 */
public class TermVectorsRequestBuilder extends ActionRequestBuilder<TermVectorsRequest, TermVectorsResponse> {

    public TermVectorsRequestBuilder(ElasticsearchClient client, TermVectorsAction action) {
        super(client, action, new TermVectorsRequest());
    }

    /**
     * Constructs a new term vector request builder for a document that will be fetch
     * from the provided index. Use {@code index}, {@code type} and
     * {@code id} to specify the document to load.
     */
    public TermVectorsRequestBuilder(ElasticsearchClient client, TermVectorsAction action, String index, String id) {
        super(client, action, new TermVectorsRequest(index, id));
    }

    /**
     * Sets the index where the document is located.
     */
    public TermVectorsRequestBuilder setIndex(String index) {
        request.index(index);
        return this;
    }

    /**
     * Sets the id of the document.
     */
    public TermVectorsRequestBuilder setId(String id) {
        request.id(id);
        return this;
    }

    /**
     * Sets the artificial document from which to generate term vectors.
     */
    public TermVectorsRequestBuilder setDoc(XContentBuilder xContent) {
        request.doc(xContent);
        return this;
    }

    /**
     * Sets the routing. Required if routing isn't id based.
     */
    public TermVectorsRequestBuilder setRouting(String routing) {
        request.routing(routing);
        return this;
    }

    /**
     * Sets the preference to execute the search. Defaults to randomize across shards. Can be set to
     * {@code _local} to prefer local shards or a custom value, which guarantees that the same order
     * will be used across different requests.
     */
    public TermVectorsRequestBuilder setPreference(String preference) {
        request.preference(preference);
        return this;
    }

    /**
     * Sets whether to return the start and stop offsets for each term if they were stored or
     * skip offsets.
     */
    public TermVectorsRequestBuilder setOffsets(boolean offsets) {
        request.offsets(offsets);
        return this;
    }

    /**
     * Sets whether to return the positions for each term if stored or skip.
     */
    public TermVectorsRequestBuilder setPositions(boolean positions) {
        request.positions(positions);
        return this;
    }

    /**
     * Sets whether to return the payloads for each term or skip.
     */
    public TermVectorsRequestBuilder setPayloads(boolean payloads) {
        request.payloads(payloads);
        return this;
    }

    /**
     * Sets whether to return the term statistics for each term in the shard or skip.
     */
    public TermVectorsRequestBuilder setTermStatistics(boolean termStatistics) {
        request.termStatistics(termStatistics);
        return this;
    }

    /**
     * Sets whether to return the field statistics for each term in the shard or skip.
     */
    public TermVectorsRequestBuilder setFieldStatistics(boolean fieldStatistics) {
        request.fieldStatistics(fieldStatistics);
        return this;
    }

    /**
     * Sets whether to return only term vectors for special selected fields. Returns the term
     * vectors for all fields if selectedFields == null
     */
    public TermVectorsRequestBuilder setSelectedFields(String... fields) {
        request.selectedFields(fields);
        return this;
    }

    /**
     * Sets whether term vectors are generated real-time.
     */
    public TermVectorsRequestBuilder setRealtime(boolean realtime) {
        request.realtime(realtime);
        return this;
    }

    /*
     * Sets the version, which will cause the get operation to only be performed if a matching
     * version exists and no changes happened on the doc since then.
     */
    public TermVectorsRequestBuilder setVersion(long version) {
        request.version(version);
        return this;
    }

    /*
     * Sets the versioning type. Defaults to {@link org.elasticsearch.index.VersionType#INTERNAL}.
     */
    public TermVectorsRequestBuilder setVersionType(VersionType versionType) {
        request.versionType(versionType);
        return this;
    }

    /**
     * Sets the analyzer used at each field when generating term vectors.
     */
    public TermVectorsRequestBuilder setPerFieldAnalyzer(Map<String, String> perFieldAnalyzer) {
        request.perFieldAnalyzer(perFieldAnalyzer);
        return this;
    }

    /**
     * Sets the settings for filtering out terms.
     */
    public TermVectorsRequestBuilder setFilterSettings(TermVectorsRequest.FilterSettings filterSettings) {
        request.filterSettings(filterSettings);
        return this;
    }
}

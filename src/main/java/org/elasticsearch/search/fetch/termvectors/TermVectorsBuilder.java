/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.fetch.termvectors;

import org.elasticsearch.action.termvectors.TermVectorsRequest;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

/**
 */
public class TermVectorsBuilder implements ToXContent {

    private Boolean fetchOnly;
    private String[] selectedFields;
    private Boolean realtime;
    private Map<String, String> perFieldAnalyzer;
    private Boolean positions;
    private Boolean offsets;
    private Boolean payloads;
    private Boolean fieldStatistics;
    private Boolean termStatistics;
    private Boolean dfs;
    private TermVectorsRequest.FilterSettings filterSettings;

    /**
     * Specifies whether to return the stored term vectors, disregarding any previous parameters.
     */
    public TermVectorsBuilder setFetchOnly(Boolean fetchOnly) {
        this.fetchOnly = fetchOnly;
        return this;
    }

    /**
     * Sets whether to return only term vectors for special selected fields. Returns the term
     * vectors for all fields if selectedFields == null
     */
    public TermVectorsBuilder setSelectedFields(String... fields) {
        this.selectedFields = fields;
        return this;
    }

    /**
     * Sets whether term vectors are generated real-time.
     */
    public TermVectorsBuilder setRealtime(Boolean realtime) {
        this.realtime = realtime;
        return this;
    }

    /**
     * Sets the analyzer used at each field when generating term vectors.
     */
    public TermVectorsBuilder setPerFieldAnalyzer(Map<String, String> perFieldAnalyzer) {
        this.perFieldAnalyzer = perFieldAnalyzer;
        return this;
    }

    /**
     * Sets the settings for filtering out terms.
     */
    public TermVectorsBuilder setFilterSettings(TermVectorsRequest.FilterSettings filterSettings) {
        this.filterSettings = filterSettings;
        return this;
    }

    /**
     * Sets whether to return the positions for each term if stored or skip.
     */
    public TermVectorsBuilder setPositions(boolean positions) {
        this.positions = positions;
        return this;
    }

    /**
     * Sets whether to return the start and stop offsets for each term if they were stored or
     * skip offsets.
     */
    public TermVectorsBuilder setOffsets(boolean offsets) {
        this.offsets = offsets;
        return this;
    }

    /**
     * Sets whether to return the payloads for each term or skip.
     */
    public TermVectorsBuilder setPayloads(boolean payloads) {
        this.payloads = payloads;
        return this;
    }

    /**
     * Sets whether to return the field statistics for each term in the shard or skip.
     */
    public TermVectorsBuilder setFieldStatistics(boolean fieldStatistics) {
        this.fieldStatistics = fieldStatistics;
        return this;
    }

    /**
     * Sets whether to return the term statistics for each term in the shard or skip.
     */
    public TermVectorsBuilder setTermStatistics(boolean termStatistics) {
        this.termStatistics = termStatistics;
        return this;
    }

    /**
     * Sets whether to use distributed frequencies instead of shard statistics.
     */
    public TermVectorsBuilder setDfs(boolean dfs) {
        this.dfs = dfs;
        return this;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (fetchOnly != null) {
            return builder.field("term_vectors", fetchOnly);
        }
        builder.startObject("term_vectors");
        if (selectedFields != null) {
            builder.startArray("fields");
            for (String field : selectedFields) {
                builder.value(field);
            }
            builder.endArray();
        }
        if (realtime != null) {
            builder.field("realtime", realtime);
        }
        if (perFieldAnalyzer != null) {
            builder.startObject("per_field_analyzer");
            for (Map.Entry<String, String> entry : perFieldAnalyzer.entrySet()) {
                builder.field(entry.getKey(), entry.getValue());
            }
            builder.endObject();
        }
        if (positions != null) {
            builder.field("positions", positions);
        }
        if (offsets != null) {
            builder.field("offsets", offsets);
        }
        if (realtime != null) {
            builder.field("realtime", realtime);
        }
        if (payloads != null) {
            builder.field("payloads", payloads);
        }
        if (fieldStatistics != null) {
            builder.field("field_statistics", fieldStatistics);
        }
        if (termStatistics != null) {
            builder.field("term_statistics", termStatistics);
        }
        if (dfs != null) {
            builder.field("dfs", dfs);
        }
        if (filterSettings != null) {
            filterSettings.toXContent(builder, params);
        }
        return builder.endObject();
    }
}

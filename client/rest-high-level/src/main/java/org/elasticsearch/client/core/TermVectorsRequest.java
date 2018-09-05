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

package org.elasticsearch.client.core;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public class TermVectorsRequest implements ToXContentObject, Validatable {

    private final String index;
    private final String type;
    private String id = null;
    private String routing = null;
    private String preference = null;
    private boolean realtime = true;
    private String[] fields = null;
    private boolean requestPositions = true;
    private boolean requestPayloads = true;
    private boolean requestOffsets = true;
    private boolean requestFieldStatistics = true;
    private boolean requestTermStatistics = false;
    private Map<String, String> perFieldAnalyzer = null;
    private Map<String, Integer> filterSettings = null;
    private XContentBuilder docBuilder = null;


    /**
     * Constructs TermVectorRequest for the given document
     * @param index - index of the document
     * @param type - type of the document
     * @param docId - id of the document
     */
    public TermVectorsRequest(String index, String type, String docId) {
        this.index = index;
        this.type = type;
        this.id = docId;
    }

    public String getIndex() {
        return index;
    }

    public String getType() {
        return type;
    }

    public String getId() {
        return id;
    }

    /**
     * Constructs TermVectorRequest for an artificial document
     * @param index - index of the document
     * @param type - type of the document
     */
    public TermVectorsRequest(String index, String type) {
        this.index = index;
        this.type = type;
    }

    /**
     * Sets the fields for which term vectors information should be retrieved
     * @param fields
     */
    public void setFields(String... fields) {
        this.fields = fields;
    }

    public String[] getFields() {
        return fields;
    }

    /**
     * Sets whether to request term positions
     * @param requestPositions
     */
    public void setPositions(boolean requestPositions) {
        this.requestPositions = requestPositions;
    }

    /**
     * Sets whether to request term payloads
     * @param requestPayloads
     */
    public void setPayloads(boolean requestPayloads) {
        this.requestPayloads = requestPayloads;
    }

    /**
     * Sets whether to request term offsets
     * @param requestOffsets
     */
    public void setOffsets(boolean requestOffsets) {
        this.requestOffsets = requestOffsets;
    }

    /**
     * Sets whether to request field statistics
     * @param requestFieldStatistics
     */
    public void setFieldStatistics(boolean requestFieldStatistics) {
        this.requestFieldStatistics = requestFieldStatistics;
    }

    /**
     * Sets whether to request term statistics
     * @param requestTermStatistics
     */
    public void setTermStatistics(boolean requestTermStatistics) {
        this.requestTermStatistics = requestTermStatistics;
    }

    /**
     * Sets different analyzers than the one at the fields
     * @param perFieldAnalyzer
     */
    public void setPerFieldAnalyzer(Map<String, String> perFieldAnalyzer) {
        this.perFieldAnalyzer = perFieldAnalyzer;
    }

    /**
     * Sets an artifical document on what to request _termvectors
     * @param docBuilder
     */
    public void setDoc(XContentBuilder docBuilder) {
        this.docBuilder = docBuilder;
    }

    /**
     * Sets conditions for terms filtering
     * @param filterSettings
     */
    public void setFilterSettings(Map<String, Integer> filterSettings) {
        this.filterSettings = filterSettings;
    }

    /**
     * Sets a routing to route a request to a particular shard
     * @param routing
     */
    public void setRouting(String routing) {
        this.routing = routing;
    }

    public String getRouting() {
        return routing;
    }

    /**
     * Set a preference of which shard copies to execute the request
     * @param preference
     */
    public void setPreference(String preference) {
        this.preference = preference;
    }

    public String getPreference() {
        return preference;
    }

    /**
     * Sets if the request should be realtime or near-realtime
     * @param realtime
     */
    public void setRealtime(boolean realtime) {
        this.realtime = realtime;
    }

    public boolean getRealtime() {
        return realtime;
    }


    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        // set values only when different from defaults
        if (requestPositions == false) builder.field("positions", false);
        if (requestPayloads == false) builder.field("payloads", false);
        if (requestOffsets == false) builder.field("offsets", false);
        if (requestFieldStatistics == false) builder.field("field_statistics", false);
        if (requestTermStatistics) builder.field("term_statistics", true);
        if (perFieldAnalyzer != null) builder.field("per_field_analyzer", perFieldAnalyzer);

        if (docBuilder != null) {
            BytesReference doc = BytesReference.bytes(docBuilder);
            try (InputStream stream = doc.streamInput()) {
                builder.rawField("doc", stream, docBuilder.contentType());
            }
        }

        if (filterSettings != null) {
            builder.startObject("filter");
            if (filterSettings.containsKey("max_num_terms")) builder.field("max_num_terms", filterSettings.get("max_num_terms"));
            if (filterSettings.containsKey("min_term_freq")) builder.field("min_term_freq", filterSettings.get("min_term_freq"));
            if (filterSettings.containsKey("max_term_freq")) builder.field("max_term_freq", filterSettings.get("max_term_freq"));
            if (filterSettings.containsKey("min_doc_freq")) builder.field("min_doc_freq", filterSettings.get("min_doc_freq"));
            if (filterSettings.containsKey("max_doc_freq")) builder.field("max_doc_freq", filterSettings.get("max_doc_freq"));
            if (filterSettings.containsKey("min_word_length")) builder.field("min_word_length", filterSettings.get("min_word_length"));
            if (filterSettings.containsKey("max_word_length")) builder.field("max_word_length", filterSettings.get("max_word_length"));
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

}

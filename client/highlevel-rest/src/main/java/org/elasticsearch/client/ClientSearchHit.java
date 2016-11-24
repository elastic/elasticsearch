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

package org.elasticsearch.client;

import org.apache.lucene.search.Explanation;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class ClientSearchHit implements SearchHit {

    private Map<String, Object> hit;
    private XContentAccessor objectPath;

    public ClientSearchHit(Map<String, Object> hit) {
        this.hit = hit;
        this.objectPath = new XContentAccessor(this.hit);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.map(this.hit);
        builder.endObject();
        return builder;
    }

    @Override
    public Iterator<SearchHitField> iterator() {
        // TODO
        return null;
    }

    @Override
    public float score() {
        return ((Double) this.objectPath.evaluate("_score")).floatValue();
    }

    @Override
    public float getScore() {
        return score();
    }

    @Override
    public String index() {
        return (String) this.objectPath.evaluate("_index");
    }

    @Override
    public String getIndex() {
        return index();
    }

    @Override
    public String id() {
        return (String) this.objectPath.evaluate("_id");
    }

    @Override
    public String getId() {
        return id();
    }

    @Override
    public String type() {
        return (String) this.objectPath.evaluate("_type");
    }

    @Override
    public String getType() {
        return type();
    }

    @Override
    public NestedIdentity getNestedIdentity() {
        // TODO
        return null;
    }

    @Override
    public long version() {
        Long version = this.objectPath.evaluateAsLong("_version");
        if (version == null) {
            return -1L; // same as returned by InternalSearchHit if version not set
        }
        return version;
    }

    @Override
    public long getVersion() {
        return version();
    }

    @Override
    public BytesReference sourceRef() {
        return new BytesArray(sourceAsString());
    }

    @Override
    public BytesReference getSourceRef() {
        return sourceRef();
    }

    @Override
    public byte[] source() {
        return BytesReference.toBytes(sourceRef());
    }

    @Override
    public boolean hasSource() {
        return this.objectPath.evaluate("_source") != null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Map<String, Object> getSource() {
        Object source = this.objectPath.evaluate("_source");
        if (source == null) {
            return null;
        }
        return (Map<String, Object>) source;
    }

    @Override
    public String sourceAsString() {
        return mapToString(getSource());
    }

    @Override
    public String getSourceAsString() {
        return sourceAsString();
    }

    @Override
    public Map<String, Object> sourceAsMap() throws ElasticsearchParseException {
        return getSource();
    }

    @Override
    public Explanation explanation() {
        // TODO
        return null;
    }

    @Override
    public Explanation getExplanation() {
        // TODO
        return null;
    }

    @Override
    public SearchHitField field(String fieldName) {
        // TODO
        return null;
    }

    @Override
    public Map<String, SearchHitField> fields() {
        // TODO
        return null;
    }

    @Override
    public Map<String, SearchHitField> getFields() {
        // TODO
        return null;
    }

    @Override
    public Map<String, HighlightField> highlightFields() {
        // TODO
        return null;
    }

    @Override
    public Map<String, HighlightField> getHighlightFields() {
        // TODO
        return null;
    }

    @Override
    public Object[] sortValues() {
        // TODO
        return null;
    }

    @Override
    public Object[] getSortValues() {
        // TODO
        return null;
    }

    @Override
    public String[] matchedQueries() {
        // TODO
        return null;
    }

    @Override
    public String[] getMatchedQueries() {
        // TODO
        return null;
    }

    @Override
    public SearchShardTarget shard() {
        // TODO
        return null;
    }

    @Override
    public SearchShardTarget getShard() {
        // TODO
        return null;
    }

    @Override
    public Map<String, SearchHits> getInnerHits() {
        // TODO
        return null;
    }

    private static String mapToString(Map<String, Object> map) {
        XContentBuilder builder;
        try {
            builder = XContentFactory.jsonBuilder();
            builder.prettyPrint();
            builder.map(map);
            return builder.string();
        } catch (IOException e) {
            throw new ElasticsearchException(e);
        }
    }
}

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
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.internal.InternalSearchHitField;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class ClientSearchHit implements Iterable<SearchHitField> {

    private final XContentAccessor objectPath;

    public ClientSearchHit(Map<String, Object> hit) {
        this.objectPath = new XContentAccessor(hit);
    }

    @Override
    public Iterator<SearchHitField> iterator() {
        return getFields().values().iterator();
    }

    public float getScore() {
        return this.objectPath.evaluateDouble("_score").floatValue();
    }

    public String getIndex() {
        return this.objectPath.evaluateString("_index");
    }

    public String getId() {
        return this.objectPath.evaluateString("_id");
    }

    public String getType() {
        return this.objectPath.evaluateString("_type");
    }

    public NestedIdentity getNestedIdentity() {
        // TODO
        return null;
    }

    public long getVersion() {
        Long version = this.objectPath.evaluateLong("_version");
        if (version == null) {
            return -1L; // same as returned by InternalSearchHit if version not set
        }
        return version;
    }

    //TODO add hasElement support to XContentAccessor?
    public boolean hasSource() {
        return this.objectPath.evaluate("_source") != null;
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> getSourceAsMap() {
        Object source = this.objectPath.evaluate("_source");
        if (source == null) {
            return null;
        }
        return (Map<String, Object>) source;
    }

    //TODO all these getSourceAs* methods are a bit misleading as after all they are all based on the map that we have already parsed into
    public String getSourceAsString() {
        return mapToString(getSourceAsMap());
    }

    public byte[] getSourceAsBytes() {
        return getSourceAsString().getBytes(StandardCharsets.UTF_8);
    }

    public Explanation getExplanation() {
        // TODO
        return null;
    }

    public SearchHitField getField(String fieldName) {
        return getFields().get(fieldName);
    }

    @SuppressWarnings("unchecked")
    public Map<String, SearchHitField> getFields() {
        Map<String, Object> originalMap = (Map<String, Object>) this.objectPath.evaluate("fields");
        Map<String, SearchHitField> fields = new HashMap<>(originalMap.size());
        for (Entry<String, Object> original : originalMap.entrySet()) {
            fields.put(original.getKey(), new InternalSearchHitField(original.getKey(), (List<Object>) original.getValue()));
        }
        return fields;
    }

    @SuppressWarnings("unchecked")
    //TODO do we have to copy over HighlightField class, SearchFieldHit and so on? or do we reuse from core?
    public Map<String, HighlightField> getHighlightFields() {
        Map<String, Object> originalMap = (Map<String, Object>) this.objectPath.evaluate("highlight");
        Map<String, HighlightField> fields = new HashMap<>(originalMap.size());
        for (Entry<String, Object> original : originalMap.entrySet()) {
            List<String> fragments = (List<String>) original.getValue();
            Text[] asText = new Text[fragments.size()];
            int i = 0;
            for (String fragment : fragments) {
                asText[i] = new Text(fragment);
                i++;
            }
            fields.put(original.getKey(), new HighlightField(original.getKey(), asText));
        }
        return fields;
    }

    @SuppressWarnings("unchecked")
    public Object[] getSortValues() {
        return ((List<Object>) this.objectPath.evaluate("sort")).toArray();
    }

    @SuppressWarnings("unchecked")
    public String[] getMatchedQueries() {
        List<String> matched = (List<String>) this.objectPath.evaluate("matched_queries");
        return matched.toArray(new String[matched.size()]);
    }

    @SuppressWarnings("unchecked")
    public Map<String, ClientSearchHits> getInnerHits() {
        Map<String, Object> originalMap = (Map<String, Object>) this.objectPath.evaluate("inner_hits");
        Map<String, ClientSearchHits> innerHits = new HashMap<>(originalMap.size());
        for (Entry<String, Object> original : originalMap.entrySet()) {
            ClientSearchHits hits = new ClientSearchHits((Map<String, Object>) original.getValue());
            innerHits.put(original.getKey(), hits);
        }
        return innerHits;
    }

    private static String mapToString(Map<String, Object> map) {
        XContentBuilder builder;
        try {
            builder = XContentFactory.jsonBuilder();
            //TODO do we need to pretty print?
            builder.prettyPrint();
            builder.map(map);
            return builder.string();
        } catch (IOException e) {
            throw new ElasticsearchException(e);
        }
    }
}

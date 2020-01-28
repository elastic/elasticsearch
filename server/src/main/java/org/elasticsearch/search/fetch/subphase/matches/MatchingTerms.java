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

package org.elasticsearch.search.fetch.subphase.matches;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class MatchingTerms extends MatchesResult {

    private static final String ROOT = "_root";

    private final Map<String, List<String>> allMatchingTerms;
    private final Map<String, Map<String, List<String>>> termsPerNamedQuery;

    public MatchingTerms(Map<String, List<String>> allMatchingTerms, Map<String, Map<String, List<String>>> termsPerNamedQuery) {
        this.allMatchingTerms = allMatchingTerms;
        this.termsPerNamedQuery = termsPerNamedQuery;
    }

    public MatchingTerms(StreamInput in) throws IOException {
        allMatchingTerms = in.readMapOfLists(StreamInput::readString, StreamInput::readString);
        termsPerNamedQuery = in.readMap(StreamInput::readString,
            i -> i.readMapOfLists(StreamInput::readString, StreamInput::readString));
    }

    public Set<String> matchedFields() {
        return allMatchingTerms.keySet();
    }

    public Set<String> matchedFields(String query) {
        if (termsPerNamedQuery.containsKey(query)) {
            return termsPerNamedQuery.get(query).keySet();
        }
        return Collections.emptySet();
    }

    public List<String> matches(String field) {
        return allMatchingTerms.get(field);
    }

    public List<String> matches(String query, String field) {
        if (termsPerNamedQuery.containsKey(query)) {
            return termsPerNamedQuery.get(query).get(field);
        }
        return Collections.emptyList();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMapOfLists(allMatchingTerms, StreamOutput::writeString, StreamOutput::writeString);
        out.writeMap(termsPerNamedQuery, StreamOutput::writeString,
            (o, m) -> o.writeMapOfLists(m, StreamOutput::writeString, StreamOutput::writeString));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (allMatchingTerms.isEmpty()) {
            return builder.startObject(MatchingTermsProcessor.NAME).endObject();
        }
        builder.startObject(MatchingTermsProcessor.NAME);
        builder.field(ROOT);
        builder.map(allMatchingTerms);
        for (String query : termsPerNamedQuery.keySet()) {
            builder.field(query);
            builder.map(termsPerNamedQuery.get(query));
        }
        return builder.endObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MatchingTerms that = (MatchingTerms) o;
        return Objects.equals(allMatchingTerms, that.allMatchingTerms) &&
            Objects.equals(termsPerNamedQuery, that.termsPerNamedQuery);
    }

    @Override
    public int hashCode() {
        return Objects.hash(allMatchingTerms, termsPerNamedQuery);
    }

    @SuppressWarnings("unchecked")
    public static MatchingTerms fromXContent(XContentParser parser) throws IOException {
        Map<String, Object> map = parser.map();
        Map<String, List<String>> allTerms = new HashMap<>();
        Map<String, Map<String, List<String>>> perQuery = new HashMap<>();
        for (String query : map.keySet()) {
            if (ROOT.equals(query)) {
                allTerms = (Map<String, List<String>>) map.get(ROOT);
            }
            else {
                perQuery.put(query, (Map<String, List<String>>)map.get(query));
            }
        }
        return new MatchingTerms(allTerms, perQuery);
    }

    @Override
    public String getWriteableName() {
        return MatchingTermsProcessor.NAME;
    }
}

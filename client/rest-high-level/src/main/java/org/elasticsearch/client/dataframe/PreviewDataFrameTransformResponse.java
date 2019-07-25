/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.dataframe;

import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class PreviewDataFrameTransformResponse {

    private static final String PREVIEW = "preview";
    private static final String MAPPINGS = "mappings";

    @SuppressWarnings("unchecked")
    public static PreviewDataFrameTransformResponse fromXContent(final XContentParser parser) throws IOException {
        Map<String, Object> previewMap = parser.mapOrdered();
        Object previewDocs = previewMap.get(PREVIEW);
        Object mappings = previewMap.get(MAPPINGS);
        return new PreviewDataFrameTransformResponse((List<Map<String, Object>>) previewDocs, (Map<String, Object>) mappings);
    }

    private List<Map<String, Object>> docs;
    private Map<String, Object> mappings;

    public PreviewDataFrameTransformResponse(List<Map<String, Object>> docs, Map<String, Object> mappings) {
        this.docs = docs;
        this.mappings = mappings;
    }

    public List<Map<String, Object>> getDocs() {
        return docs;
    }

    public Map<String, Object> getMappings() {
        return mappings;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }

        PreviewDataFrameTransformResponse other = (PreviewDataFrameTransformResponse) obj;
        return Objects.equals(other.docs, docs) && Objects.equals(other.mappings, mappings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(docs, mappings);
    }

}

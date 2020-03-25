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

package org.elasticsearch.client.ilm;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * The response object returned by the Explain Lifecycle API.
 *
 * Since the API can be run over multiple indices the response provides a map of
 * index to the explanation of the lifecycle status for that index.
 */
public class ExplainLifecycleResponse implements ToXContentObject {

    private static final ParseField INDICES_FIELD = new ParseField("indices");

    private Map<String, IndexLifecycleExplainResponse> indexResponses;

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<ExplainLifecycleResponse, Void> PARSER = new ConstructingObjectParser<>(
            "explain_lifecycle_response", a -> new ExplainLifecycleResponse(((List<IndexLifecycleExplainResponse>) a[0]).stream()
                    .collect(Collectors.toMap(IndexLifecycleExplainResponse::getIndex, Function.identity()))));
    static {
        PARSER.declareNamedObjects(ConstructingObjectParser.constructorArg(), (p, c, n) -> IndexLifecycleExplainResponse.PARSER.apply(p, c),
                INDICES_FIELD);
    }

    public static ExplainLifecycleResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public ExplainLifecycleResponse(Map<String, IndexLifecycleExplainResponse> indexResponses) {
        this.indexResponses = indexResponses;
    }

    /**
     * @return a map of the responses from each requested index. The maps key is
     *         the index name and the value is the
     *         {@link IndexLifecycleExplainResponse} describing the current
     *         lifecycle status of that index
     */
    public Map<String, IndexLifecycleExplainResponse> getIndexResponses() {
        return indexResponses;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startObject(INDICES_FIELD.getPreferredName());
        for (IndexLifecycleExplainResponse indexResponse : indexResponses.values()) {
            builder.field(indexResponse.getIndex(), indexResponse);
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexResponses);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        ExplainLifecycleResponse other = (ExplainLifecycleResponse) obj;
        return Objects.equals(indexResponses, other.indexResponses);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

}

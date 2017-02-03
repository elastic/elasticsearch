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

package org.elasticsearch.index.rankeval;

import org.elasticsearch.action.support.ToXContentToBytes;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

/**
 * Defines a QA specification: All end user supplied query intents will be mapped to the search request specified in this search request
 * template and executed against the targetIndex given. Any filters that should be applied in the target system can be specified as well.
 *
 * The resulting document lists can then be compared against what was specified in the set of rated documents as part of a QAQuery.
 * */
@SuppressWarnings("unchecked")
public class RatedRequest extends ToXContentToBytes implements Writeable {
    private String id;
    private List<String> indices = new ArrayList<>();
    private List<String> types = new ArrayList<>();
    private List<String> summaryFields = new ArrayList<>();
    /** Collection of rated queries for this query QA specification.*/
    private List<RatedDocument> ratedDocs = new ArrayList<>();
    /** Search request to execute for this rated request, can be null, if template and corresponding params are supplied. */
    @Nullable
    private SearchSourceBuilder testRequest;
    /** Map of parameters to use for filling a query template, can be used instead of providing testRequest. */
    private Map<String, Object> params = new HashMap<>();
    @Nullable
    private String templateId;

    public RatedRequest(
            String id, List<RatedDocument> ratedDocs, SearchSourceBuilder testRequest, Map<String, Object> params, String templateId) {
        if (params != null && (params.size() > 0 && testRequest != null)) {
            throw new IllegalArgumentException(
                    "Ambiguous rated request: Set both, verbatim test request and test request template parameters.");
        }
        if (templateId != null && testRequest != null) {
            throw new IllegalArgumentException(
                    "Ambiguous rated request: Set both, verbatim test request and test request template parameters.");
        }
        if ((params == null || params.size() < 1) && testRequest == null) {
            throw new IllegalArgumentException(
                    "Need to set at least test request or test request template parameters.");
        }
        if ((params != null && params.size() > 0) && templateId == null) {
            throw new IllegalArgumentException(
                    "If template parameters are supplied need to set id of template to apply them to too.");
        }
        // No documents with same _index/_type/id allowed.
        Set<DocumentKey> docKeys = new HashSet<>();
        for (RatedDocument doc : ratedDocs) {
            if (docKeys.add(doc.getKey()) == false) {
                String docKeyToString = doc.getKey().toString().replaceAll("\n", "").replaceAll("  ", " ");
                throw new IllegalArgumentException(
                        "Found duplicate rated document key [" + docKeyToString + "]");
            }
        }

        this.id = id;
        this.testRequest = testRequest;
        this.ratedDocs = ratedDocs;
        if (params != null) {
            this.params = params;
        }
        this.templateId = templateId;
    }

    public RatedRequest(String id, List<RatedDocument> ratedDocs, Map<String, Object> params, String templateId) {
        this(id, ratedDocs, null, params, templateId);
    }

    public RatedRequest(String id, List<RatedDocument> ratedDocs, SearchSourceBuilder testRequest) {
        this(id, ratedDocs, testRequest, new HashMap<>(), null);
    }

    public RatedRequest(StreamInput in) throws IOException {
        this.id = in.readString();
        testRequest = in.readOptionalWriteable(SearchSourceBuilder::new);

        int indicesSize = in.readInt();
        indices = new ArrayList<>(indicesSize);
        for (int i = 0; i < indicesSize; i++) {
            this.indices.add(in.readString());
        }
        int typesSize = in.readInt();
        types = new ArrayList<>(typesSize);
        for (int i = 0; i < typesSize; i++) {
            this.types.add(in.readString());
        }
        int intentSize = in.readInt();
        ratedDocs = new ArrayList<>(intentSize);
        for (int i = 0; i < intentSize; i++) {
            ratedDocs.add(new RatedDocument(in));
        }
        this.params = in.readMap();
        int summaryFieldsSize = in.readInt();
        summaryFields = new ArrayList<>(summaryFieldsSize);
        for (int i = 0; i < summaryFieldsSize; i++) {
            this.summaryFields.add(in.readString());
        }
        this.templateId = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeOptionalWriteable(testRequest);

        out.writeInt(indices.size());
        for (String index : indices) {
            out.writeString(index);
        }
        out.writeInt(types.size());
        for (String type : types) {
            out.writeString(type);
        }
        out.writeInt(ratedDocs.size());
        for (RatedDocument ratedDoc : ratedDocs) {
            ratedDoc.writeTo(out);
        }
        out.writeMap(params);
        out.writeInt(summaryFields.size());
        for (String fieldName : summaryFields) {
            out.writeString(fieldName);
        }
        out.writeOptionalString(this.templateId);
    }

    public SearchSourceBuilder getTestRequest() {
        return testRequest;
    }

    public void setIndices(List<String> indices) {
        this.indices = indices;
    }

    public List<String> getIndices() {
        return indices;
    }

    public void setTypes(List<String> types) {
        this.types = types;
    }

    public List<String> getTypes() {
        return types;
    }

    /** Returns a user supplied spec id for easier referencing. */
    public String getId() {
        return id;
    }

    /** Returns a list of rated documents to evaluate. */
    public List<RatedDocument> getRatedDocs() {
        return ratedDocs;
    }

    public Map<String, Object> getParams() {
        return this.params;
    }

    public String getTemplateId() {
        return this.templateId;
    }

    /** Returns a list of fields that are included in the docs summary of matched documents. */
    public List<String> getSummaryFields() {
        return summaryFields;
    }

    public void setSummaryFields(List<String> summaryFields) {
        if (summaryFields == null) {
            throw new IllegalArgumentException("Setting summaryFields to null not allowed.");
        }
        this.summaryFields = summaryFields;
    }

    private static final ParseField ID_FIELD = new ParseField("id");
    private static final ParseField REQUEST_FIELD = new ParseField("request");
    private static final ParseField RATINGS_FIELD = new ParseField("ratings");
    private static final ParseField PARAMS_FIELD = new ParseField("params");
    private static final ParseField FIELDS_FIELD = new ParseField("summary_fields");
    private static final ParseField TEMPLATE_ID_FIELD = new ParseField("template_id");

    private static final ConstructingObjectParser<RatedRequest, QueryParseContext> PARSER =
            new ConstructingObjectParser<>("requests", a -> new RatedRequest(
                    (String) a[0], (List<RatedDocument>) a[1], (SearchSourceBuilder) a[2], (Map<String, Object>) a[3], (String) a[4]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), ID_FIELD);
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), (p, c) -> {
                return RatedDocument.fromXContent(p);
        }, RATINGS_FIELD);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> {
            try {
                return SearchSourceBuilder.fromXContent(c);
            } catch (IOException ex) {
                throw new ParsingException(p.getTokenLocation(), "error parsing request", ex);
            }
        } , REQUEST_FIELD);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> {
            try {
                return (Map) p.map();
            } catch (IOException ex) {
                throw new ParsingException(p.getTokenLocation(), "error parsing ratings", ex);
            }
        }, PARAMS_FIELD);
        PARSER.declareStringArray(RatedRequest::setSummaryFields, FIELDS_FIELD);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), TEMPLATE_ID_FIELD);
    }

    /**
     * Parses {@link RatedRequest} from rest representation:
     *
     * Example:
     *  {
     *   "id": "coffee_query",
     *   "request": {
     *           "query": {
     *               "bool": {
     *                   "must": [
     *                       {"match": {"beverage": "coffee"}},
     *                       {"term": {"browser": {"value": "safari"}}},
     *                       {"term": {"time_of_day": {"value": "morning","boost": 2}}},
     *                       {"term": {"ip_location": {"value": "ams","boost": 10}}}]}
     *           },
     *           "size": 10
     *   },
     *   "summary_fields" : ["body"],
     *   "ratings": [{ "1": 1 }, { "2": 0 }, { "3": 1 } ]
     *  }
     */
    public static RatedRequest fromXContent(XContentParser parser) {
        return PARSER.apply(parser, new QueryParseContext(parser));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ID_FIELD.getPreferredName(), this.id);
        if (testRequest != null) {
            builder.field(REQUEST_FIELD.getPreferredName(), this.testRequest);
        }
        builder.startObject(PARAMS_FIELD.getPreferredName());
        for (Entry<String, Object> entry : this.params.entrySet()) {
            builder.field(entry.getKey(), entry.getValue());
        }
        builder.endObject();
        builder.startArray(RATINGS_FIELD.getPreferredName());
        for (RatedDocument doc : this.ratedDocs) {
            doc.toXContent(builder, params);
        }
        builder.endArray();
        if (this.summaryFields.isEmpty() == false) {
            builder.startArray(FIELDS_FIELD.getPreferredName());
            for (String field : this.summaryFields) {
                builder.value(field);
            }
            builder.endArray();
        }
        if (this.templateId != null) {
            builder.field(TEMPLATE_ID_FIELD.getPreferredName(), this.templateId);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public final boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        RatedRequest other = (RatedRequest) obj;

        return Objects.equals(id, other.id) &&
                Objects.equals(testRequest, other.testRequest) &&
                Objects.equals(indices, other.indices) &&
                Objects.equals(types, other.types) &&
                Objects.equals(summaryFields, other.summaryFields) &&
                Objects.equals(ratedDocs, other.ratedDocs) &&
                Objects.equals(params, other.params) &&
                Objects.equals(templateId, other.templateId);
    }

    @Override
    public final int hashCode() {
        return Objects.hash(id, testRequest, indices, types, summaryFields, ratedDocs, params, templateId);
    }
}

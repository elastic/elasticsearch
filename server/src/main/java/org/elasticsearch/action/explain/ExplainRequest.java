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

package org.elasticsearch.action.explain;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.single.shard.SingleShardRequest;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.internal.AliasFilter;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Explain request encapsulating the explain query and document identifier to get an explanation for.
 */
public class ExplainRequest extends SingleShardRequest<ExplainRequest> implements ToXContentObject {

    private static final ParseField QUERY_FIELD = new ParseField("query");

    private String id;
    private String routing;
    private String preference;
    private QueryBuilder query;
    private String[] storedFields;
    private FetchSourceContext fetchSourceContext;

    private AliasFilter filteringAlias = new AliasFilter(null, Strings.EMPTY_ARRAY);

    long nowInMillis;

    public ExplainRequest() {
    }

    public ExplainRequest(String index, String id) {
        this.index = index;
        this.id = id;
    }

    ExplainRequest(StreamInput in) throws IOException {
        super(in);
        if (in.getVersion().before(Version.V_8_0_0)) {
            String type = in.readString();
            assert MapperService.SINGLE_MAPPING_NAME.equals(type);
        }
        id = in.readString();
        routing = in.readOptionalString();
        preference = in.readOptionalString();
        query = in.readNamedWriteable(QueryBuilder.class);
        filteringAlias = new AliasFilter(in);
        storedFields = in.readOptionalStringArray();
        fetchSourceContext = in.readOptionalWriteable(FetchSourceContext::new);
        nowInMillis = in.readVLong();
    }

    public String id() {
        return id;
    }

    public ExplainRequest id(String id) {
        this.id = id;
        return this;
    }

    public String routing() {
        return routing;
    }

    public ExplainRequest routing(String routing) {
        this.routing = routing;
        return this;
    }

    /**
     * Simple sets the routing. Since the parent is only used to get to the right shard.
     */
    public ExplainRequest parent(String parent) {
        this.routing = parent;
        return this;
    }

    public String preference() {
        return preference;
    }

    public ExplainRequest preference(String preference) {
        this.preference = preference;
        return this;
    }

    public QueryBuilder query() {
        return query;
    }

    public ExplainRequest query(QueryBuilder query) {
        this.query = query;
        return this;
    }

    /**
     * Allows setting the {@link FetchSourceContext} for this request, controlling if and how _source should be returned.
     */
    public ExplainRequest fetchSourceContext(FetchSourceContext context) {
        this.fetchSourceContext = context;
        return this;
    }

    public FetchSourceContext fetchSourceContext() {
        return fetchSourceContext;
    }


    public String[] storedFields() {
        return storedFields;
    }

    public ExplainRequest storedFields(String[] fields) {
        this.storedFields = fields;
        return this;
    }

    public AliasFilter filteringAlias() {
        return filteringAlias;
    }

    public ExplainRequest filteringAlias(AliasFilter filteringAlias) {
        if (filteringAlias != null) {
            this.filteringAlias = filteringAlias;
        }

        return this;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validateNonNullIndex();
        if (Strings.isEmpty(id)) {
            validationException = addValidationError("id is missing", validationException);
        }
        if (query == null) {
            validationException = ValidateActions.addValidationError("query is missing", validationException);
        }
        return validationException;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getVersion().before(Version.V_8_0_0)) {
            out.writeString(MapperService.SINGLE_MAPPING_NAME);
        }
        out.writeString(id);
        out.writeOptionalString(routing);
        out.writeOptionalString(preference);
        out.writeNamedWriteable(query);
        filteringAlias.writeTo(out);
        out.writeOptionalStringArray(storedFields);
        out.writeOptionalWriteable(fetchSourceContext);
        out.writeVLong(nowInMillis);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(QUERY_FIELD.getPreferredName(), query);
        builder.endObject();
        return builder;
    }
}

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
package org.elasticsearch.search.collapse;

import org.apache.lucene.index.IndexOptions;
import org.elasticsearch.action.support.ToXContentToBytes;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.query.InnerHitBuilder;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.search.SearchContextException;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Objects;

/**
 * A builder that enables field collapsing on search request.
 */
public class CollapseBuilder extends ToXContentToBytes implements Writeable {
    public static final ParseField FIELD_FIELD = new ParseField("field");
    public static final ParseField INNER_HITS_FIELD = new ParseField("inner_hits");
    public static final ParseField MAX_CONCURRENT_GROUP_REQUESTS_FIELD = new ParseField("max_concurrent_group_searches");
    private static final ObjectParser<CollapseBuilder, QueryParseContext> PARSER =
        new ObjectParser<>("collapse", CollapseBuilder::new);

    static {
        PARSER.declareString(CollapseBuilder::setField, FIELD_FIELD);
        PARSER.declareInt(CollapseBuilder::setMaxConcurrentGroupRequests, MAX_CONCURRENT_GROUP_REQUESTS_FIELD);
        PARSER.declareObject(CollapseBuilder::setInnerHits,
            (p, c) -> InnerHitBuilder.fromXContent(c), INNER_HITS_FIELD);
    }

    private String field;
    private InnerHitBuilder innerHit;
    private int maxConcurrentGroupRequests = 0;

    private CollapseBuilder() {}

    /**
     * Public constructor
     * @param field The name of the field to collapse on
     */
    public CollapseBuilder(String field) {
        Objects.requireNonNull(field, "field must be non-null");
        this.field = field;
    }

    public CollapseBuilder(StreamInput in) throws IOException {
        this.field = in.readString();
        this.maxConcurrentGroupRequests = in.readVInt();
        this.innerHit = in.readOptionalWriteable(InnerHitBuilder::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(field);
        out.writeVInt(maxConcurrentGroupRequests);
        out.writeOptionalWriteable(innerHit);
    }

    public static CollapseBuilder fromXContent(QueryParseContext context) throws IOException {
        CollapseBuilder builder = PARSER.parse(context.parser(), new CollapseBuilder(), context);
        return builder;
    }

    // for object parser only
    private CollapseBuilder setField(String field) {
        if (Strings.isEmpty(field)) {
            throw new IllegalArgumentException("field name is null or empty");
        }
        this.field = field;
        return this;
    }

    public CollapseBuilder setInnerHits(InnerHitBuilder innerHit) {
        this.innerHit = innerHit;
        return this;
    }

    public CollapseBuilder setMaxConcurrentGroupRequests(int num) {
        if (num < 1) {
            throw new IllegalArgumentException("maxConcurrentGroupRequests` must be positive");
        }
        this.maxConcurrentGroupRequests = num;
        return this;
    }

    /**
     * The name of the field to collapse against
     */
    public String getField() {
        return this.field;
    }

    /**
     * The inner hit options to expand the collapsed results
     */
    public InnerHitBuilder getInnerHit() {
        return this.innerHit;
    }

    /**
     * Returns the amount of group requests that are allowed to be ran concurrently in the inner_hits phase.
     */
    public int getMaxConcurrentGroupRequests() {
        return maxConcurrentGroupRequests;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        innerToXContent(builder);
        builder.endObject();
        return builder;
    }

    private void innerToXContent(XContentBuilder builder) throws IOException {
        builder.field(FIELD_FIELD.getPreferredName(), field);
        if (maxConcurrentGroupRequests > 0) {
            builder.field(MAX_CONCURRENT_GROUP_REQUESTS_FIELD.getPreferredName(), maxConcurrentGroupRequests);
        }
        if (innerHit != null) {
            builder.field(INNER_HITS_FIELD.getPreferredName(), innerHit);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CollapseBuilder that = (CollapseBuilder) o;

        if (maxConcurrentGroupRequests != that.maxConcurrentGroupRequests) return false;
        if (!field.equals(that.field)) return false;
        return innerHit != null ? innerHit.equals(that.innerHit) : that.innerHit == null;

    }

    @Override
    public int hashCode() {
        int result = field.hashCode();
        result = 31 * result + (innerHit != null ? innerHit.hashCode() : 0);
        result = 31 * result + maxConcurrentGroupRequests;
        return result;
    }

    public CollapseContext build(SearchContext context) {
        if (context.scrollContext() != null) {
            throw new SearchContextException(context, "cannot use `collapse` in a scroll context");
        }
        if (context.searchAfter() != null) {
            throw new SearchContextException(context, "cannot use `collapse` in conjunction with `search_after`");
        }
        if (context.rescore() != null && context.rescore().isEmpty() == false) {
            throw new SearchContextException(context, "cannot use `collapse` in conjunction with `rescore`");
        }

        MappedFieldType fieldType = context.getQueryShardContext().fieldMapper(field);
        if (fieldType == null) {
            throw new SearchContextException(context, "no mapping found for `" + field + "` in order to collapse on");
        }
        if (fieldType instanceof KeywordFieldMapper.KeywordFieldType == false &&
            fieldType instanceof NumberFieldMapper.NumberFieldType == false) {
            throw new SearchContextException(context, "unknown type for collapse field `" + field +
                "`, only keywords and numbers are accepted");
        }

        if (fieldType.hasDocValues() == false) {
            throw new SearchContextException(context, "cannot collapse on field `" + field + "` without `doc_values`");
        }
        if (fieldType.indexOptions() == IndexOptions.NONE && innerHit != null) {
            throw new SearchContextException(context, "cannot expand `inner_hits` for collapse field `"
                + field + "`, " + "only indexed field can retrieve `inner_hits`");
        }
        return new CollapseContext(fieldType, innerHit);
    }
}

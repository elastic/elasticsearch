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
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.query.InnerHitBuilder;
import org.elasticsearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A builder that enables field collapsing on search request.
 */
public class CollapseBuilder implements Writeable, ToXContentObject {
    public static final ParseField FIELD_FIELD = new ParseField("field");
    public static final ParseField INNER_HITS_FIELD = new ParseField("inner_hits");
    public static final ParseField MAX_CONCURRENT_GROUP_REQUESTS_FIELD = new ParseField("max_concurrent_group_searches");
    private static final ObjectParser<CollapseBuilder, Void> PARSER =
        new ObjectParser<>("collapse", CollapseBuilder::new);

    static {
        PARSER.declareString(CollapseBuilder::setField, FIELD_FIELD);
        PARSER.declareInt(CollapseBuilder::setMaxConcurrentGroupRequests, MAX_CONCURRENT_GROUP_REQUESTS_FIELD);
        PARSER.declareField((parser, builder, context) -> {
            XContentParser.Token currentToken = parser.currentToken();
            if (currentToken == XContentParser.Token.START_OBJECT) {
                builder.setInnerHits(InnerHitBuilder.fromXContent(parser));
            } else if (currentToken == XContentParser.Token.START_ARRAY) {
                List<InnerHitBuilder> innerHitBuilders = new ArrayList<>();
                for (currentToken = parser.nextToken(); currentToken != XContentParser.Token.END_ARRAY; currentToken = parser.nextToken()) {
                    if (currentToken == XContentParser.Token.START_OBJECT) {
                        innerHitBuilders.add(InnerHitBuilder.fromXContent(parser));
                    } else {
                        throw new ParsingException(parser.getTokenLocation(), "Invalid token in inner_hits array");
                    }
                }

                builder.setInnerHits(innerHitBuilders);
            }
        }, INNER_HITS_FIELD, ObjectParser.ValueType.OBJECT_ARRAY);
    }

    private String field;
    private List<InnerHitBuilder> innerHits = Collections.emptyList();
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
        this.innerHits = in.readList(InnerHitBuilder::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(field);
        out.writeVInt(maxConcurrentGroupRequests);
        out.writeList(innerHits);
    }

    public static CollapseBuilder fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
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
        this.innerHits = Collections.singletonList(innerHit);
        return this;
    }

    public CollapseBuilder setInnerHits(List<InnerHitBuilder> innerHits) {
        this.innerHits = innerHits;
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
    public List<InnerHitBuilder> getInnerHits() {
        return this.innerHits;
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
        if (innerHits.isEmpty() == false) {
            if (innerHits.size() == 1) {
                builder.field(INNER_HITS_FIELD.getPreferredName(), innerHits.get(0));
            } else {
                builder.startArray(INNER_HITS_FIELD.getPreferredName());
                for (InnerHitBuilder innerHit : innerHits) {
                    innerHit.toXContent(builder, ToXContent.EMPTY_PARAMS);
                }
                builder.endArray();
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CollapseBuilder that = (CollapseBuilder) o;

        if (maxConcurrentGroupRequests != that.maxConcurrentGroupRequests) return false;
        if (!field.equals(that.field)) return false;
        return Objects.equals(innerHits, that.innerHits);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(field, innerHits);
        result = 31 * result + maxConcurrentGroupRequests;
        return result;
    }

    public CollapseContext build(QueryShardContext queryShardContext) {
        MappedFieldType fieldType = queryShardContext.fieldMapper(field);
        if (fieldType == null) {
            throw new IllegalArgumentException("no mapping found for `" + field + "` in order to collapse on");
        }
        if (fieldType instanceof KeywordFieldMapper.KeywordFieldType == false &&
            fieldType instanceof NumberFieldMapper.NumberFieldType == false) {
            throw new IllegalArgumentException("unknown type for collapse field `" + field +
                "`, only keywords and numbers are accepted");
        }

        if (fieldType.hasDocValues() == false) {
            throw new IllegalArgumentException("cannot collapse on field `" + field + "` without `doc_values`");
        }
        if (fieldType.indexOptions() == IndexOptions.NONE && (innerHits != null && !innerHits.isEmpty())) {
            throw new IllegalArgumentException("cannot expand `inner_hits` for collapse field `"
                + field + "`, " + "only indexed field can retrieve `inner_hits`");
        }

        return new CollapseContext(field, fieldType, innerHits);
    }
}

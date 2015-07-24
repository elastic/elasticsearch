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

package org.elasticsearch.index.query;

import org.apache.lucene.search.*;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.internal.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.object.ObjectMapper;

import java.io.IOException;
import java.util.Collection;
import java.util.Objects;

/**
 * Constructs a filter that have only null values or no value in the original field.
 */
public class MissingQueryBuilder extends AbstractQueryBuilder<MissingQueryBuilder> {

    public static final String NAME = "missing";

    public static final boolean DEFAULT_NULL_VALUE = false;

    public static final boolean DEFAULT_EXISTENCE_VALUE = true;

    private final String fieldPattern;

    private boolean nullValue = DEFAULT_NULL_VALUE;

    private boolean existence = DEFAULT_EXISTENCE_VALUE;

    static final MissingQueryBuilder PROTOTYPE = new MissingQueryBuilder(null);

    public MissingQueryBuilder(String fieldPattern) {
        this.fieldPattern = fieldPattern;
    }

    public String fieldPattern() {
        return this.fieldPattern;
    }

    /**
     * Should the missing filter automatically include fields with null value configured in the
     * mappings. Defaults to <tt>false</tt>.
     */
    public MissingQueryBuilder nullValue(boolean nullValue) {
        this.nullValue = nullValue;
        return this;
    }

    /**
     * Returns true if the missing filter will include documents where the field contains a null value, otherwise
     * these documents will not be included.
     */
    public boolean nullValue() {
        return this.nullValue;
    }

    /**
     * Should the missing filter include documents where the field doesn't exist in the docs.
     * Defaults to <tt>true</tt>.
     */
    public MissingQueryBuilder existence(boolean existence) {
        this.existence = existence;
        return this;
    }

    /**
     * Returns true if the missing filter will include documents where the field has no values, otherwise
     * these documents will not be included.
     */
    public boolean existence() {
        return this.existence;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field("field", fieldPattern);
        builder.field("null_value", nullValue);
        builder.field("existence", existence);
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    protected Query doToQuery(QueryParseContext parseContext) throws IOException {
        return newFilter(parseContext, fieldPattern, existence, nullValue);
    }

    public static Query newFilter(QueryParseContext parseContext, String fieldPattern, boolean existence, boolean nullValue) {
        if (!existence && !nullValue) {
            throw new QueryParsingException(parseContext, "missing must have either existence, or null_value, or both set to true");
        }

        final FieldNamesFieldMapper.FieldNamesFieldType fieldNamesFieldType = (FieldNamesFieldMapper.FieldNamesFieldType) parseContext.mapperService().fullName(FieldNamesFieldMapper.NAME);
        if (fieldNamesFieldType == null) {
            // can only happen when no types exist, so no docs exist either
            return Queries.newMatchNoDocsQuery();
        }

        ObjectMapper objectMapper = parseContext.getObjectMapper(fieldPattern);
        if (objectMapper != null) {
            // automatic make the object mapper pattern
            fieldPattern = fieldPattern + ".*";
        }

        Collection<String> fields = parseContext.simpleMatchToIndexNames(fieldPattern);
        if (fields.isEmpty()) {
            if (existence) {
                // if we ask for existence of fields, and we found none, then we should match on all
                return Queries.newMatchAllQuery();
            }
            return null;
        }

        Query existenceFilter = null;
        Query nullFilter = null;

        if (existence) {
            BooleanQuery boolFilter = new BooleanQuery();
            for (String field : fields) {
                MappedFieldType fieldType = parseContext.fieldMapper(field);
                Query filter = null;
                if (fieldNamesFieldType.isEnabled()) {
                    final String f;
                    if (fieldType != null) {
                        f = fieldType.names().indexName();
                    } else {
                        f = field;
                    }
                    filter = fieldNamesFieldType.termQuery(f, parseContext);
                }
                // if _field_names are not indexed, we need to go the slow way
                if (filter == null && fieldType != null) {
                    filter = fieldType.rangeQuery(null, null, true, true);
                }
                if (filter == null) {
                    filter = new TermRangeQuery(field, null, null, true, true);
                }
                boolFilter.add(filter, BooleanClause.Occur.SHOULD);
            }

            existenceFilter = boolFilter;
            existenceFilter = Queries.not(existenceFilter);;
        }

        if (nullValue) {
            for (String field : fields) {
                MappedFieldType fieldType = parseContext.fieldMapper(field);
                if (fieldType != null) {
                    nullFilter = fieldType.nullValueQuery();
                }
            }
        }

        Query filter;
        if (nullFilter != null) {
            if (existenceFilter != null) {
                BooleanQuery combined = new BooleanQuery();
                combined.add(existenceFilter, BooleanClause.Occur.SHOULD);
                combined.add(nullFilter, BooleanClause.Occur.SHOULD);
                // cache the not filter as well, so it will be faster
                filter = combined;
            } else {
                filter = nullFilter;
            }
        } else {
            filter = existenceFilter;
        }

        if (filter == null) {
            return null;
        }

        return new ConstantScoreQuery(filter);
    }

    @Override
    public QueryValidationException validate() {
        QueryValidationException validationException = null;
        if (Strings.isEmpty(this.fieldPattern)) {
            validationException = addValidationError("missing must be provided with a [field]", validationException);
        }
        if (!existence && !nullValue) {
            validationException = addValidationError("missing must have either existence, or null_value, or both set to true", validationException);
        }
        return validationException;
    }

    @Override
    protected MissingQueryBuilder doReadFrom(StreamInput in) throws IOException {
        MissingQueryBuilder missingQueryBuilder = new MissingQueryBuilder(in.readString());
        missingQueryBuilder.nullValue = in.readBoolean();
        missingQueryBuilder.existence = in.readBoolean();
        return missingQueryBuilder;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(fieldPattern);
        out.writeBoolean(nullValue);
        out.writeBoolean(existence);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldPattern, nullValue, existence);
    }

    @Override
    protected boolean doEquals(MissingQueryBuilder other) {
        return Objects.equals(fieldPattern, other.fieldPattern) &&
                Objects.equals(nullValue, other.nullValue) &&
                Objects.equals(existence, other.existence);
    }
}

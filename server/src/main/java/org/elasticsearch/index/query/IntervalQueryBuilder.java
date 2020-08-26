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

import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.queries.intervals.IntervalQuery;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.MappedFieldType;

import java.io.IOException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Builder for {@link IntervalQuery}
 */
public class IntervalQueryBuilder extends AbstractQueryBuilder<IntervalQueryBuilder> {

    public static final String NAME = "intervals";

    private final String field;
    private final IntervalsSourceProvider sourceProvider;

    public IntervalQueryBuilder(String field, IntervalsSourceProvider sourceProvider) {
        this.field = field;
        this.sourceProvider = sourceProvider;
    }

    public IntervalQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.field = in.readString();
        this.sourceProvider = in.readNamedWriteable(IntervalsSourceProvider.class);
    }

    public String getField() {
        return field;
    }

    public IntervalsSourceProvider getSourceProvider() {
        return sourceProvider;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(field);
        out.writeNamedWriteable(sourceProvider);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(field);
        builder.startObject();
        sourceProvider.toXContent(builder, params);
        printBoostAndQueryName(builder);
        builder.endObject();
        builder.endObject();
    }

    public static IntervalQueryBuilder fromXContent(XContentParser parser) throws IOException {
        if (parser.nextToken() != XContentParser.Token.FIELD_NAME) {
            throw new ParsingException(parser.getTokenLocation(), "Expected [FIELD_NAME] but got [" + parser.currentToken() + "]");
        }
        String field = parser.currentName();
        if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
            throw new ParsingException(parser.getTokenLocation(), "Expected [START_OBJECT] but got [" + parser.currentToken() + "]");
        }
        String name = null;
        float boost = 1;
        IntervalsSourceProvider provider = null;
        String providerName = null;
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            if (parser.currentToken() != XContentParser.Token.FIELD_NAME) {
                throw new ParsingException(parser.getTokenLocation(),
                    "Expected [FIELD_NAME] but got [" + parser.currentToken() + "]");
            }
            switch (parser.currentName()) {
                case "_name":
                    parser.nextToken();
                    name = parser.text();
                    break;
                case "boost":
                    parser.nextToken();
                    boost = parser.floatValue();
                    break;
                default:
                    if (providerName != null) {
                        throw new ParsingException(parser.getTokenLocation(),
                            "Only one interval rule can be specified, found [" + providerName + "] and [" + parser.currentName() + "]");
                    }
                    providerName = parser.currentName();
                    provider = IntervalsSourceProvider.fromXContent(parser);

            }
        }
        if (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            throw new ParsingException(parser.getTokenLocation(),
                "Expected [END_OBJECT] but got [" + parser.currentToken() + "]");
        }
        if (provider == null) {
            throw new ParsingException(parser.getTokenLocation(), "Missing intervals from interval query definition");
        }
        IntervalQueryBuilder builder = new IntervalQueryBuilder(field, provider);
        builder.queryName(name);
        builder.boost(boost);
        return builder;

    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        MappedFieldType fieldType = context.fieldMapper(field);
        if (fieldType == null) {
            // Be lenient with unmapped fields so that cross-index search will work nicely
            return new MatchNoDocsQuery();
        }
        Set<String> maskedFields = new HashSet<>();
        sourceProvider.extractFields(maskedFields);
        for (String maskedField : maskedFields) {
            MappedFieldType ft = context.fieldMapper(maskedField);
            if (ft == null) {
                // Be lenient with unmapped fields so that cross-index search will work nicely
                return new MatchNoDocsQuery();
            }
        }
        return new IntervalQuery(field, sourceProvider.getSource(context, fieldType));
    }

    @Override
    protected boolean doEquals(IntervalQueryBuilder other) {
        return Objects.equals(field, other.field) && Objects.equals(sourceProvider, other.sourceProvider);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(field, sourceProvider);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}

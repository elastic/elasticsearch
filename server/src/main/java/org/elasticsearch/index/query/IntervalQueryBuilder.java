/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.queries.intervals.IntervalQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

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
        boostAndQueryNameToXContent(builder);
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
                throw new ParsingException(parser.getTokenLocation(), "Expected [FIELD_NAME] but got [" + parser.currentToken() + "]");
            }
            switch (parser.currentName()) {
                case "_name" -> {
                    parser.nextToken();
                    name = parser.text();
                }
                case "boost" -> {
                    parser.nextToken();
                    boost = parser.floatValue();
                }
                default -> {
                    if (providerName != null) {
                        throw new ParsingException(
                            parser.getTokenLocation(),
                            "Only one interval rule can be specified, found [" + providerName + "] and [" + parser.currentName() + "]"
                        );
                    }
                    providerName = parser.currentName();
                    provider = IntervalsSourceProvider.fromXContent(parser);
                }
            }
        }
        if (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            throw new ParsingException(parser.getTokenLocation(), "Expected [END_OBJECT] but got [" + parser.currentToken() + "]");
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
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        MappedFieldType fieldType = context.getFieldType(field);
        if (fieldType == null) {
            // Be lenient with unmapped fields so that cross-index search will work nicely
            return new MatchNoDocsQuery();
        }
        Set<String> maskedFields = new HashSet<>();
        sourceProvider.extractFields(maskedFields);
        for (String maskedField : maskedFields) {
            MappedFieldType ft = context.getFieldType(maskedField);
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

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.ZERO;
    }
}

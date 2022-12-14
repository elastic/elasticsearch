/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

public class ExactQueryBuilder extends AbstractQueryBuilder<ExactQueryBuilder> {

    public static final String NAME = "exact";

    private final String field;
    private final Object value;

    public ExactQueryBuilder(String field, Object value) {
        this.field = field;
        this.value = value;
    }

    public ExactQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.field = in.readString();
        this.value = in.readGenericValue();
    }

    public static ExactQueryBuilder fromXContent(XContentParser parser) throws IOException {
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.nextToken(), parser);
        String field = parser.currentName();
        XContentParser.Token nextToken = parser.nextToken();
        ExactQueryBuilder query;
        if (nextToken == XContentParser.Token.START_OBJECT) {
            query = fromXContentComplex(field, parser);
        } else {
            query = fromXContentSimple(field, parser);
        }
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.nextToken(), parser);
        return query;
    }

    private static ExactQueryBuilder fromXContentSimple(String field, XContentParser parser) throws IOException {
        Object value = XContentParserUtils.parseFieldsValue(parser);
        return new ExactQueryBuilder(field, value);
    }

    private static ExactQueryBuilder fromXContentComplex(String field, XContentParser parser) throws IOException {
        Object value = null;
        String queryName = null;
        float boost = 1.0f;
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser);
            String fieldName = parser.currentName();
            parser.nextToken();
            switch (fieldName) {
                case "value" -> value = XContentParserUtils.parseFieldsValue(parser);
                case "boost" -> boost = parser.floatValue();
                case "_name" -> queryName = parser.text();
                default -> throw new XContentParseException(parser.getTokenLocation(), "Unknown field name [" + fieldName + "]");
            }
        }
        ExactQueryBuilder query = new ExactQueryBuilder(field, value);
        query.queryName(queryName);
        query.boost(boost);
        return query;
    }

    public String getField() {
        return field;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_8_7_0;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(this.field);
        out.writeGenericValue(this.value);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(getName());
        builder.startObject(this.field);
        builder.field("value", this.value);
        boostAndQueryNameToXContent(builder);
        builder.endObject();
        builder.endObject();
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        MappedFieldType fieldType = context.getFieldType(this.field);
        if (fieldType == null) {
            return new MatchNoDocsQuery();
        }
        return fieldType.exactQuery(this.value, context);
    }

    @Override
    protected boolean doEquals(ExactQueryBuilder other) {
        return Objects.equals(this.field, other.field) && Objects.equals(this.value, other.value);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(this.field, this.value);
    }
}

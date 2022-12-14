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
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.nextToken(), parser);
        String field = parser.currentName();
        parser.nextToken();
        Object value = XContentParserUtils.parseFieldsValue(parser);
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.nextToken(), parser);
        return new ExactQueryBuilder(field, value);
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
        super.writeTo(out);
        out.writeString(this.field);
        out.writeGenericValue(this.value);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(getName());
        builder.field(this.field, this.value);
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

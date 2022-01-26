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
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

public class TypeQueryBuilder extends AbstractQueryBuilder<TypeQueryBuilder> {
    public static final String NAME = "type";

    private static final ParseField VALUE_FIELD = new ParseField("value");
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(TypeQueryBuilder.class);
    static final String TYPES_DEPRECATION_MESSAGE = "[types removal] Type queries are deprecated, "
        + "prefer to filter on a field instead.";

    private final String type;

    public TypeQueryBuilder(String type) {
        if (type == null) {
            throw new IllegalArgumentException("[type] cannot be null");
        }
        this.type = type;
    }

    /**
     * Read from a stream.
     */
    public TypeQueryBuilder(StreamInput in) throws IOException {
        super(in);
        if (in.getVersion().onOrAfter(Version.V_6_3_0)) {
            type = in.readString();
        } else {
            type = in.readBytesRef().utf8ToString();
        }
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        if (out.getVersion().onOrAfter(Version.V_6_3_0)) {
            out.writeString(type);
        } else {
            out.writeBytesRef(new BytesRef(type));
        }
    }

    public String type() {
        return type;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(VALUE_FIELD.getPreferredName(), type);
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    public static TypeQueryBuilder fromXContent(XContentParser parser) throws IOException {
        String type = null;
        String queryName = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    queryName = parser.text();
                } else if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    boost = parser.floatValue();
                } else if (VALUE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    type = parser.text();
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "[" + TypeQueryBuilder.NAME + "] filter doesn't support [" + currentFieldName + "]"
                    );
                }
            } else {
                throw new ParsingException(
                    parser.getTokenLocation(),
                    "[" + TypeQueryBuilder.NAME + "] filter doesn't support [" + currentFieldName + "]"
                );
            }
        }

        if (type == null) {
            throw new ParsingException(
                parser.getTokenLocation(),
                "[" + TypeQueryBuilder.NAME + "] filter needs to be provided with a value for the type"
            );
        }
        return new TypeQueryBuilder(type).boost(boost).queryName(queryName);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        deprecationLogger.critical(DeprecationCategory.TYPES, "type_query", TYPES_DEPRECATION_MESSAGE);
        if (context.getType().equals(type)) {
            return Queries.newNonNestedFilter(context.indexVersionCreated());
        } else {
            // no type means no documents
            return new MatchNoDocsQuery();
        }
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(type);
    }

    @Override
    protected boolean doEquals(TypeQueryBuilder other) {
        return Objects.equals(type, other.type);
    }
}

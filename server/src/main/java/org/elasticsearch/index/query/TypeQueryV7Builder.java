/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.index.mapper.MapperService;

import java.io.IOException;

public class TypeQueryV7Builder extends AbstractQueryBuilder<TypeQueryV7Builder> {
    private static final DeprecationLogger deprecationLogger =  DeprecationLogger.getLogger(TypeQueryV7Builder.class);
    public static final String TYPES_DEPRECATION_MESSAGE = "[types removal] Type queries are deprecated, " +
        "prefer to filter on a field instead.";

    private static final String NAME = "type";
    public static final ParseField NAME_V7 = new ParseField(NAME).forRestApiVersion(RestApiVersion.equalTo(RestApiVersion.V_7));
    private static final ParseField VALUE_FIELD = new ParseField("value");
    private static final ObjectParser<TypeQueryV7Builder, Void> PARSER = new ObjectParser<>(NAME, TypeQueryV7Builder::new);

    static {
        PARSER.declareString(QueryBuilder::queryName,
            AbstractQueryBuilder.NAME_FIELD.forRestApiVersion(RestApiVersion.equalTo(RestApiVersion.V_7)));
        PARSER.declareFloat(QueryBuilder::boost,
            AbstractQueryBuilder.BOOST_FIELD.forRestApiVersion(RestApiVersion.equalTo(RestApiVersion.V_7)));
        PARSER.declareString(TypeQueryV7Builder::setValue,
            VALUE_FIELD.forRestApiVersion(RestApiVersion.equalTo(RestApiVersion.V_7)));
    }

    private String value;

    public TypeQueryV7Builder() {
    }

    /**
     * Read from a stream.
     */
    public TypeQueryV7Builder(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(VALUE_FIELD.getPreferredName(), MapperService.SINGLE_MAPPING_NAME);
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        return new MatchNoDocsQuery();
    }

    @Override
    protected boolean doEquals(TypeQueryV7Builder other) {
        return true;
    }

    @Override
    protected int doHashCode() {
        return 0;
    }

    public static TypeQueryV7Builder fromXContent(XContentParser parser) throws IOException {
        deprecationLogger.compatibleApiWarning("type_query", TYPES_DEPRECATION_MESSAGE);
        throw new ParsingException(parser.getTokenLocation(), TYPES_DEPRECATION_MESSAGE);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    public void setValue(String value){
        this.value = value;
    }
}

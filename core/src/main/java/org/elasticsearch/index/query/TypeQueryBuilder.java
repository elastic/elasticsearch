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
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.DocumentMapper;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

public class TypeQueryBuilder extends AbstractQueryBuilder<TypeQueryBuilder> {
    public static final String NAME = "type";

    private static final ParseField VALUE_FIELD = new ParseField("value");

    private final BytesRef type;

    public TypeQueryBuilder(String type) {
        if (type == null) {
            throw new IllegalArgumentException("[type] cannot be null");
        }
        this.type = BytesRefs.toBytesRef(type);
    }

    TypeQueryBuilder(BytesRef type) {
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
        type = in.readBytesRef();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeBytesRef(type);
    }

    public String type() {
        return BytesRefs.toString(this.type);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(VALUE_FIELD.getPreferredName(), type.utf8ToString());
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    public static Optional<TypeQueryBuilder> fromXContent(QueryParseContext parseContext) throws IOException {
        XContentParser parser = parseContext.parser();
        BytesRef type = null;

        String queryName = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (parseContext.getParseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.NAME_FIELD)) {
                    queryName = parser.text();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.BOOST_FIELD)) {
                    boost = parser.floatValue();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, VALUE_FIELD)) {
                    type = parser.utf8Bytes();
                } else {
                    throw new ParsingException(parser.getTokenLocation(),
                            "[" + TypeQueryBuilder.NAME + "] filter doesn't support [" + currentFieldName + "]");
                }
            } else {
                throw new ParsingException(parser.getTokenLocation(),
                        "[" + TypeQueryBuilder.NAME + "] filter doesn't support [" + currentFieldName + "]");
            }
        }

        if (type == null) {
            throw new ParsingException(parser.getTokenLocation(),
                    "[" + TypeQueryBuilder.NAME + "] filter needs to be provided with a value for the type");
        }
        return Optional.of(new TypeQueryBuilder(type)
                .boost(boost)
                .queryName(queryName));
    }


    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        //LUCENE 4 UPGRADE document mapper should use bytesref as well?
        DocumentMapper documentMapper = context.getMapperService().documentMapper(type.utf8ToString());
        if (documentMapper == null) {
            // no type means no documents
            return new MatchNoDocsQuery();
        } else {
            return documentMapper.typeFilter();
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
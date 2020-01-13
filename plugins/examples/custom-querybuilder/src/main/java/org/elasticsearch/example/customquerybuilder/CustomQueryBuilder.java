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

package org.elasticsearch.example.customquerybuilder;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.join.query.HasChildQueryBuilder;
import org.elasticsearch.percolator.PercolateQueryBuilder;

import java.io.IOException;
import java.util.Objects;

public class CustomQueryBuilder extends AbstractQueryBuilder<CustomQueryBuilder> {
    private static final ParseField PHRASE_FIELD = new ParseField("phrase");
    private static final String QUERY_FIELD_NAME = "query";
    private static final String CHILD_SCOPE = "child";
    private String phrase;

    public static final String NAME = "custom";

    public CustomQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.phrase = in.readString();
    }

    public CustomQueryBuilder(String phrase) {
        this.phrase = phrase;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(phrase);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(PHRASE_FIELD.getPreferredName(), phrase);
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        XContentBuilder docBuilder = XContentFactory.jsonBuilder().startObject();
        docBuilder.field(PHRASE_FIELD.getPreferredName(), phrase);
        docBuilder.endObject();

        PercolateQueryBuilder percolateQuery =
            new PercolateQueryBuilder(QUERY_FIELD_NAME, BytesReference.bytes(docBuilder), XContentType.JSON);
        HasChildQueryBuilder hasChildQueryBuilder = new HasChildQueryBuilder(CHILD_SCOPE, percolateQuery, ScoreMode.Max);
        return hasChildQueryBuilder.toQuery(context);
    }

    public static CustomQueryBuilder fromXContent(XContentParser parser) throws IOException, ParsingException {
        String queryName = null;
        String phrase = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                throw new ParsingException(parser.getTokenLocation(),
                    "[custom] query does not support object in [" + currentFieldName + "] field");
            } else if (token.isValue()) {
                if (currentFieldName == null) {
                    throw new IllegalStateException("Value came before field");
                }
                if (PHRASE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    phrase = parser.text();
                } else if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    boost = parser.floatValue();
                } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    queryName = parser.text();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[custom] query does not support [" + currentFieldName + "]");
                }
            }
        }

        CustomQueryBuilder queryBuilder = new CustomQueryBuilder(phrase);
        queryBuilder.boost(boost);
        if (queryName != null) {
            queryBuilder.queryName(queryName);
        }
        return queryBuilder;
    }

    @Override
    protected boolean doEquals(CustomQueryBuilder other) {
        return Objects.equals(phrase, other.phrase);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(phrase);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}

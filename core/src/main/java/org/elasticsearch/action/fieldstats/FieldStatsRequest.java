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

package org.elasticsearch.action.fieldstats;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 */
public class FieldStatsRequest extends BroadcastRequest<FieldStatsRequest> {

    public final static String DEFAULT_LEVEL = "cluster";

    private String[] fields = Strings.EMPTY_ARRAY;
    private String level = DEFAULT_LEVEL;
    private IndexConstraint[] indexConstraints = new IndexConstraint[0];

    public String[] getFields() {
        return fields;
    }

    public void setFields(String[] fields) {
        if (fields == null) {
            throw new NullPointerException("specified fields can't be null");
        }
        this.fields = fields;
    }

    public IndexConstraint[] getIndexConstraints() {
        return indexConstraints;
    }

    public void setIndexConstraints(IndexConstraint[] indexConstraints) {
        if (indexConstraints == null) {
            throw new NullPointerException("specified index_contraints can't be null");
        }
        this.indexConstraints = indexConstraints;
    }

    public void source(BytesReference content) throws IOException {
        List<IndexConstraint> indexConstraints = new ArrayList<>();
        List<String> fields = new ArrayList<>();
        try (XContentParser parser = XContentHelper.createParser(content)) {
            String fieldName = null;
            Token token = parser.nextToken();
            assert token == Token.START_OBJECT;
            for (token = parser.nextToken(); token != Token.END_OBJECT; token = parser.nextToken()) {
                switch (token) {
                    case FIELD_NAME:
                        fieldName = parser.currentName();
                        break;
                    case START_OBJECT:
                        if ("index_constraints".equals(fieldName)) {
                            parseIndexContraints(indexConstraints, parser);
                        } else {
                            throw new IllegalArgumentException("unknown field [" + fieldName + "]");
                        }
                        break;
                    case START_ARRAY:
                        if ("fields".equals(fieldName)) {
                            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                                if (token.isValue()) {
                                    fields.add(parser.text());
                                } else {
                                    throw new IllegalArgumentException("unexpected token [" + token + "]");
                                }
                            }
                        } else {
                            throw new IllegalArgumentException("unknown field [" + fieldName + "]");
                        }
                        break;
                    default:
                        throw new IllegalArgumentException("unexpected token [" + token + "]");
                }
            }
        }
        this.fields = fields.toArray(new String[fields.size()]);
        this.indexConstraints = indexConstraints.toArray(new IndexConstraint[indexConstraints.size()]);
    }

    private void parseIndexContraints(List<IndexConstraint> indexConstraints, XContentParser parser) throws IOException {
        Token token = parser.currentToken();
        assert token == Token.START_OBJECT;
        String field = null;
        String currentName = null;
        for (token = parser.nextToken(); token != Token.END_OBJECT; token = parser.nextToken()) {
            if (token == Token.FIELD_NAME) {
                field = currentName = parser.currentName();
            } else if (token == Token.START_OBJECT) {
                for (Token fieldToken = parser.nextToken(); fieldToken != Token.END_OBJECT; fieldToken = parser.nextToken()) {
                    if (fieldToken == Token.FIELD_NAME) {
                        currentName = parser.currentName();
                    } else if (fieldToken == Token.START_OBJECT) {
                        IndexConstraint.Property property = IndexConstraint.Property.parse(currentName);
                        String value = null;
                        String optionalFormat = null;
                        IndexConstraint.Comparison comparison = null;
                        for (Token propertyToken = parser.nextToken(); propertyToken != Token.END_OBJECT; propertyToken = parser.nextToken()) {
                            if (propertyToken.isValue()) {
                                if ("format".equals(parser.currentName())) {
                                    optionalFormat = parser.text();
                                } else {
                                    comparison = IndexConstraint.Comparison.parse(parser.currentName());
                                    value = parser.text();
                                }
                            } else {
                                if (propertyToken != Token.FIELD_NAME) {
                                    throw new IllegalArgumentException("unexpected token [" + propertyToken + "]");
                                }
                            }
                        }
                        indexConstraints.add(new IndexConstraint(field, property, comparison, value, optionalFormat));
                    } else {
                        throw new IllegalArgumentException("unexpected token [" + fieldToken + "]");
                    }
                }
            } else {
                throw new IllegalArgumentException("unexpected token [" + token + "]");
            }
        }
    }

    public String level() {
        return level;
    }

    public void level(String level) {
        this.level = level;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validate();
        if ("cluster".equals(level) == false && "indices".equals(level) == false) {
            validationException = ValidateActions.addValidationError("invalid level option [" + level + "]", validationException);
        }
        if (fields == null || fields.length == 0) {
            validationException = ValidateActions.addValidationError("no fields specified", validationException);
        }
        return validationException;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        fields = in.readStringArray();
        int size = in.readVInt();
        indexConstraints = new IndexConstraint[size];
        for (int i = 0; i < size; i++) {
            indexConstraints[i] = new IndexConstraint(in);
        }
        level = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArrayNullable(fields);
        out.writeVInt(indexConstraints.length);
        for (IndexConstraint indexConstraint : indexConstraints) {
            out.writeString(indexConstraint.getField());
            out.writeByte(indexConstraint.getProperty().getId());
            out.writeByte(indexConstraint.getComparison().getId());
            out.writeString(indexConstraint.getValue());
            if (out.getVersion().onOrAfter(Version.V_2_0_1)) {
                out.writeOptionalString(indexConstraint.getOptionalFormat());
            }
        }
        out.writeString(level);
    }

}

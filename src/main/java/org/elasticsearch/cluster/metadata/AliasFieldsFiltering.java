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

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 */
public class AliasFieldsFiltering implements Streamable, ToXContent {

    private String[] includes;

    public AliasFieldsFiltering(StreamInput in) throws IOException {
        readFrom(in);
    }

    public AliasFieldsFiltering(String[] includes) {
        this.includes = includes;
    }

    public String[] getIncludes() {
        return includes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AliasFieldsFiltering that = (AliasFieldsFiltering) o;
        return Arrays.equals(includes, that.includes);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(includes);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        includes = in.readStringArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringArrayNullable(includes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (includes.length != 0) {
            builder.startObject(Fields.FIELDS);
            builder.startArray(Fields.INCLUDES);
            for (String field : includes) {
                builder.value(field);
            }
            builder.endArray();
            builder.endObject();
        }
        return builder;
    }

    public static AliasFieldsFiltering fromXContext(XContentParser parser) throws IOException {
        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            throw new ElasticsearchParseException("could not read alias fields filtering from xcontent. expected an object but found [" + parser.currentToken() + "] instead");
        }
        String[] includes = Strings.EMPTY_ARRAY;
        String fieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("includes".equals(fieldName)) {
                    includes = readArray(parser);
                } else {
                    throw new ElasticsearchParseException("Unknown field [" + fieldName + "]");
                }
            } else {
                throw new ElasticsearchParseException("Unknown token [" + token + "]");
            }
        }
        return new AliasFieldsFiltering(includes);
    }

    private static String[] readArray(XContentParser parser) throws IOException {
        List<String> values = new ArrayList<>();
        while(parser.nextToken() != XContentParser.Token.END_ARRAY) {
            if (parser.currentToken() == XContentParser.Token.VALUE_STRING) {
                values.add(parser.text());
            } else {
                throw new ElasticsearchParseException("expected an array of strings for field [" + parser.currentName() + "] but found a [" + parser.currentToken() + "] token instead");
            }
        }
        return values.toArray(new String[values.size()]);
    }

    static final class Fields {

        static final XContentBuilderString FIELDS = new XContentBuilderString("fields");
        static final XContentBuilderString INCLUDES = new XContentBuilderString("includes");

    }
}

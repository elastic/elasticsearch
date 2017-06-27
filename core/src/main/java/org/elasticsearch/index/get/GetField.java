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

package org.elasticsearch.index.get;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.MapperService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.common.xcontent.XContentParserUtils.parseStoredFieldsValue;

public class GetField implements Streamable, ToXContent, Iterable<Object> {

    private String name;
    private List<Object> values;

    private GetField() {
    }

    public GetField(String name, List<Object> values) {
        this.name = Objects.requireNonNull(name, "name must not be null");
        this.values = Objects.requireNonNull(values, "values must not be null");
    }

    public String getName() {
        return name;
    }

    public Object getValue() {
        if (values != null && !values.isEmpty()) {
            return values.get(0);
        }
        return null;
    }

    public List<Object> getValues() {
        return values;
    }

    public boolean isMetadataField() {
        return MapperService.isMetadataField(name);
    }

    @Override
    public Iterator<Object> iterator() {
        return values.iterator();
    }

    public static GetField readGetField(StreamInput in) throws IOException {
        GetField result = new GetField();
        result.readFrom(in);
        return result;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        name = in.readString();
        int size = in.readVInt();
        values = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            values.add(in.readGenericValue());
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeVInt(values.size());
        for (Object obj : values) {
            out.writeGenericValue(obj);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(name);
        for (Object value : values) {
            //this call doesn't really need to support writing any kind of object.
            //Stored fields values are converted using MappedFieldType#valueForDisplay.
            //As a result they can either be Strings, Numbers, Booleans, or BytesReference, that's all.
            builder.value(value);
        }
        builder.endArray();
        return builder;
    }

    public static GetField fromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser::getTokenLocation);
        String fieldName = parser.currentName();
        XContentParser.Token token = parser.nextToken();
        ensureExpectedToken(XContentParser.Token.START_ARRAY, token, parser::getTokenLocation);
        List<Object> values = new ArrayList<>();
        while((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            values.add(parseStoredFieldsValue(parser));
        }
        return new GetField(fieldName, values);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GetField objects = (GetField) o;
        return Objects.equals(name, objects.name) &&
                Objects.equals(values, objects.values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, values);
    }

    @Override
    public String toString() {
        return "GetField{" +
                "name='" + name + '\'' +
                ", values=" + values +
                '}';
    }
}

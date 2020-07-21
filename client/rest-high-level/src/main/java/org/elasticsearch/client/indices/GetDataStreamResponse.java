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
package org.elasticsearch.client.indices;

import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;


public class GetDataStreamResponse {

    private final List<DataStream> dataStreams;

    GetDataStreamResponse(List<DataStream> dataStreams) {
        this.dataStreams = dataStreams;
    }

    public List<DataStream> getDataStreams() {
        return dataStreams;
    }

    public static GetDataStreamResponse fromXContent(XContentParser parser) throws IOException {
        final List<DataStream> templates = new ArrayList<>();
        for (XContentParser.Token token = parser.nextToken(); token != XContentParser.Token.END_OBJECT; token = parser.nextToken()) {
            if (token == XContentParser.Token.START_ARRAY) {
                while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                    templates.add(DataStream.fromXContent(parser));
                }
            }
        }
        return new GetDataStreamResponse(templates);
    }

    @Override
    public int hashCode() {
        return Objects.hash(new HashSet<>(this.dataStreams));
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        GetDataStreamResponse other = (GetDataStreamResponse) obj;
        return Objects.equals(new HashSet<>(this.dataStreams), new HashSet<>(other.dataStreams));
    }

    @Override
    public String toString() {
        List<DataStream> thisList = new ArrayList<>(this.dataStreams);
        thisList.sort(Comparator.comparing(DataStream::getName));
        return "GetDataStreamResponse [dataStreams=" + thisList + "]";
    }
}

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

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public final class DataStream {

    private final String name;
    private final String timeStampField;
    private final List<String> indices;
    private final long generation;

    public DataStream(String name, String timeStampField, List<String> indices, long generation) {
        this.name = name;
        this.timeStampField = timeStampField;
        this.indices = indices;
        this.generation = generation;
    }

    public String getName() {
        return name;
    }

    public String getTimeStampField() {
        return timeStampField;
    }

    public List<String> getIndices() {
        return indices;
    }

    public long getGeneration() {
        return generation;
    }

    public static final ParseField NAME_FIELD = new ParseField("name");
    public static final ParseField TIMESTAMP_FIELD_FIELD = new ParseField("timestamp_field");
    public static final ParseField INDICES_FIELD = new ParseField("indices");
    public static final ParseField GENERATION_FIELD = new ParseField("generation");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<DataStream, Void> PARSER = new ConstructingObjectParser<>("data_stream",
        args -> {
            String timeStampField = (String) ((Map<?, ?>) args[1]).get("name");
            List<String> indices =
                ((List<Map<String, String>>) args[2]).stream().map(m -> m.get("index_name")).collect(Collectors.toList());
            return new DataStream((String) args[0], timeStampField, indices, (Long) args[3]);
        });

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), NAME_FIELD);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> p.map(), TIMESTAMP_FIELD_FIELD);
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), (p, c) -> p.mapStrings(), INDICES_FIELD);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), GENERATION_FIELD);
    }

    public static DataStream fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataStream that = (DataStream) o;
        return name.equals(that.name) &&
            timeStampField.equals(that.timeStampField) &&
            indices.equals(that.indices) &&
            generation == that.generation;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, timeStampField, indices, generation);
    }
}

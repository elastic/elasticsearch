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

import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.Nullable;
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
    ClusterHealthStatus dataStreamStatus;
    @Nullable
    String indexTemplate;
    @Nullable
    String ilmPolicyName;

    public DataStream(String name, String timeStampField, List<String> indices, long generation, ClusterHealthStatus dataStreamStatus,
                      @Nullable String indexTemplate, @Nullable String ilmPolicyName) {
        this.name = name;
        this.timeStampField = timeStampField;
        this.indices = indices;
        this.generation = generation;
        this.dataStreamStatus = dataStreamStatus;
        this.indexTemplate = indexTemplate;
        this.ilmPolicyName = ilmPolicyName;
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

    public ClusterHealthStatus getDataStreamStatus() {
        return dataStreamStatus;
    }

    public String getIndexTemplate() {
        return indexTemplate;
    }

    public String getIlmPolicyName() {
        return ilmPolicyName;
    }

    public static final ParseField NAME_FIELD = new ParseField("name");
    public static final ParseField TIMESTAMP_FIELD_FIELD = new ParseField("timestamp_field");
    public static final ParseField INDICES_FIELD = new ParseField("indices");
    public static final ParseField GENERATION_FIELD = new ParseField("generation");
    public static final ParseField STATUS_FIELD = new ParseField("status");
    public static final ParseField INDEX_TEMPLATE_FIELD = new ParseField("template");
    public static final ParseField ILM_POLICY_FIELD = new ParseField("ilm_policy");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<DataStream, Void> PARSER = new ConstructingObjectParser<>("data_stream",
        args -> {
            String dataStreamName = (String) args[0];
            String timeStampField = (String) ((Map<?, ?>) args[1]).get("name");
            List<String> indices =
                ((List<Map<String, String>>) args[2]).stream().map(m -> m.get("index_name")).collect(Collectors.toList());
            Long generation = (Long) args[3];
            String statusStr = (String) args[4];
            ClusterHealthStatus status = ClusterHealthStatus.fromString(statusStr);
            String indexTemplate = (String) args[5];
            String ilmPolicy = (String) args[6];
            return new DataStream(dataStreamName, timeStampField, indices, generation, status, indexTemplate, ilmPolicy);
        });

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), NAME_FIELD);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> p.map(), TIMESTAMP_FIELD_FIELD);
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), (p, c) -> p.mapStrings(), INDICES_FIELD);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), GENERATION_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), STATUS_FIELD);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), INDEX_TEMPLATE_FIELD);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), ILM_POLICY_FIELD);
    }

    public static DataStream fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataStream that = (DataStream) o;
        return generation == that.generation &&
            name.equals(that.name) &&
            timeStampField.equals(that.timeStampField) &&
            indices.equals(that.indices) &&
            dataStreamStatus == that.dataStreamStatus &&
            Objects.equals(indexTemplate, that.indexTemplate) &&
            Objects.equals(ilmPolicyName, that.ilmPolicyName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, timeStampField, indices, generation, dataStreamStatus, indexTemplate, ilmPolicyName);
    }
}

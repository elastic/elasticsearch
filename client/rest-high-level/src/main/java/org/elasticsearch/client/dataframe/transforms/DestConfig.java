/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.dataframe.transforms;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Configuration containing the destination index for the {@link DataFrameTransformConfig}
 */
public class DestConfig implements ToXContentObject {

    public static final ParseField INDEX = new ParseField("index");
    public static final ParseField PIPELINE = new ParseField("pipeline");

    public static final ConstructingObjectParser<DestConfig, Void> PARSER = new ConstructingObjectParser<>("data_frame_config_dest",
        true,
        args -> new DestConfig((String)args[0], (String)args[1]));

    static {
        PARSER.declareString(constructorArg(), INDEX);
        PARSER.declareString(optionalConstructorArg(), PIPELINE);
    }

    private final String index;
    private final String pipeline;

    DestConfig(String index, String pipeline) {
        this.index = Objects.requireNonNull(index, INDEX.getPreferredName());
        this.pipeline = pipeline;
    }

    public String getIndex() {
        return index;
    }

    public String getPipeline() {
        return pipeline;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(INDEX.getPreferredName(), index);
        if (pipeline != null) {
            builder.field(PIPELINE.getPreferredName(), pipeline);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if (other == null || other.getClass() != getClass()) {
            return false;
        }

        DestConfig that = (DestConfig) other;
        return Objects.equals(index, that.index) &&
            Objects.equals(pipeline, that.pipeline);
    }

    @Override
    public int hashCode(){
        return Objects.hash(index, pipeline);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String index;
        private String pipeline;

        /**
         * Sets which index to which to write the data
         * @param index where to write the data
         * @return The {@link Builder} with index set
         */
        public Builder setIndex(String index) {
            this.index = Objects.requireNonNull(index, INDEX.getPreferredName());
            return this;
        }

        /**
         * Sets the pipeline through which the indexed documents should be processed
         * @param pipeline The pipeline ID
         * @return The {@link Builder} with pipeline set
         */
        public Builder setPipeline(String pipeline) {
            this.pipeline = pipeline;
            return this;
        }

        public DestConfig build() {
            return new DestConfig(index, pipeline);
        }
    }
}

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
package org.elasticsearch.protocol.xpack.rollup;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.protocol.xpack.rollup.job.RollupJobConfig;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class PutRollupJobRequest extends AcknowledgedRequest<PutRollupJobRequest> implements IndicesRequest, ToXContentObject {

    private RollupJobConfig config;
    private IndicesOptions indicesOptions = IndicesOptions.fromOptions(false, false, true, false);

    public PutRollupJobRequest(final RollupJobConfig config) {
        this.config = config;
    }

    public PutRollupJobRequest() {
    }

    /**
     * @return the configuration of the rollup job to create
     */
    public RollupJobConfig getConfig() {
        return config;
    }

    /**
     * Sets the configuration of the rollup job to create
     * @param config the {@link RollupJobConfig}
     */
    public void setConfig(final RollupJobConfig config) {
        this.config = config;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        this.config = new RollupJobConfig(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        this.config.writeTo(out);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public ActionRequestValidationException validateMappings(final Map<String, Map<String, FieldCapabilities>> fieldCapsResponse) {
        final ActionRequestValidationException validationException = new ActionRequestValidationException();
        if (fieldCapsResponse.size() == 0) {
            validationException.addValidationError("Could not find any fields in the index/index-pattern that were configured in job");
            return validationException;
        }
        config.validateMappings(fieldCapsResponse, validationException);
        if (validationException.validationErrors().size() > 0) {
            return validationException;
        }
        return null;
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        return config.toXContent(builder, params);
    }

    @Override
    public String[] indices() {
        return new String[]{this.config.getIndexPattern()};
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    @Override
    public int hashCode() {
        return Objects.hash(config, indicesOptions, timeout);
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final PutRollupJobRequest other = (PutRollupJobRequest) obj;
        return Objects.equals(config, other.config);
    }

    public static PutRollupJobRequest fromXContent(final XContentParser parser, final String id) throws IOException {
        return new PutRollupJobRequest(RollupJobConfig.fromXContent(parser, id));
    }
}

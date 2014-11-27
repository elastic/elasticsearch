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

package org.elasticsearch.index.query.functionscore;

import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.MultiValueMode;

import java.io.IOException;
import java.util.Locale;

public abstract class DecayFunctionBuilder extends ScoreFunctionBuilder {

    protected static final String ORIGIN = "origin";
    protected static final String SCALE = "scale";
    protected static final String DECAY = "decay";
    protected static final String OFFSET = "offset";

    private String fieldName;
    private Object origin;
    private Object scale;
    private double decay = -1;
    private Object offset;
    private MultiValueMode multiValueMode = null;

    public DecayFunctionBuilder(String fieldName, Object origin, Object scale) {
        this.fieldName = fieldName;
        this.origin = origin;
        this.scale = scale;
    }

    public DecayFunctionBuilder setDecay(double decay) {
        if (decay <= 0 || decay >= 1.0) {
            throw new ElasticsearchIllegalStateException("scale weight parameter must be in range 0..1!");
        }
        this.decay = decay;
        return this;
    }

    public DecayFunctionBuilder setOffset(Object offset) {
        this.offset = offset;
        return this;
    }

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(getName());
        builder.startObject(fieldName);
        if (origin != null) {
            builder.field(ORIGIN, origin);
        }
        builder.field(SCALE, scale);
        if (decay > 0) {
            builder.field(DECAY, decay);
        }
        if (offset != null) {
            builder.field(OFFSET, offset);
        }
        builder.endObject();
        if (multiValueMode != null) {
            builder.field(DecayFunctionParser.MULTI_VALUE_MODE.getPreferredName(), multiValueMode.name());
        }
        builder.endObject();
    }

    public ScoreFunctionBuilder setMultiValueMode(MultiValueMode multiValueMode) {
        this.multiValueMode = multiValueMode;
        return this;
    }

    public ScoreFunctionBuilder setMultiValueMode(String multiValueMode) {
        this.multiValueMode = MultiValueMode.fromString(multiValueMode.toUpperCase(Locale.ROOT));
        return this;
    }
}
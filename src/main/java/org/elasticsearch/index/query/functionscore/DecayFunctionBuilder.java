/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public abstract class DecayFunctionBuilder implements ScoreFunctionBuilder {

    protected static final String REFERNECE = "reference";
    protected static final String SCALE = "scale";
    protected static final String SCALE_WEIGHT = "scale_weight";

    private String fieldName;
    private Object reference;
    private Object scale;
    private double scaleWeight = -1;

    public DecayFunctionBuilder(String fieldName, Object reference, Object scale) {
        this.fieldName = fieldName;
        this.reference = reference;
        this.scale = scale;
    }
    public DecayFunctionBuilder setScaleWeight(double scaleWeight) {
        if(scaleWeight <=0 || scaleWeight >= 1.0) {
            throw new ElasticSearchIllegalStateException("scale weight parameter must be in range 0..1!");
        }
        this.scaleWeight = scaleWeight;
        return this;
    }
   
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(getName());
        builder.startObject(fieldName);
        builder.field(REFERNECE, reference);
        builder.field(SCALE, scale);
        if (scaleWeight > 0) {
            builder.field(SCALE_WEIGHT, scaleWeight);
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }

}
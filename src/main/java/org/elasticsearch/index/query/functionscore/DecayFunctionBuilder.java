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


import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public abstract class DecayFunctionBuilder implements ScoreFunctionBuilder {

    protected static final String REFERNECE = "reference";
    protected static final String SCALE = "scale";
    protected static final String SCALE_WEIGHT = "scale_weight";
    protected static final String SCALE_DEFAULT = "0.5";
    
    private String fieldName;
    private String reference;
    private String scale;
    private String scaleWeight;

    public void setParameters(String fieldName, String reference, String scale, String scaleWeight) {
        if(this.fieldName != null ) {
            throw new ElasticSearchException("Parameters in distance function were already set!");
        }
        this.fieldName = fieldName;
        this.reference = reference;
        this.scale = scale;
        this.scaleWeight = scaleWeight;
    }

    public void setParameters(String fieldName, String reference, String scale) {
        setParameters(fieldName, reference, scale, SCALE_DEFAULT);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(getName());
            builder.startObject(fieldName);
            builder.field(REFERNECE, reference);
            builder.field(SCALE, scale);
            builder.field(SCALE_WEIGHT, scaleWeight);
            builder.endObject();
        builder.endObject();
        return builder;
    }

    public void addGeoParams(String fieldName, double lat, double lon, String scale) {
        addGeoParams(fieldName, lat, lon, scale, SCALE_DEFAULT);
    }

    public void addGeoParams(String fieldName, double lat, double lon, String scale, String scaleWeight) {
        String geoLoc = Double.toString(lat) + ", " + Double.toString(lon);
        setParameters(fieldName, geoLoc, scale, scaleWeight);
    }
}
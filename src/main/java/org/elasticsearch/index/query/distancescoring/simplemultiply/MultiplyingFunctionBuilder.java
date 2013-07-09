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

package org.elasticsearch.index.query.distancescoring.simplemultiply;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.distancescoring.ScoreFunctionBuilder;

public abstract class MultiplyingFunctionBuilder implements ScoreFunctionBuilder {

    List<Var> vars = null;

    public void addVariable(String fieldName, String scale, String reference) {
        if (vars == null) {
            vars = new ArrayList<Var>();
        }
        vars.add(new Var(fieldName, reference, scale));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        for (Var var : vars) {
            builder.field(var.fieldName);
            builder.startObject();
            builder.field("reference", var.reference);
            builder.field("scale", var.scale);
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    class Var {
        String fieldName;
        String reference;
        String scale;

        public Var(String fieldName, String reference, String scale) {
            this.fieldName = fieldName;
            this.reference = reference;
            this.scale = scale;
        }
    }

    public void addGeoVariable(String fieldName, double lat, double lon, String scale) {

        String geoLoc = Double.toString(lat) + ", " + Double.toString(lon);
        addVariable(fieldName, scale, geoLoc);

    }
}
/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.index.query.json;

import org.elasticsearch.util.json.JsonBuilder;

import java.io.IOException;

/**
 * @author kimchy (Shay Banon)
 */
public class RangeJsonQueryBuilder extends BaseJsonQueryBuilder {

    private final String name;

    private Object from;

    private Object to;

    private boolean includeLower = true;

    private boolean includeUpper = true;

    private float boost = -1;

    public RangeJsonQueryBuilder(String name) {
        this.name = name;
    }

    public RangeJsonQueryBuilder from(String from) {
        this.from = from;
        return this;
    }

    public RangeJsonQueryBuilder from(int from) {
        this.from = from;
        return this;
    }

    public RangeJsonQueryBuilder from(long from) {
        this.from = from;
        return this;
    }

    public RangeJsonQueryBuilder from(float from) {
        this.from = from;
        return this;
    }

    public RangeJsonQueryBuilder from(double from) {
        this.from = from;
        return this;
    }

    public RangeJsonQueryBuilder to(String to) {
        this.to = to;
        return this;
    }

    public RangeJsonQueryBuilder to(int to) {
        this.to = to;
        return this;
    }

    public RangeJsonQueryBuilder to(long to) {
        this.to = to;
        return this;
    }

    public RangeJsonQueryBuilder to(float to) {
        this.to = to;
        return this;
    }

    public RangeJsonQueryBuilder to(double to) {
        this.to = to;
        return this;
    }

    public RangeJsonQueryBuilder includeLower(boolean includeLower) {
        this.includeLower = includeLower;
        return this;
    }

    public RangeJsonQueryBuilder includeUpper(boolean includeUpper) {
        this.includeUpper = includeUpper;
        return this;
    }

    public RangeJsonQueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    @Override protected void doJson(JsonBuilder builder) throws IOException {
        builder.startObject(RangeJsonQueryParser.NAME);
        builder.startObject(name);
        builder.field("from", from);
        builder.field("to", to);
        builder.field("includeLower", includeLower);
        builder.field("includeUpper", includeUpper);
        if (boost != -1) {
            builder.field("boost", boost);
        }
        builder.endObject();
        builder.endObject();
    }
}

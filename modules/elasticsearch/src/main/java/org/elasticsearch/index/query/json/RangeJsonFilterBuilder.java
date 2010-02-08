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
public class RangeJsonFilterBuilder extends BaseJsonFilterBuilder {

    private final String name;

    private Object from;

    private Object to;

    private boolean includeLower = true;

    private boolean includeUpper = true;

    public RangeJsonFilterBuilder(String name) {
        this.name = name;
    }

    public RangeJsonFilterBuilder from(String from) {
        this.from = from;
        return this;
    }

    public RangeJsonFilterBuilder from(int from) {
        this.from = from;
        return this;
    }

    public RangeJsonFilterBuilder from(long from) {
        this.from = from;
        return this;
    }

    public RangeJsonFilterBuilder from(float from) {
        this.from = from;
        return this;
    }

    public RangeJsonFilterBuilder from(double from) {
        this.from = from;
        return this;
    }

    public RangeJsonFilterBuilder to(String to) {
        this.to = to;
        return this;
    }

    public RangeJsonFilterBuilder to(int to) {
        this.to = to;
        return this;
    }

    public RangeJsonFilterBuilder to(long to) {
        this.to = to;
        return this;
    }

    public RangeJsonFilterBuilder to(float to) {
        this.to = to;
        return this;
    }

    public RangeJsonFilterBuilder to(double to) {
        this.to = to;
        return this;
    }

    public RangeJsonFilterBuilder includeLower(boolean includeLower) {
        this.includeLower = includeLower;
        return this;
    }

    public RangeJsonFilterBuilder includeUpper(boolean includeUpper) {
        this.includeUpper = includeUpper;
        return this;
    }

    @Override protected void doJson(JsonBuilder builder) throws IOException {
        builder.startObject(RangeJsonFilterParser.NAME);
        builder.startObject(name);
        builder.field("from", from);
        builder.field("to", to);
        builder.field("includeLower", includeLower);
        builder.field("includeUpper", includeUpper);
        builder.endObject();
        builder.endObject();
    }
}
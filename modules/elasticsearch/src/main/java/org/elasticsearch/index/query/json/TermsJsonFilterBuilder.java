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
public class TermsJsonFilterBuilder extends BaseJsonFilterBuilder {

    private final String name;

    private final Object[] values;

    public TermsJsonFilterBuilder(String name, String... values) {
        this(name, (Object[]) values);
    }

    public TermsJsonFilterBuilder(String name, int... values) {
        this.name = name;
        this.values = new Integer[values.length];
        for (int i = 0; i < values.length; i++) {
            this.values[i] = values[i];
        }
    }

    public TermsJsonFilterBuilder(String name, long... values) {
        this.name = name;
        this.values = new Long[values.length];
        for (int i = 0; i < values.length; i++) {
            this.values[i] = values[i];
        }
    }

    public TermsJsonFilterBuilder(String name, float... values) {
        this.name = name;
        this.values = new Float[values.length];
        for (int i = 0; i < values.length; i++) {
            this.values[i] = values[i];
        }
    }

    public TermsJsonFilterBuilder(String name, double... values) {
        this.name = name;
        this.values = new Double[values.length];
        for (int i = 0; i < values.length; i++) {
            this.values[i] = values[i];
        }
    }

    private TermsJsonFilterBuilder(String name, Object... values) {
        this.name = name;
        this.values = values;
    }

    @Override public void doJson(JsonBuilder builder) throws IOException {
        builder.startObject(TermsJsonFilterParser.NAME);
        builder.startArray(name);
        for (Object value : values) {
            builder.value(value);
        }
        builder.endArray();
        builder.endObject();
    }
}
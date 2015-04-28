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

package org.elasticsearch.index.query.functionscore.fieldvaluefactor;

import org.elasticsearch.common.lucene.search.function.FieldValueFactorFunction;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilder;

import java.io.IOException;
import java.util.Locale;

/**
 * Builder to construct {@code field_value_factor} functions for a function
 * score query.
 */
public class FieldValueFactorFunctionBuilder extends ScoreFunctionBuilder {
    private String field = null;
    private Float factor = null;
    private Double missing = null;
    private FieldValueFactorFunction.Modifier modifier = null;

    public FieldValueFactorFunctionBuilder(String fieldName) {
        this.field = fieldName;
    }

    @Override
    public String getName() {
        return FieldValueFactorFunctionParser.NAMES[0];
    }

    public FieldValueFactorFunctionBuilder factor(float boostFactor) {
        this.factor = boostFactor;
        return this;
    }

    /**
     * Value used instead of the field value for documents that don't have that field defined.
     */
    public FieldValueFactorFunctionBuilder missing(double missing) {
        this.missing = missing;
        return this;
    }

    public FieldValueFactorFunctionBuilder modifier(FieldValueFactorFunction.Modifier modifier) {
        this.modifier = modifier;
        return this;
    }

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(getName());
        if (field != null) {
            builder.field("field", field);
        }

        if (factor != null) {
            builder.field("factor", factor);
        }

        if (missing != null) {
            builder.field("missing", missing);
        }

        if (modifier != null) {
            builder.field("modifier", modifier.toString().toLowerCase(Locale.ROOT));
        }
        builder.endObject();
    }
}

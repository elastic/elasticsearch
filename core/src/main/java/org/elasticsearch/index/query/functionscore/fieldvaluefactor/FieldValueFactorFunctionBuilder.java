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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.function.FieldValueFactorFunction;
import org.elasticsearch.common.lucene.search.function.ScoreFunction;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilder;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

/**
 * Builder to construct {@code field_value_factor} functions for a function
 * score query.
 */
public class FieldValueFactorFunctionBuilder extends ScoreFunctionBuilder<FieldValueFactorFunctionBuilder> {

    public static final FieldValueFactorFunction.Modifier DEFAULT_MODIFIER = FieldValueFactorFunction.Modifier.NONE;
    public static final float DEFAULT_FACTOR = 1;

    private final String field;
    private float factor = DEFAULT_FACTOR;
    private Double missing;
    private FieldValueFactorFunction.Modifier modifier = DEFAULT_MODIFIER;

    public FieldValueFactorFunctionBuilder(String fieldName) {
        if (fieldName == null) {
            throw new IllegalArgumentException("field_value_factor: field must not be null");
        }
        this.field = fieldName;
    }

    @Override
    public String getName() {
        return FieldValueFactorFunctionParser.NAMES[0];
    }

    public String fieldName() {
        return this.field;
    }

    public FieldValueFactorFunctionBuilder factor(float boostFactor) {
        this.factor = boostFactor;
        return this;
    }

    public float factor() {
        return this.factor;
    }

    /**
     * Value used instead of the field value for documents that don't have that field defined.
     */
    public FieldValueFactorFunctionBuilder missing(double missing) {
        this.missing = missing;
        return this;
    }

    public Double missing() {
        return this.missing;
    }

    public FieldValueFactorFunctionBuilder modifier(FieldValueFactorFunction.Modifier modifier) {
        if (modifier == null) {
            throw new IllegalArgumentException("field_value_factor: modifier must not be null");
        }
        this.modifier = modifier;
        return this;
    }

    public FieldValueFactorFunction.Modifier modifier() {
        return this.modifier;
    }

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(getName());
        builder.field("field", field);
        builder.field("factor", factor);
        if (missing != null) {
            builder.field("missing", missing);
        }
        builder.field("modifier", modifier.name().toLowerCase(Locale.ROOT));
        builder.endObject();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(field);
        out.writeFloat(factor);
        if (missing == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeDouble(missing);
        }
        modifier.writeTo(out);
    }

    @Override
    protected FieldValueFactorFunctionBuilder doReadFrom(StreamInput in) throws IOException {
        FieldValueFactorFunctionBuilder functionBuilder = new FieldValueFactorFunctionBuilder(in.readString());
        functionBuilder.factor = in.readFloat();
        if (in.readBoolean()) {
            functionBuilder.missing = in.readDouble();
        }
        functionBuilder.modifier = FieldValueFactorFunction.Modifier.readModifierFrom(in);
        return functionBuilder;
    }

    @Override
    protected boolean doEquals(FieldValueFactorFunctionBuilder functionBuilder) {
        return Objects.equals(this.field, functionBuilder.field) &&
                Objects.equals(this.factor, functionBuilder.factor) &&
                Objects.equals(this.missing, functionBuilder.missing) &&
                Objects.equals(this.modifier, functionBuilder.modifier);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(this.field, this.factor, this.missing, this.modifier);
    }

    @Override
    protected ScoreFunction doToFunction(QueryShardContext context) {
        MappedFieldType fieldType = context.getMapperService().fullName(field);
        IndexNumericFieldData fieldData = null;
        if (fieldType == null) {
            if(missing == null) {
                throw new ElasticsearchException("Unable to find a field mapper for field [" + field + "]. No 'missing' value defined.");
            }
        } else {
            fieldData = context.getForField(fieldType);
        }
        return new FieldValueFactorFunction(field, factor, modifier, missing, fieldData);
    }
}

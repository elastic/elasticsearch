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
import org.elasticsearch.index.query.QueryValidationException;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilder;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

/**
 * Builder to construct {@code field_value_factor} functions for a function
 * score query.
 */
public class FieldValueFactorFunctionBuilder extends ScoreFunctionBuilder<FieldValueFactorFunctionBuilder> {
    private final String field;
    private Float factor = null;
    private Double missing = null;
    private FieldValueFactorFunction.Modifier modifier = FieldValueFactorFunction.Modifier.NONE;

    static final FieldValueFactorFunctionBuilder PROTOTYPE = new FieldValueFactorFunctionBuilder(null);

    public FieldValueFactorFunctionBuilder(String fieldName) {
        this.field = fieldName;
    }

    @Override
    public String getWriteableName() {
        return FieldValueFactorFunctionParser.NAMES[0];
    }

    public FieldValueFactorFunctionBuilder factor(float boostFactor) {
        this.factor = boostFactor;
        return this;
    }

    /**
     * Value used instead of the field value for documents that don't have that field defined.
     */
    public FieldValueFactorFunctionBuilder missing(Double missing) {
        this.missing = missing;
        return this;
    }

    public FieldValueFactorFunctionBuilder modifier(FieldValueFactorFunction.Modifier modifier) {
        this.modifier = (modifier == null) ? FieldValueFactorFunction.Modifier.NONE : modifier;
        return this;
    }

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(getWriteableName());
        builder.field("field", field);
        if (factor != null) {
            builder.field("factor", factor);
        }
        if (missing != null) {
            builder.field("missing", missing);
        }
        builder.field("modifier", modifier.toString().toLowerCase(Locale.ROOT));
        builder.endObject();
    }

    @Override
    protected ScoreFunction doScoreFunction(QueryShardContext context) throws IOException {
        MappedFieldType fieldType = context.mapperService().smartNameFieldType(field);
        if (fieldType == null) {
            throw new ElasticsearchException("Unable to find a field mapper for field [" + field + "]");
        }
        return new FieldValueFactorFunction(field, factor, modifier, missing,
                (IndexNumericFieldData)context.fieldData().getForField(fieldType));
    }

    @Override
    public QueryValidationException validate() {
        QueryValidationException validationException = null;
        if (field == null) {
            validationException = addValidationError("field must not be null", validationException);
        }
        return validationException;
    }

    @Override
    protected FieldValueFactorFunctionBuilder doReadFrom(StreamInput in) throws IOException {
        FieldValueFactorFunctionBuilder fieldValueFactorBuilder = new FieldValueFactorFunctionBuilder(in.readString());
        fieldValueFactorBuilder.factor = in.readOptionalFloat();
        fieldValueFactorBuilder.missing = in.readOptionalDouble();
        fieldValueFactorBuilder.modifier = FieldValueFactorFunction.Modifier.fromString(in.readOptionalString());
        return fieldValueFactorBuilder;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(field);
        out.writeOptionalFloat(factor);
        out.writeOptionalDouble(missing);
        out.writeOptionalString(modifier.toString());
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(field, factor, missing, modifier);
    }

    @Override
    protected boolean doEquals(FieldValueFactorFunctionBuilder other) {
        return Objects.equals(field, other.field) &&
                Objects.equals(factor, other.factor) &&
                Objects.equals(missing, other.missing) &&
                Objects.equals(modifier, other.modifier);
    }
}

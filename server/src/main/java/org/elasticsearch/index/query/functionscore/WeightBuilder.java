/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query.functionscore;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.function.ScoreFunction;
import org.elasticsearch.common.lucene.search.function.WeightFactorFunction;
import org.elasticsearch.common.lucene.search.function.WeightFieldFactorFunction;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * A query that multiplies the weight to the score.
 */
public class WeightBuilder extends ScoreFunctionBuilder<WeightBuilder> {
    public static final String NAME = "weight";

    private String field;
    private Double missing;
    private Float weight;
    private ScoreFunction scoreFunction;

    /**
     * Standard constructor.
     */
    public WeightBuilder() {
        super();
    }

    public WeightBuilder(String fieldName) {
        if (fieldName == null) {
            throw new IllegalArgumentException("weight_field: field must not be null");
        }
        this.field = fieldName;
    }

    /**
     * Read from a stream.
     */
    public WeightBuilder(StreamInput in) throws IOException {
        super(in);
        field = in.readOptionalString();
        missing = in.readOptionalDouble();
        weight = in.readOptionalFloat();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeOptionalString(field);
        out.writeOptionalDouble(missing);
        out.writeOptionalFloat(weight);
    }

    @Override
    public String getName() {
        return NAME;
    }

    public WeightBuilder setMissing(double missing) {
        this.missing = missing;
        return this;
    }

    public void setScoreFunction(ScoreFunction scoreFunction) {
        this.scoreFunction = scoreFunction;
    }

    public WeightBuilder setWeight(float weight) {
        this.weight = checkWeight(weight);
        return this;
    }

    private Float checkWeight(Float weight) {
        if (weight != null && Float.compare(weight, 0) < 0) {
            throw new IllegalArgumentException("[weight] cannot be negative for a filtering function");
        }
        return weight;
    }

    public Float getWeight() {
        return weight;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        if (weight != null) {
            builder.field("weight");
            builder.value(weight);
        } else {
            builder.startObject(getName());
            builder.field("field", field);
            if (missing != null) {
                builder.field("missing", missing);
            }
            builder.endObject();
        }
    }

    @Override
    protected boolean doEquals(WeightBuilder functionBuilder) {
        if (functionBuilder == null) {
            return false;
        }

        return Objects.equals(this.field, functionBuilder.field)
            && Objects.equals(this.missing, functionBuilder.missing)
            && Objects.equals(this.weight, functionBuilder.weight);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(
            this.field == null ? 0 : this.field,
            this.missing == null ? 0 : this.missing,
            this.weight == null ? 0 : this.weight);
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_EMPTY;
    }

    @Override
    protected ScoreFunction doToFunction(SearchExecutionContext context) throws IOException {
        if (weight != null) {
            return new WeightFactorFunction(weight, scoreFunction);
        }

        IndexNumericFieldData fieldData = null;
        if (context.isFieldMapped(field)) {
            fieldData = context.getForField(context.getFieldType(field), MappedFieldType.FielddataOperation.SEARCH);
        } else {
            if (missing == null) {
                throw new ElasticsearchException("Unable to find a field mapper for field [" + field + "]. No 'missing' value defined.");
            }
        }

        return new WeightFieldFactorFunction(field, missing, fieldData, scoreFunction);
    }

    public static WeightBuilder fromXContent(XContentParser parser) throws IOException, ParsingException {
        String currentFieldName = null;
        String field = null;
        Double missing = null;
        Float weight = null;
        XContentParser.Token token = parser.currentToken();

        if (token.isValue()) {
            weight = parser.floatValue();
            return new WeightBuilder().setWeight(weight);
        }

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if ("field".equals(currentFieldName)) {
                    field = parser.text();
                } else if ("missing".equals(currentFieldName)) {
                    missing = parser.doubleValue();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), NAME + " query does not support [" + currentFieldName + "]");
                }
            }
        }

        if (field == null) {
            throw new ParsingException(parser.getTokenLocation(), "[" + NAME + "] required field 'field' missing");
        }

        WeightBuilder weightFieldBuilder = new WeightBuilder(field);
        if (missing != null) {
            weightFieldBuilder.setMissing(missing);
        }
        return weightFieldBuilder;
    }
}

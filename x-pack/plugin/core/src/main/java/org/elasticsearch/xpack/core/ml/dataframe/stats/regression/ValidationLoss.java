/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.dataframe.stats.regression;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.dataframe.stats.common.FoldValues;
import org.elasticsearch.xpack.core.ml.utils.ToXContentParams;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class ValidationLoss implements ToXContentObject, Writeable {

    public static final ParseField LOSS_TYPE = new ParseField("loss_type");
    public static final ParseField FOLD_VALUES = new ParseField("fold_values");

    public static ValidationLoss fromXContent(XContentParser parser, boolean ignoreUnknownFields) {
        return createParser(ignoreUnknownFields).apply(parser, null);
    }

    @SuppressWarnings("unchecked")
    private static ConstructingObjectParser<ValidationLoss, Void> createParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<ValidationLoss, Void> parser = new ConstructingObjectParser<>("regression_validation_loss",
                ignoreUnknownFields,
            a -> new ValidationLoss((String) a[0], (List<FoldValues>) a[1]));

        parser.declareString(ConstructingObjectParser.constructorArg(), LOSS_TYPE);
        parser.declareObjectArray(ConstructingObjectParser.constructorArg(),
            (p, c) -> FoldValues.fromXContent(p, ignoreUnknownFields), FOLD_VALUES);
        return parser;
    }

    private final String lossType;
    private final List<FoldValues> foldValues;

    public ValidationLoss(String lossType, List<FoldValues> values) {
        this.lossType = Objects.requireNonNull(lossType);
        this.foldValues = Objects.requireNonNull(values);
    }

    public ValidationLoss(StreamInput in) throws IOException {
        lossType = in.readString();
        foldValues = in.readList(FoldValues::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(lossType);
        out.writeList(foldValues);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(LOSS_TYPE.getPreferredName(), lossType);
        if (params.paramAsBoolean(ToXContentParams.FOR_INTERNAL_STORAGE, false)) {
            builder.field(FOLD_VALUES.getPreferredName(), foldValues);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ValidationLoss that = (ValidationLoss) o;
        return Objects.equals(lossType, that.lossType) && Objects.equals(foldValues, that.foldValues);
    }

    @Override
    public int hashCode() {
        return Objects.hash(lossType, foldValues);
    }
}

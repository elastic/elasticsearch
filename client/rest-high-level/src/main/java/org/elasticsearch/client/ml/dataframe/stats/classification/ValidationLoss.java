/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.dataframe.stats.classification;

import org.elasticsearch.client.ml.dataframe.stats.common.FoldValues;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class ValidationLoss implements ToXContentObject {

    public static final ParseField LOSS_TYPE = new ParseField("loss_type");
    public static final ParseField FOLD_VALUES = new ParseField("fold_values");

    @SuppressWarnings("unchecked")
    public static ConstructingObjectParser<ValidationLoss, Void> PARSER = new ConstructingObjectParser<>("classification_validation_loss",
        true,
        a -> new ValidationLoss((String) a[0], (List<FoldValues>) a[1]));

    static {
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), LOSS_TYPE);
        PARSER.declareObjectArray(ConstructingObjectParser.optionalConstructorArg(), FoldValues.PARSER, FOLD_VALUES);
    }

    private final String lossType;
    private final List<FoldValues> foldValues;

    public ValidationLoss(String lossType, List<FoldValues> values) {
        this.lossType = lossType;
        this.foldValues = values;
    }

    public String getLossType() {
        return lossType;
    }

    public List<FoldValues> getFoldValues() {
        return foldValues;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (lossType != null) {
            builder.field(LOSS_TYPE.getPreferredName(), lossType);
        }
        if (foldValues != null) {
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

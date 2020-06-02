/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.ml.dataframe.stats.regression;

import org.elasticsearch.client.ml.dataframe.stats.common.FoldValues;
import org.elasticsearch.common.ParseField;
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
    public static ConstructingObjectParser<ValidationLoss, Void> PARSER = new ConstructingObjectParser<>("regression_validation_loss",
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

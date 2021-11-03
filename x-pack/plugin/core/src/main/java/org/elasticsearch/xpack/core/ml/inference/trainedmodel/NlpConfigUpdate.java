/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.MapHelper;
import org.elasticsearch.xpack.core.ml.utils.NamedXContentObject;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.NlpConfig.RESULTS_FIELD;

public abstract class NlpConfigUpdate implements InferenceConfigUpdate, NamedXContentObject {

    @SuppressWarnings("unchecked")
    public static TokenizationUpdate tokenizationFromMap(Map<String, Object> map) {
        Map<String, Object> tokenziation = (Map<String, Object>) map.remove("tokenization");
        if (tokenziation == null) {
            return null;
        }

        Map<String, Object> bert = (Map<String, Object>) map.remove("bert");
        if (bert == null) {
            throw ExceptionsHelper.badRequestException("not bert");
        }
        Object truncate = bert.remove("truncate");
        return new BertTokenizationUpdate((Tokenization.Truncate) truncate);
    }

    protected final TokenizationUpdate tokenizationUpdate;


    public NlpConfigUpdate(@Nullable TokenizationUpdate tokenizationUpdate) {
        this.tokenizationUpdate = tokenizationUpdate;
    }

    public NlpConfigUpdate(StreamInput in) throws IOException {
        tokenizationUpdate = in.readOptionalNamedWriteable(TokenizationUpdate.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalNamedWriteable(tokenizationUpdate);
    }

    protected boolean isNoop() {
        return tokenizationUpdate == null || tokenizationUpdate.isNoop();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        if (tokenizationUpdate != null) {
            builder.field(NlpConfig.TOKENIZATION.getPreferredName(), tokenizationUpdate);
        }
        doXContentBody(builder, params);
        builder.endObject();
        return builder;
    }

    public abstract XContentBuilder doXContentBody(XContentBuilder builder, ToXContent.Params params) throws IOException;
}

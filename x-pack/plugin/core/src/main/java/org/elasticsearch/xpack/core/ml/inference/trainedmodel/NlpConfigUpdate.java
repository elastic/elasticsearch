/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.NamedXContentObject;
import org.elasticsearch.xpack.core.ml.utils.NamedXContentObjectHelper;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public abstract class NlpConfigUpdate implements InferenceConfigUpdate, NamedXContentObject {

    @SuppressWarnings("unchecked")
    public static TokenizationUpdate tokenizationFromMap(Map<String, Object> map) {
        Map<String, Object> tokenization = (Map<String, Object>) map.remove("tokenization");
        if (tokenization == null) {
            return null;
        }

        Map<String, BiFunction<Tokenization.Truncate, Integer, TokenizationUpdate>> knownTokenizers = Map.of(
            BertTokenization.NAME.getPreferredName(),
            BertTokenizationUpdate::new,
            MPNetTokenization.NAME.getPreferredName(),
            MPNetTokenizationUpdate::new,
            RobertaTokenizationUpdate.NAME.getPreferredName(),
            RobertaTokenizationUpdate::new
        );

        Map<String, Object> tokenizationConfig = null;
        BiFunction<Tokenization.Truncate, Integer, TokenizationUpdate> updater = null;
        for (var tokenizerType : knownTokenizers.keySet()) {
            tokenizationConfig = (Map<String, Object>) tokenization.remove(tokenizerType);
            if (tokenizationConfig != null) {
                updater = knownTokenizers.get(tokenizerType);
                break;
            }
        }

        if (tokenizationConfig == null && tokenization.isEmpty() == false) {
            throw ExceptionsHelper.badRequestException(
                "unknown tokenization type expecting one of {} got {}",
                knownTokenizers.keySet().stream().sorted().collect(Collectors.toList()),
                tokenization.keySet()
            );
        }
        if (tokenizationConfig == null) {
            return null;
        }
        Tokenization.Truncate truncate = Optional.ofNullable(tokenizationConfig.remove("truncate"))
            .map(t -> Tokenization.Truncate.fromString(t.toString()))
            .orElse(null);
        Integer span = (Integer) Optional.ofNullable(tokenizationConfig.remove("span")).orElse(null);
        if (truncate == null && span == null) {
            return null;
        }
        return updater.apply(truncate, span);
    }

    protected final TokenizationUpdate tokenizationUpdate;

    public NlpConfigUpdate(@Nullable TokenizationUpdate tokenizationUpdate) {
        this.tokenizationUpdate = tokenizationUpdate;
    }

    public NlpConfigUpdate(StreamInput in) throws IOException {
        if (in.getVersion().onOrAfter(Version.V_8_1_0)) {
            tokenizationUpdate = in.readOptionalNamedWriteable(TokenizationUpdate.class);
        } else {
            tokenizationUpdate = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().onOrAfter(Version.V_8_1_0)) {
            out.writeOptionalNamedWriteable(tokenizationUpdate);
        }
    }

    protected boolean isNoop() {
        return tokenizationUpdate == null || tokenizationUpdate.isNoop();
    }

    public TokenizationUpdate getTokenizationUpdate() {
        return tokenizationUpdate;
    }

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        if (tokenizationUpdate != null) {
            NamedXContentObjectHelper.writeNamedObject(builder, params, NlpConfig.TOKENIZATION.getPreferredName(), tokenizationUpdate);
        }
        doXContentBody(builder, params);
        builder.endObject();
        return builder;
    }

    public abstract XContentBuilder doXContentBody(XContentBuilder builder, ToXContent.Params params) throws IOException;

    /**
     * Required because this class implements 2 interfaces defining the
     * method {@code String getName()} and the compiler insists it must
     * be resolved here in the abstract class
     */
    @Override
    public String getName() {
        return InferenceConfigUpdate.super.getName();
    }
}

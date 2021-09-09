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
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.ml.inference.persistence.InferenceIndexConstants;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.NamedXContentObjectHelper;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

public class PassThroughConfig implements NlpConfig {

    public static final String NAME = "pass_through";

    public static PassThroughConfig fromXContentStrict(XContentParser parser) {
        return STRICT_PARSER.apply(parser, null);
    }

    public static PassThroughConfig fromXContentLenient(XContentParser parser) {
        return LENIENT_PARSER.apply(parser, null);
    }

    private static final ConstructingObjectParser<PassThroughConfig, Void> STRICT_PARSER = createParser(false);
    private static final ConstructingObjectParser<PassThroughConfig, Void> LENIENT_PARSER = createParser(true);

    private static ConstructingObjectParser<PassThroughConfig, Void> createParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<PassThroughConfig, Void> parser = new ConstructingObjectParser<>(NAME, ignoreUnknownFields,
            a -> new PassThroughConfig((VocabularyConfig) a[0], (Tokenization) a[1]));
        parser.declareObject(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> {
                if (ignoreUnknownFields == false) {
                    throw ExceptionsHelper.badRequestException(
                        "illegal setting [{}] on inference model creation",
                        VOCABULARY.getPreferredName()
                    );
                }
                return VocabularyConfig.fromXContentLenient(p);
            },
            VOCABULARY
        );
        parser.declareNamedObject(
            ConstructingObjectParser.optionalConstructorArg(), (p, c, n) -> p.namedObject(Tokenization.class, n, ignoreUnknownFields),
            TOKENIZATION
        );
        return parser;
    }

    private final VocabularyConfig vocabularyConfig;
    private final Tokenization tokenization;

    public PassThroughConfig(@Nullable VocabularyConfig vocabularyConfig, @Nullable Tokenization tokenization) {
        this.vocabularyConfig = Optional.ofNullable(vocabularyConfig)
            .orElse(new VocabularyConfig(InferenceIndexConstants.nativeDefinitionStore()));
        this.tokenization = tokenization == null ? Tokenization.createDefault() : tokenization;
    }

    public PassThroughConfig(StreamInput in) throws IOException {
        vocabularyConfig = new VocabularyConfig(in);
        tokenization = in.readNamedWriteable(Tokenization.class);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(VOCABULARY.getPreferredName(), vocabularyConfig, params);
        NamedXContentObjectHelper.writeNamedObject(builder, params, TOKENIZATION.getPreferredName(), tokenization);
        builder.endObject();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        vocabularyConfig.writeTo(out);
        out.writeNamedWriteable(tokenization);
    }

    @Override
    public boolean isTargetTypeSupported(TargetType targetType) {
        return false;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_8_0_0;
    }

    @Override
    public boolean isAllocateOnly() {
        return true;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PassThroughConfig that = (PassThroughConfig) o;
        return Objects.equals(vocabularyConfig, that.vocabularyConfig)
            && Objects.equals(tokenization, that.tokenization);
    }

    @Override
    public int hashCode() {
        return Objects.hash(vocabularyConfig, tokenization);
    }

    @Override
    public VocabularyConfig getVocabularyConfig() {
        return vocabularyConfig;
    }

    @Override
    public Tokenization getTokenization() {
        return tokenization;
    }
}

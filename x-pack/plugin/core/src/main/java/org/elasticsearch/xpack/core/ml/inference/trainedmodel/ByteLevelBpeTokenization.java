/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.action.PutTrainedModelVocabularyAction;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

/**
 * Tokenization settings for byte-level BPE models (for example GPT-2 style vocabularies) that ship
 * a merge table alongside the vocabulary. The in-process tokenizer applies UTF-8 byte mapping and
 * BPE merges using the same engine as the RoBERTa tokenizer, but special token strings are
 * configurable so deployments can align with Hugging Face tokenizer metadata.
 * {@code do_lower_case} is not applied by the byte-level BPE analyzer and must not be set to {@code true};
 * requests that set it to {@code true} are rejected like {@link RobertaTokenization}.
 * When {@code with_special_tokens} is {@code true}, the configured BOS and EOS strings must be present
 * in the vocabulary; that requirement is enforced when the inference tokenizer is constructed for the model (not only in
 * {@link #validateVocabulary(org.elasticsearch.xpack.core.ml.action.PutTrainedModelVocabularyAction.Request)}), consistent with
 * {@link RobertaTokenization}.
 * The configured mask string is optional for encoding-only inference, and the mask token id may be unset if that token
 * is missing from the vocabulary (masking-oriented tasks should ensure it exists).
 */
public class ByteLevelBpeTokenization extends Tokenization {

    /** Name used in inference configuration XContent and named writeables. */
    public static final String NAME = "byte_level_bpe";

    /**
     * {@link Tokenization} wire format for byte-level BPE; do not send to nodes that do not support this version.
     */
    public static final TransportVersion ML_BYTE_LEVEL_BPE_TOKENIZATION_ADDED = TransportVersion.fromName("ml_byte_level_bpe_tokenization");

    private static final ParseField ADD_PREFIX_SPACE = new ParseField("add_prefix_space");
    private static final ParseField UNK_TOKEN = new ParseField("unk_token");
    private static final ParseField PAD_TOKEN = new ParseField("pad_token");
    private static final ParseField BOS_TOKEN = new ParseField("bos_token");
    private static final ParseField EOS_TOKEN = new ParseField("eos_token");
    private static final ParseField MASK_TOKEN = new ParseField("mask_token");

    private static final boolean DEFAULT_ADD_PREFIX_SPACE = false;
    private static final String DEFAULT_UNK_TOKEN = "<unk>";
    private static final String DEFAULT_PAD_TOKEN = "<pad>";
    private static final String DEFAULT_BOS_TOKEN = "<s>";
    private static final String DEFAULT_EOS_TOKEN = "</s>";
    private static final String DEFAULT_MASK_TOKEN = "<mask>";

    public static ConstructingObjectParser<ByteLevelBpeTokenization, Void> createParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<ByteLevelBpeTokenization, Void> parser = new ConstructingObjectParser<>(
            "byte_level_bpe_tokenization",
            ignoreUnknownFields,
            a -> new ByteLevelBpeTokenization(
                (Boolean) a[0],
                (Boolean) a[1],
                (Integer) a[2],
                a[3] == null ? null : Truncate.fromString((String) a[3]),
                (Integer) a[4],
                (Boolean) a[5],
                (String) a[6],
                (String) a[7],
                (String) a[8],
                (String) a[9],
                (String) a[10]
            )
        );
        declareCommonFields(parser);
        parser.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), ADD_PREFIX_SPACE);
        parser.declareString(ConstructingObjectParser.optionalConstructorArg(), UNK_TOKEN);
        parser.declareString(ConstructingObjectParser.optionalConstructorArg(), PAD_TOKEN);
        parser.declareString(ConstructingObjectParser.optionalConstructorArg(), BOS_TOKEN);
        parser.declareString(ConstructingObjectParser.optionalConstructorArg(), EOS_TOKEN);
        parser.declareString(ConstructingObjectParser.optionalConstructorArg(), MASK_TOKEN);
        return parser;
    }

    private static final ConstructingObjectParser<ByteLevelBpeTokenization, Void> LENIENT_PARSER = createParser(true);
    private static final ConstructingObjectParser<ByteLevelBpeTokenization, Void> STRICT_PARSER = createParser(false);

    public static ByteLevelBpeTokenization fromXContent(XContentParser parser, boolean lenient) {
        return lenient ? LENIENT_PARSER.apply(parser, null) : STRICT_PARSER.apply(parser, null);
    }

    private final boolean addPrefixSpace;
    private final String unkToken;
    private final String padToken;
    private final String bosToken;
    private final String eosToken;
    private final String maskToken;

    public ByteLevelBpeTokenization(
        @Nullable Boolean doLowerCase,
        @Nullable Boolean withSpecialTokens,
        @Nullable Integer maxSequenceLength,
        @Nullable Truncate truncate,
        @Nullable Integer span,
        @Nullable Boolean addPrefixSpace,
        @Nullable String unkToken,
        @Nullable String padToken,
        @Nullable String bosToken,
        @Nullable String eosToken,
        @Nullable String maskToken
    ) {
        this(
            withSpecialTokens,
            maxSequenceLength,
            truncate,
            span,
            addPrefixSpace,
            resolveSpecialToken(unkToken, DEFAULT_UNK_TOKEN),
            resolveSpecialToken(padToken, DEFAULT_PAD_TOKEN),
            resolveSpecialToken(bosToken, DEFAULT_BOS_TOKEN),
            resolveSpecialToken(eosToken, DEFAULT_EOS_TOKEN),
            resolveSpecialToken(maskToken, DEFAULT_MASK_TOKEN)
        );
        if (Boolean.TRUE.equals(doLowerCase)) {
            throw new IllegalArgumentException("unable to set [do_lower_case] to [true] for byte_level_bpe tokenizer");
        }
    }

    private ByteLevelBpeTokenization(
        @Nullable Boolean withSpecialTokens,
        @Nullable Integer maxSequenceLength,
        @Nullable Truncate truncate,
        @Nullable Integer span,
        @Nullable Boolean addPrefixSpace,
        String unkToken,
        String padToken,
        String bosToken,
        String eosToken,
        String maskToken
    ) {
        super(false, withSpecialTokens, maxSequenceLength, truncate, span);
        this.addPrefixSpace = Optional.ofNullable(addPrefixSpace).orElse(DEFAULT_ADD_PREFIX_SPACE);
        this.unkToken = unkToken;
        this.padToken = padToken;
        this.bosToken = bosToken;
        this.eosToken = eosToken;
        this.maskToken = maskToken;
    }

    public ByteLevelBpeTokenization(StreamInput in) throws IOException {
        super(in);
        if (doLowerCase) {
            throw new IllegalArgumentException("unable to set [do_lower_case] to [true] for byte_level_bpe tokenizer");
        }
        this.addPrefixSpace = in.readBoolean();
        this.unkToken = in.readString();
        this.padToken = in.readString();
        this.bosToken = in.readString();
        this.eosToken = in.readString();
        this.maskToken = in.readString();
    }

    private static String resolveSpecialToken(@Nullable String value, String defaultValue) {
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return value;
    }

    public boolean isAddPrefixSpace() {
        return addPrefixSpace;
    }

    public String getUnkToken() {
        return unkToken;
    }

    public String getPadToken() {
        return padToken;
    }

    public String getBosToken() {
        return bosToken;
    }

    public String getEosToken() {
        return eosToken;
    }

    @Override
    Tokenization buildWindowingTokenization(int updatedMaxSeqLength, int updatedSpan) {
        return new ByteLevelBpeTokenization(
            false,
            withSpecialTokens,
            updatedMaxSeqLength,
            Truncate.NONE,
            updatedSpan,
            addPrefixSpace,
            unkToken,
            padToken,
            bosToken,
            eosToken,
            maskToken
        );
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().supports(ML_BYTE_LEVEL_BPE_TOKENIZATION_ADDED) == false) {
            throw new ElasticsearchStatusException(
                "Cannot send byte_level_bpe tokenization to an older node. "
                    + "Please wait until all nodes are upgraded before using byte_level_bpe tokenization",
                RestStatus.BAD_REQUEST
            );
        }
        super.writeTo(out);
        out.writeBoolean(addPrefixSpace);
        out.writeString(unkToken);
        out.writeString(padToken);
        out.writeString(bosToken);
        out.writeString(eosToken);
        out.writeString(maskToken);
    }

    @Override
    public String getMaskToken() {
        return maskToken;
    }

    @Override
    XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(ADD_PREFIX_SPACE.getPreferredName(), addPrefixSpace);
        builder.field(UNK_TOKEN.getPreferredName(), unkToken);
        builder.field(PAD_TOKEN.getPreferredName(), padToken);
        builder.field(BOS_TOKEN.getPreferredName(), bosToken);
        builder.field(EOS_TOKEN.getPreferredName(), eosToken);
        builder.field(MASK_TOKEN.getPreferredName(), maskToken);
        return builder;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public void validateVocabulary(PutTrainedModelVocabularyAction.Request request) {
        if (request.getMerges().isEmpty()) {
            throw new ElasticsearchStatusException(
                "cannot put vocabulary for model [{}] as tokenizer type [{}] requires [{}] to be provided and non-empty",
                RestStatus.BAD_REQUEST,
                request.getModelId(),
                getName(),
                PutTrainedModelVocabularyAction.Request.MERGES.getPreferredName()
            );
        }
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        ByteLevelBpeTokenization that = (ByteLevelBpeTokenization) o;
        return addPrefixSpace == that.addPrefixSpace
            && Objects.equals(unkToken, that.unkToken)
            && Objects.equals(padToken, that.padToken)
            && Objects.equals(bosToken, that.bosToken)
            && Objects.equals(eosToken, that.eosToken)
            && Objects.equals(maskToken, that.maskToken);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), addPrefixSpace, unkToken, padToken, bosToken, eosToken, maskToken);
    }
}

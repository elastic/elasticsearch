/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.xcontent.AbstractObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.inference.common.parser.StatefulValue;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.common.parser.StatefulValue.applyUpdate;
import static org.elasticsearch.xpack.inference.common.parser.StringParser.validateStringIsNotNullOrEmpty;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockConstants.ACCESS_KEY_FIELD;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockConstants.MODEL_FIELD;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockConstants.PROVIDER_FIELD;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockConstants.REGION_FIELD;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockConstants.SECRET_KEY_FIELD;

public abstract class AmazonBedrockServiceSettings extends FilteredXContentObject implements ServiceSettings {

    protected static final String AMAZON_BEDROCK_BASE_NAME = "amazon_bedrock";

    /**
     * Registers the common Bedrock service-settings fields (model, region, provider, rate_limit) onto the given parser.
     */
    public static <B extends AmazonBedrockServiceSettings.Builder<? extends AmazonBedrockServiceSettings>> void declareCommonFields(
        AbstractObjectParser<B, ConfigurationParseContext> parser
    ) {
        parser.declareString(AmazonBedrockServiceSettings.Builder::setRegion, new ParseField(REGION_FIELD));
        parser.declareString(AmazonBedrockServiceSettings.Builder::setModel, new ParseField(MODEL_FIELD));
        parser.declareString(AmazonBedrockServiceSettings.Builder::setProvider, new ParseField(PROVIDER_FIELD));
        parser.declareObject(
            AmazonBedrockServiceSettings.Builder::setRateLimitSettings,
            // An explicitly empty rate_limit object ({}) resolves to the default rate limit rather than null, so the setter is never
            // invoked with null.
            (p, c) -> RateLimitSettings.createParser(c == ConfigurationParseContext.PERSISTENT, DEFAULT_RATE_LIMIT_SETTINGS).apply(p, null),
            new ParseField(RateLimitSettings.FIELD_NAME)
        );
        // access_key/secret_key appear in the same JSON block as service settings in REST requests; DefaultSecretSettings extracts them
        // separately. Declare it here as a no-op so the strict REQUEST parser does not reject it as an unknown field.
        parser.declareString((b, v) -> {}, new ParseField(ACCESS_KEY_FIELD));
        parser.declareString((b, v) -> {}, new ParseField(SECRET_KEY_FIELD));
    }

    protected final String region;
    protected final String model;
    protected final AmazonBedrockProvider provider;
    protected final RateLimitSettings rateLimitSettings;

    // the default requests per minute are defined as per-model in the "Runtime quotas" on AWS
    // see: https://docs.aws.amazon.com/bedrock/latest/userguide/quotas.html
    // setting this to 240 requests per minute (4 requests / sec) is a sane default for us as it should be enough for
    // decent throughput without exceeding the minimal for _most_ items. The user should consult
    // the table above if using a model that might have a lesser limit (e.g. Anthropic Claude 3.5)
    public static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(240);

    /**
     * Accumulates the parsed common fields and assembles a {@link AmazonBedrockServiceSettings}, enforcing that the required fields are
     * present. Task-specific builders extend this and contribute their own fields.
     *
     * @param <T> the task-specific settings type produced by {@link #build(String, String, AmazonBedrockProvider, RateLimitSettings,
     * ConfigurationParseContext)}
     */
    public abstract static class Builder<T extends AmazonBedrockServiceSettings> {
        private String region;
        private String model;
        private String provider;
        private RateLimitSettings rateLimitSettings;

        public void setRegion(String region) {
            this.region = region;
        }

        public void setModel(String model) {
            this.model = model;
        }

        public void setProvider(String provider) {
            this.provider = provider;
        }

        public void setRateLimitSettings(RateLimitSettings rateLimitSettings) {
            this.rateLimitSettings = rateLimitSettings;
        }

        protected abstract T build(
            String region,
            String model,
            AmazonBedrockProvider provider,
            RateLimitSettings rateLimitSettings,
            ConfigurationParseContext context
        );

        public final T build(ConfigurationParseContext context) {
            validateStringIsNotNullOrEmpty(region, REGION_FIELD);
            validateStringIsNotNullOrEmpty(model, MODEL_FIELD);
            validateStringIsNotNullOrEmpty(provider, PROVIDER_FIELD);
            return build(region, model, AmazonBedrockProvider.fromString(provider), rateLimitSettings, context);
        }
    }

    /**
     * Creates a {@link AmazonBedrockServiceSettings} from a map of settings using the given parser.
     *
     * @param map     the map to parse
     * @param context the context in which the parsing is done
     * @param parser  the parser to use for parsing the settings
     * @return the created {@link AmazonBedrockServiceSettings}
     */
    public static <T extends AmazonBedrockServiceSettings> T fromMap(
        Map<String, Object> map,
        ConfigurationParseContext context,
        ObjectParser<? extends AmazonBedrockServiceSettings.Builder<T>, ConfigurationParseContext> parser
    ) {
        try (var xParser = XContentHelper.mapToXContentParser(XContentParserConfiguration.EMPTY, map)) {
            return parser.apply(xParser, context).build(context);
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse [{}]", e, ModelConfigurations.SERVICE_SETTINGS);
        }
    }

    protected AmazonBedrockCommonSettings updateCommonSettings(
        Map<String, Object> serviceSettings,
        ValidationException validationException
    ) {
        var extractedRateLimitSettings = RateLimitSettings.of(
            serviceSettings,
            this.rateLimitSettings,
            validationException,
            ConfigurationParseContext.REQUEST
        );

        return new AmazonBedrockCommonSettings(this.region, this.model, this.provider, extractedRateLimitSettings);
    }

    protected record AmazonBedrockCommonSettings(
        String region,
        String model,
        AmazonBedrockProvider provider,
        @Nullable RateLimitSettings rateLimitSettings
    ) {}

    protected AmazonBedrockServiceSettings(StreamInput in) throws IOException {
        this.region = in.readString();
        this.model = in.readString();
        this.provider = in.readEnum(AmazonBedrockProvider.class);
        this.rateLimitSettings = new RateLimitSettings(in);
    }

    protected AmazonBedrockServiceSettings(
        String region,
        String model,
        AmazonBedrockProvider provider,
        @Nullable RateLimitSettings rateLimitSettings
    ) {
        this.region = Objects.requireNonNull(region);
        this.model = Objects.requireNonNull(model);
        this.provider = Objects.requireNonNull(provider);
        this.rateLimitSettings = Objects.requireNonNullElse(rateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.minimumCompatible();
    }

    public String region() {
        return region;
    }

    @Override
    public String modelId() {
        return model;
    }

    public AmazonBedrockProvider provider() {
        return provider;
    }

    public RateLimitSettings rateLimitSettings() {
        return rateLimitSettings;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(region);
        out.writeString(model);
        out.writeEnum(provider);
        rateLimitSettings.writeTo(out);
    }

    public void addBaseXContent(XContentBuilder builder, Params params) throws IOException {
        toXContentFragmentOfExposedFields(builder, params);
    }

    protected void addXContentFragmentOfExposedFields(XContentBuilder builder, Params params) throws IOException {
        builder.field(REGION_FIELD, region);
        builder.field(MODEL_FIELD, model);
        builder.field(PROVIDER_FIELD, provider.name());
        rateLimitSettings.toXContent(builder, params);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        AmazonBedrockServiceSettings that = (AmazonBedrockServiceSettings) o;
        return Objects.equals(region, that.region)
            && Objects.equals(model, that.model)
            && Objects.equals(provider, that.provider)
            && Objects.equals(rateLimitSettings, that.rateLimitSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(region, model, provider, rateLimitSettings);
    }

    /**
     * Registers the common Bedrock fields that may be changed by an update request. Only {@code rate_limit} is mutable; the
     * immutable fields (such as {@code model}, {@code region} and {@code provider}) are intentionally not declared so that a strict update
     * parser rejects attempts to change them.
     */
    public static void declareCommonUpdatableFields(AbstractObjectParser<? extends CommonUpdate, Void> parser) {
        StatefulValue.declareNullable(
            parser,
            (update, value) -> update.rateLimitSettings = value,
            (p) -> RateLimitSettings.createParser(false, null).apply(p, null),
            new ParseField(RateLimitSettings.FIELD_NAME),
            ObjectParser.ValueType.OBJECT_OR_NULL
        );
    }

    /**
     * Common fields parsed from an update request. Because settings are immutable, each subclass builds the new instance itself,
     * calling {@link #mergedRateLimitSettings(AmazonBedrockServiceSettings)} to resolve the shared fields.
     */
    public static class CommonUpdate {

        protected StatefulValue<RateLimitSettings> rateLimitSettings = StatefulValue.undefined();

        /**
         * Resolves the rate limit settings to use after applying the update following the tri-state convention: an omitted field keeps
         * the current value, an explicit null resets the field to the default rate limit, and a present value replaces the current one.
         */
        protected RateLimitSettings mergedRateLimitSettings(AmazonBedrockServiceSettings existing) {
            return applyUpdate(rateLimitSettings, existing.rateLimitSettings(), DEFAULT_RATE_LIMIT_SETTINGS);
        }
    }
}

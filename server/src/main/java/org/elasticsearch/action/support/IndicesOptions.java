/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.support;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParser.Token;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeStringArrayValue;

/**
 * Contains all the multi-target syntax options. These options are split into groups depending on what aspect of the syntax they
 * influence.
 *
 * @param concreteTargetOptions, applies only to concrete targets and defines how the response will handle when a concrete
 *                               target does not exist.
 * @param wildcardOptions, applies only to wildcard expressions and defines how the wildcards will be expanded and if it will
 *                        be acceptable to have expressions that results to no indices.
 * @param generalOptions, applies to all the resolved indices and defines if throttled will be included and if certain type of
 *                        aliases or indices are allowed, or they will throw an error.
 */
public record IndicesOptions(ConcreteTargetOptions concreteTargetOptions, WildcardOptions wildcardOptions, GeneralOptions generalOptions)
    implements
        ToXContentFragment {

    public static IndicesOptions.Builder builder() {
        return new Builder();
    }

    public static IndicesOptions.Builder builder(IndicesOptions indicesOptions) {
        return new Builder(indicesOptions);
    }

    /**
     * Controls the way the target indices will be handled.
     * @param allowUnavailableTargets, if false when any of the concrete targets requested does not exist, throw an error
     */
    public record ConcreteTargetOptions(boolean allowUnavailableTargets) implements ToXContentFragment {
        public static final String IGNORE_UNAVAILABLE = "ignore_unavailable";
        public static final ConcreteTargetOptions ALLOW_UNAVAILABLE_TARGETS = new ConcreteTargetOptions(true);
        public static final ConcreteTargetOptions ERROR_WHEN_UNAVAILABLE_TARGETS = new ConcreteTargetOptions(false);

        public static ConcreteTargetOptions fromParameter(Object ignoreUnavailableString, ConcreteTargetOptions defaultOption) {
            if (ignoreUnavailableString == null && defaultOption != null) {
                return defaultOption;
            }
            return nodeBooleanValue(ignoreUnavailableString, IGNORE_UNAVAILABLE)
                ? ALLOW_UNAVAILABLE_TARGETS
                : ERROR_WHEN_UNAVAILABLE_TARGETS;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.field(IGNORE_UNAVAILABLE, allowUnavailableTargets);
        }
    }

    /**
     * Controls the way the wildcard expressions will be resolved.
     * @param matchOpen, open indices will be matched
     * @param matchClosed, closed indices will be matched
     * @param includeHidden, hidden indices will be included in the result. This is a post filter, it requires matchOpen or matchClosed
     *                      to have an effect.
     * @param resolveAliases, aliases will be included in the result, if false we treat them like they do not exist
     * @param allowEmptyExpressions, when an expression does not result in any indices, if false it throws an error if true it treats it as
     *                               an empty result
     */
    public record WildcardOptions(
        boolean matchOpen,
        boolean matchClosed,
        boolean includeHidden,
        boolean resolveAliases,
        boolean allowEmptyExpressions
    ) implements ToXContentFragment {

        public static final String EXPAND_WILDCARDS = "expand_wildcards";
        public static final String ALLOW_NO_INDICES = "allow_no_indices";

        public static final WildcardOptions DEFAULT = new WildcardOptions(true, false, false, true, true);

        public static WildcardOptions parseParameters(Object expandWildcards, Object allowNoIndices, WildcardOptions defaultOptions) {
            if (expandWildcards == null && allowNoIndices == null) {
                return defaultOptions;
            }
            WildcardOptions.Builder builder = defaultOptions == null ? new Builder() : new Builder(defaultOptions);
            if (expandWildcards != null) {
                builder.matchNone();
                builder.expandStates(nodeStringArrayValue(expandWildcards));
            }

            if (allowNoIndices != null) {
                builder.allowEmptyExpressions(nodeBooleanValue(allowNoIndices, ALLOW_NO_INDICES));
            }
            return builder.build();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return toXContent(builder, false);
        }

        /**
         * This converter to XContent only includes the fields a user can interact, internal options like the resolveAliases
         * are not added.
         * @param wildcardStatesAsUserInput, some parts of the code expect the serialization of the expand_wildcards field
         *                                   to be a comma separated string that matches the allowed user input, this includes
         *                                   all the states along with the values 'all' and 'none'.
         */
        public XContentBuilder toXContent(XContentBuilder builder, boolean wildcardStatesAsUserInput) throws IOException {
            EnumSet<WildcardStates> legacyStates = EnumSet.noneOf(WildcardStates.class);
            if (matchOpen()) {
                legacyStates.add(WildcardStates.OPEN);
            }
            if (matchClosed()) {
                legacyStates.add(WildcardStates.CLOSED);
            }
            if (includeHidden()) {
                legacyStates.add(WildcardStates.HIDDEN);
            }
            if (wildcardStatesAsUserInput) {
                if (legacyStates.isEmpty()) {
                    builder.field(EXPAND_WILDCARDS, "none");
                } else if (legacyStates.equals(EnumSet.allOf(WildcardStates.class))) {
                    builder.field(EXPAND_WILDCARDS, "all");
                } else {
                    builder.field(
                        EXPAND_WILDCARDS,
                        legacyStates.stream().map(WildcardStates::displayName).collect(Collectors.joining(","))
                    );
                }
            } else {
                builder.startArray(EXPAND_WILDCARDS);
                for (WildcardStates state : legacyStates) {
                    builder.value(state.displayName());
                }
                builder.endArray();
            }
            builder.field(ALLOW_NO_INDICES, allowEmptyExpressions());
            return builder;
        }

        public static class Builder {
            private boolean matchOpen;
            private boolean matchClosed;
            private boolean includeHidden;
            private boolean resolveAliases;
            private boolean allowEmptyExpressions;

            Builder() {
                this(DEFAULT);
            }

            Builder(WildcardOptions options) {
                matchOpen = options.matchOpen;
                matchClosed = options.matchClosed;
                includeHidden = options.includeHidden;
                resolveAliases = options.resolveAliases;
                allowEmptyExpressions = options.allowEmptyExpressions;
            }

            /**
             * Open indices will be matched. Defaults to true.
             */
            public Builder matchOpen(boolean matchOpen) {
                this.matchOpen = matchOpen;
                return this;
            }

            /**
             * Closed indices will be matched. Default to false.
             */
            public Builder matchClosed(boolean matchClosed) {
                this.matchClosed = matchClosed;
                return this;
            }

            /**
             * Hidden indices will be included from the result. Defaults to false.
             */
            public Builder includeHidden(boolean includeHidden) {
                this.includeHidden = includeHidden;
                return this;
            }

            /**
             * Aliases will be included in the result. Defaults to true.
             */
            public Builder resolveAliases(boolean resolveAliases) {
                this.resolveAliases = resolveAliases;
                return this;
            }

            /**
             * If true, when any of the expressions does not match any indices, we consider the result of this expression
             * empty; if all the expressions are empty then we have a successful but empty response.
             * If false, we throw an error immediately, so even if other expressions would result into indices the response
             * will contain only the error. Defaults to true.
             */
            public Builder allowEmptyExpressions(boolean allowEmptyExpressions) {
                this.allowEmptyExpressions = allowEmptyExpressions;
                return this;
            }

            /**
             * Disables expanding wildcards.
             */
            public Builder matchNone() {
                matchOpen = false;
                matchClosed = false;
                includeHidden = false;
                return this;
            }

            /**
             * Maximises the resolution of indices, we will match open, closed and hidden targets.
             */
            public Builder all() {
                matchOpen = true;
                matchClosed = true;
                includeHidden = true;
                return this;
            }

            /**
             * Parses the list of wildcard states to expand as provided by the user.
             * Logs a warning when the option 'none' is used along with other options because the position in the list
             * changes the outcome.
             */
            public Builder expandStates(String[] expandStates) {
                // Calling none() simulates a user providing an empty set of states
                matchNone();
                for (String expandState : expandStates) {
                    switch (expandState) {
                        case "open" -> matchOpen(true);
                        case "closed" -> matchClosed(true);
                        case "hidden" -> includeHidden(true);
                        case "all" -> all();
                        case "none" -> {
                            matchNone();
                            if (expandStates.length > 1) {
                                DEPRECATION_LOGGER.warn(DeprecationCategory.API, EXPAND_WILDCARDS, WILDCARD_NONE_DEPRECATION_MESSAGE);
                            }
                        }
                        default -> throw new IllegalArgumentException("No valid expand wildcard value [" + expandState + "]");
                    }
                }
                return this;
            }

            public WildcardOptions build() {
                return new WildcardOptions(matchOpen, matchClosed, includeHidden, resolveAliases, allowEmptyExpressions);
            }
        }

        public static Builder builder() {
            return new Builder();
        }

        public static Builder builder(WildcardOptions wildcardOptions) {
            return new Builder(wildcardOptions);
        }
    }

    /**
     * These options apply on all indices that have been selected by the other Options. It can either filter the response or
     * define what type of indices or aliases are not allowed which will result in an error response.
     * @param allowAliasToMultipleIndices, allow aliases to multiple indices, true by default.
     * @param allowClosedIndices, allow closed indices, true by default.
     * @param ignoreThrottled, filters out throttled (aka frozen indices), defaults to true.
     */
    public record GeneralOptions(boolean allowAliasToMultipleIndices, boolean allowClosedIndices, @Deprecated boolean ignoreThrottled)
        implements
            ToXContentFragment {

        public static final String IGNORE_THROTTLED = "ignore_throttled";
        public static final GeneralOptions DEFAULT = new GeneralOptions(true, true, false);

        public static GeneralOptions parseParameter(Object ignoreThrottled, GeneralOptions defaultOptions) {
            if (ignoreThrottled == null && defaultOptions != null) {
                return defaultOptions;
            }
            return (defaultOptions == null ? new Builder() : new Builder(defaultOptions)).ignoreThrottled(
                nodeBooleanValue(ignoreThrottled, IGNORE_THROTTLED)
            ).build();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.field(IGNORE_THROTTLED, ignoreThrottled());
        }

        public static class Builder {
            private boolean allowAliasToMultipleIndices;
            private boolean allowClosedIndices;
            private boolean ignoreThrottled;

            public Builder() {
                this(DEFAULT);
            }

            Builder(GeneralOptions options) {
                allowAliasToMultipleIndices = options.allowAliasToMultipleIndices;
                allowClosedIndices = options.allowClosedIndices;
                ignoreThrottled = options.ignoreThrottled;
            }

            /**
             * Aliases that resolve to multiple indices are accepted when true, otherwise the resolution will throw an error.
             * Defaults to true.
             */
            public Builder allowAliasToMultipleIndices(boolean allowAliasToMultipleIndices) {
                this.allowAliasToMultipleIndices = allowAliasToMultipleIndices;
                return this;
            }

            /**
             * Closed indices are accepted when true, otherwise the resolution will throw an error.
             * Defaults to true.
             */
            public Builder allowClosedIndices(boolean allowClosedIndices) {
                this.allowClosedIndices = allowClosedIndices;
                return this;
            }

            /**
             * Throttled indices will not be included in the result. Defaults to false.
             */
            public Builder ignoreThrottled(boolean ignoreThrottled) {
                this.ignoreThrottled = ignoreThrottled;
                return this;
            }

            public GeneralOptions build() {
                return new GeneralOptions(allowAliasToMultipleIndices, allowClosedIndices, ignoreThrottled);
            }
        }

        public static Builder builder() {
            return new Builder();
        }

        public static Builder builder(GeneralOptions generalOptions) {
            return new Builder(generalOptions);
        }
    }

    /**
     * This class is maintained for backwards compatibility and performance purposes. We use it for serialisation along with {@link Option}.
     */
    private enum WildcardStates {
        OPEN,
        CLOSED,
        HIDDEN;

        static WildcardOptions toWildcardOptions(EnumSet<WildcardStates> states, boolean allowNoIndices, boolean ignoreAlias) {
            return WildcardOptions.builder()
                .matchOpen(states.contains(OPEN))
                .matchClosed(states.contains(CLOSED))
                .includeHidden(states.contains(HIDDEN))
                .allowEmptyExpressions(allowNoIndices)
                .resolveAliases(ignoreAlias == false)
                .build();
        }

        String displayName() {
            return toString().toLowerCase(Locale.ROOT);
        }
    }

    /**
     * This class is maintained for backwards compatibility and performance purposes. We use it for serialisation along with
     * {@link WildcardStates}.
     */
    private enum Option {
        ALLOW_UNAVAILABLE_CONCRETE_TARGETS,
        EXCLUDE_ALIASES,
        ALLOW_EMPTY_WILDCARD_EXPRESSIONS,
        ERROR_WHEN_ALIASES_TO_MULTIPLE_INDICES,

        ERROR_WHEN_CLOSED_INDICES,
        IGNORE_THROTTLED
    }

    private static final DeprecationLogger DEPRECATION_LOGGER = DeprecationLogger.getLogger(IndicesOptions.class);
    private static final String IGNORE_THROTTLED_DEPRECATION_MESSAGE = "[ignore_throttled] parameter is deprecated "
        + "because frozen indices have been deprecated. Consider cold or frozen tiers in place of frozen indices.";

    private static final String WILDCARD_NONE_DEPRECATION_MESSAGE = "Combining the value 'none' with other options is deprecated "
        + "because it is order sensitive. Please revise the expression to work without the 'none' option or only use 'none'.";

    public static final IndicesOptions DEFAULT = new IndicesOptions(
        ConcreteTargetOptions.ERROR_WHEN_UNAVAILABLE_TARGETS,
        WildcardOptions.DEFAULT,
        GeneralOptions.DEFAULT
    );

    public static final IndicesOptions STRICT_EXPAND_OPEN = IndicesOptions.builder()
        .concreteTargetOptions(ConcreteTargetOptions.ERROR_WHEN_UNAVAILABLE_TARGETS)
        .wildcardOptions(
            WildcardOptions.builder()
                .matchOpen(true)
                .matchClosed(false)
                .includeHidden(false)
                .allowEmptyExpressions(true)
                .resolveAliases(true)
        )
        .generalOptions(GeneralOptions.builder().allowAliasToMultipleIndices(true).allowClosedIndices(true).ignoreThrottled(false))
        .build();
    public static final IndicesOptions LENIENT_EXPAND_OPEN = IndicesOptions.builder()
        .concreteTargetOptions(ConcreteTargetOptions.ALLOW_UNAVAILABLE_TARGETS)
        .wildcardOptions(
            WildcardOptions.builder()
                .matchOpen(true)
                .matchClosed(false)
                .includeHidden(false)
                .allowEmptyExpressions(true)
                .resolveAliases(true)
        )
        .generalOptions(GeneralOptions.builder().allowAliasToMultipleIndices(true).allowClosedIndices(true).ignoreThrottled(false))
        .build();
    public static final IndicesOptions LENIENT_EXPAND_OPEN_HIDDEN = IndicesOptions.builder()
        .concreteTargetOptions(ConcreteTargetOptions.ALLOW_UNAVAILABLE_TARGETS)
        .wildcardOptions(
            WildcardOptions.builder()
                .matchOpen(true)
                .matchClosed(false)
                .includeHidden(true)
                .allowEmptyExpressions(true)
                .resolveAliases(true)
        )
        .generalOptions(GeneralOptions.builder().allowAliasToMultipleIndices(true).allowClosedIndices(true).ignoreThrottled(false))
        .build();
    public static final IndicesOptions LENIENT_EXPAND_OPEN_CLOSED = IndicesOptions.builder()
        .concreteTargetOptions(ConcreteTargetOptions.ALLOW_UNAVAILABLE_TARGETS)
        .wildcardOptions(
            WildcardOptions.builder()
                .matchOpen(true)
                .matchClosed(true)
                .includeHidden(false)
                .allowEmptyExpressions(true)
                .resolveAliases(true)
        )
        .generalOptions(GeneralOptions.builder().allowAliasToMultipleIndices(true).allowClosedIndices(true).ignoreThrottled(false))
        .build();
    public static final IndicesOptions LENIENT_EXPAND_OPEN_CLOSED_HIDDEN = IndicesOptions.builder()
        .concreteTargetOptions(ConcreteTargetOptions.ALLOW_UNAVAILABLE_TARGETS)
        .wildcardOptions(
            WildcardOptions.builder().matchOpen(true).matchClosed(true).includeHidden(true).allowEmptyExpressions(true).resolveAliases(true)
        )
        .generalOptions(GeneralOptions.builder().allowAliasToMultipleIndices(true).allowClosedIndices(true).ignoreThrottled(false))
        .build();
    public static final IndicesOptions STRICT_EXPAND_OPEN_CLOSED = IndicesOptions.builder()
        .concreteTargetOptions(ConcreteTargetOptions.ERROR_WHEN_UNAVAILABLE_TARGETS)
        .wildcardOptions(
            WildcardOptions.builder()
                .matchOpen(true)
                .matchClosed(true)
                .includeHidden(false)
                .allowEmptyExpressions(true)
                .resolveAliases(true)
        )
        .generalOptions(GeneralOptions.builder().allowAliasToMultipleIndices(true).allowClosedIndices(true).ignoreThrottled(false))
        .build();
    public static final IndicesOptions STRICT_EXPAND_OPEN_CLOSED_HIDDEN = IndicesOptions.builder()
        .concreteTargetOptions(ConcreteTargetOptions.ERROR_WHEN_UNAVAILABLE_TARGETS)
        .wildcardOptions(
            WildcardOptions.builder().matchOpen(true).matchClosed(true).includeHidden(true).allowEmptyExpressions(true).resolveAliases(true)
        )
        .generalOptions(GeneralOptions.builder().allowAliasToMultipleIndices(true).allowClosedIndices(true).ignoreThrottled(false))
        .build();
    public static final IndicesOptions STRICT_EXPAND_OPEN_FORBID_CLOSED = IndicesOptions.builder()
        .concreteTargetOptions(ConcreteTargetOptions.ERROR_WHEN_UNAVAILABLE_TARGETS)
        .wildcardOptions(
            WildcardOptions.builder()
                .matchOpen(true)
                .matchClosed(false)
                .includeHidden(false)
                .allowEmptyExpressions(true)
                .resolveAliases(true)
        )
        .generalOptions(GeneralOptions.builder().allowClosedIndices(false).allowAliasToMultipleIndices(true).ignoreThrottled(false))
        .build();
    public static final IndicesOptions STRICT_EXPAND_OPEN_HIDDEN_FORBID_CLOSED = IndicesOptions.builder()
        .concreteTargetOptions(ConcreteTargetOptions.ERROR_WHEN_UNAVAILABLE_TARGETS)
        .wildcardOptions(
            WildcardOptions.builder()
                .matchOpen(true)
                .matchClosed(false)
                .includeHidden(true)
                .allowEmptyExpressions(true)
                .resolveAliases(true)
        )
        .generalOptions(GeneralOptions.builder().allowClosedIndices(false).allowAliasToMultipleIndices(true).ignoreThrottled(false))
        .build();
    public static final IndicesOptions STRICT_EXPAND_OPEN_FORBID_CLOSED_IGNORE_THROTTLED = IndicesOptions.builder()
        .concreteTargetOptions(ConcreteTargetOptions.ERROR_WHEN_UNAVAILABLE_TARGETS)
        .wildcardOptions(
            WildcardOptions.builder()
                .matchOpen(true)
                .matchClosed(false)
                .includeHidden(false)
                .allowEmptyExpressions(true)
                .resolveAliases(true)
        )
        .generalOptions(GeneralOptions.builder().ignoreThrottled(true).allowClosedIndices(false).allowAliasToMultipleIndices(true))
        .build();
    public static final IndicesOptions STRICT_SINGLE_INDEX_NO_EXPAND_FORBID_CLOSED = IndicesOptions.builder()
        .concreteTargetOptions(ConcreteTargetOptions.ERROR_WHEN_UNAVAILABLE_TARGETS)
        .wildcardOptions(
            WildcardOptions.builder()
                .matchOpen(false)
                .matchClosed(false)
                .includeHidden(false)
                .allowEmptyExpressions(true)
                .resolveAliases(true)
        )
        .generalOptions(GeneralOptions.builder().allowAliasToMultipleIndices(false).allowClosedIndices(false).ignoreThrottled(false))
        .build();
    public static final IndicesOptions STRICT_NO_EXPAND_FORBID_CLOSED = IndicesOptions.builder()
        .concreteTargetOptions(ConcreteTargetOptions.ERROR_WHEN_UNAVAILABLE_TARGETS)
        .wildcardOptions(
            WildcardOptions.builder()
                .matchOpen(false)
                .matchClosed(false)
                .includeHidden(false)
                .allowEmptyExpressions(true)
                .resolveAliases(true)
        )
        .generalOptions(GeneralOptions.builder().allowClosedIndices(false).allowAliasToMultipleIndices(true).ignoreThrottled(false))
        .build();

    /**
     * @return Whether specified concrete indices should be ignored when unavailable (missing or closed)
     */
    public boolean ignoreUnavailable() {
        return concreteTargetOptions.allowUnavailableTargets();
    }

    /**
     * @return Whether to ignore if a wildcard expression resolves to no concrete indices.
     * The `_all` string or empty list of indices count as wildcard expressions too.
     * Also when an alias points to a closed index this option decides if no concrete indices
     * are allowed.
     */
    public boolean allowNoIndices() {
        return wildcardOptions.allowEmptyExpressions();
    }

    /**
     * @return Whether wildcard expressions should get expanded to open indices
     */
    public boolean expandWildcardsOpen() {
        return wildcardOptions.matchOpen();
    }

    /**
     * @return Whether wildcard expressions should get expanded to closed indices
     */
    public boolean expandWildcardsClosed() {
        return wildcardOptions.matchClosed();
    }

    /**
     * @return whether wildcard expression should get expanded
     */
    public boolean expandWildcardExpressions() {
        // only check open/closed since if we do not expand to open or closed it doesn't make sense to
        // expand to hidden
        return expandWildcardsOpen() || expandWildcardsClosed();
    }

    /**
     * @return Whether wildcard expressions should get expanded to hidden indices
     */
    public boolean expandWildcardsHidden() {
        return wildcardOptions.includeHidden();
    }

    /**
     * @return Whether execution on closed indices is allowed.
     */
    public boolean forbidClosedIndices() {
        return generalOptions.allowClosedIndices() == false;
    }

    /**
     * @return whether aliases pointing to multiple indices are allowed
     */
    public boolean allowAliasesToMultipleIndices() {
        return generalOptions().allowAliasToMultipleIndices();
    }

    /**
     * @return whether aliases should be ignored (when resolving a wildcard)
     */
    public boolean ignoreAliases() {
        return wildcardOptions.resolveAliases() == false;
    }

    /**
     * @return whether indices that are marked as throttled should be ignored
     */
    public boolean ignoreThrottled() {
        return generalOptions().ignoreThrottled();
    }

    public void writeIndicesOptions(StreamOutput out) throws IOException {
        EnumSet<Option> backwardsCompatibleOptions = EnumSet.noneOf(Option.class);
        if (allowNoIndices()) {
            backwardsCompatibleOptions.add(Option.ALLOW_EMPTY_WILDCARD_EXPRESSIONS);
        }
        if (ignoreAliases()) {
            backwardsCompatibleOptions.add(Option.EXCLUDE_ALIASES);
        }
        if (allowAliasesToMultipleIndices() == false) {
            backwardsCompatibleOptions.add(Option.ERROR_WHEN_ALIASES_TO_MULTIPLE_INDICES);
        }
        if (forbidClosedIndices()) {
            backwardsCompatibleOptions.add(Option.ERROR_WHEN_CLOSED_INDICES);
        }
        if (ignoreThrottled()) {
            backwardsCompatibleOptions.add(Option.IGNORE_THROTTLED);
        }
        if (ignoreUnavailable()) {
            backwardsCompatibleOptions.add(Option.ALLOW_UNAVAILABLE_CONCRETE_TARGETS);
        }
        out.writeEnumSet(backwardsCompatibleOptions);

        EnumSet<WildcardStates> states = EnumSet.noneOf(WildcardStates.class);
        if (wildcardOptions.matchOpen()) {
            states.add(WildcardStates.OPEN);
        }
        if (wildcardOptions.matchClosed) {
            states.add(WildcardStates.CLOSED);
        }
        if (wildcardOptions.includeHidden()) {
            states.add(WildcardStates.HIDDEN);
        }
        out.writeEnumSet(states);
    }

    public static IndicesOptions readIndicesOptions(StreamInput in) throws IOException {
        EnumSet<Option> options = in.readEnumSet(Option.class);
        WildcardOptions wildcardOptions = WildcardStates.toWildcardOptions(
            in.readEnumSet(WildcardStates.class),
            options.contains(Option.ALLOW_EMPTY_WILDCARD_EXPRESSIONS),
            options.contains(Option.EXCLUDE_ALIASES)
        );
        GeneralOptions generalOptions = GeneralOptions.builder()
            .allowClosedIndices(options.contains(Option.ERROR_WHEN_CLOSED_INDICES) == false)
            .allowAliasToMultipleIndices(options.contains(Option.ERROR_WHEN_ALIASES_TO_MULTIPLE_INDICES) == false)
            .ignoreThrottled(options.contains(Option.IGNORE_THROTTLED))
            .build();
        return new IndicesOptions(
            options.contains(Option.ALLOW_UNAVAILABLE_CONCRETE_TARGETS)
                ? ConcreteTargetOptions.ALLOW_UNAVAILABLE_TARGETS
                : ConcreteTargetOptions.ERROR_WHEN_UNAVAILABLE_TARGETS,
            wildcardOptions,
            generalOptions
        );
    }

    public static class Builder {
        private ConcreteTargetOptions concreteTargetOptions;
        private WildcardOptions wildcardOptions;
        private GeneralOptions generalOptions;

        Builder() {
            this(DEFAULT);
        }

        Builder(IndicesOptions indicesOptions) {
            concreteTargetOptions = indicesOptions.concreteTargetOptions;
            wildcardOptions = indicesOptions.wildcardOptions;
            generalOptions = indicesOptions.generalOptions;
        }

        public Builder concreteTargetOptions(ConcreteTargetOptions concreteTargetOptions) {
            this.concreteTargetOptions = concreteTargetOptions;
            return this;
        }

        public Builder wildcardOptions(WildcardOptions wildcardOptions) {
            this.wildcardOptions = wildcardOptions;
            return this;
        }

        public Builder wildcardOptions(WildcardOptions.Builder wildcardOptions) {
            this.wildcardOptions = wildcardOptions.build();
            return this;
        }

        public Builder generalOptions(GeneralOptions generalOptions) {
            this.generalOptions = generalOptions;
            return this;
        }

        public Builder generalOptions(GeneralOptions.Builder generalOptions) {
            this.generalOptions = generalOptions.build();
            return this;
        }

        public IndicesOptions build() {
            return new IndicesOptions(concreteTargetOptions, wildcardOptions, generalOptions);
        }
    }

    public static IndicesOptions fromOptions(
        boolean ignoreUnavailable,
        boolean allowNoIndices,
        boolean expandToOpenIndices,
        boolean expandToClosedIndices
    ) {
        return fromOptions(ignoreUnavailable, allowNoIndices, expandToOpenIndices, expandToClosedIndices, false);
    }

    public static IndicesOptions fromOptions(
        boolean ignoreUnavailable,
        boolean allowNoIndices,
        boolean expandToOpenIndices,
        boolean expandToClosedIndices,
        boolean expandToHiddenIndices
    ) {
        return fromOptions(
            ignoreUnavailable,
            allowNoIndices,
            expandToOpenIndices,
            expandToClosedIndices,
            expandToHiddenIndices,
            true,
            false,
            false,
            false
        );
    }

    public static IndicesOptions fromOptions(
        boolean ignoreUnavailable,
        boolean allowNoIndices,
        boolean expandToOpenIndices,
        boolean expandToClosedIndices,
        IndicesOptions defaultOptions
    ) {
        return fromOptions(
            ignoreUnavailable,
            allowNoIndices,
            expandToOpenIndices,
            expandToClosedIndices,
            defaultOptions.expandWildcardsHidden(),
            defaultOptions.allowAliasesToMultipleIndices(),
            defaultOptions.forbidClosedIndices(),
            defaultOptions.ignoreAliases(),
            defaultOptions.ignoreThrottled()
        );
    }

    public static IndicesOptions fromOptions(
        boolean ignoreUnavailable,
        boolean allowNoIndices,
        boolean expandToOpenIndices,
        boolean expandToClosedIndices,
        boolean allowAliasesToMultipleIndices,
        boolean forbidClosedIndices,
        boolean ignoreAliases,
        boolean ignoreThrottled
    ) {
        return fromOptions(
            ignoreUnavailable,
            allowNoIndices,
            expandToOpenIndices,
            expandToClosedIndices,
            false,
            allowAliasesToMultipleIndices,
            forbidClosedIndices,
            ignoreAliases,
            ignoreThrottled
        );
    }

    public static IndicesOptions fromOptions(
        boolean ignoreUnavailable,
        boolean allowNoIndices,
        boolean expandToOpenIndices,
        boolean expandToClosedIndices,
        boolean expandToHiddenIndices,
        boolean allowAliasesToMultipleIndices,
        boolean forbidClosedIndices,
        boolean ignoreAliases,
        boolean ignoreThrottled
    ) {
        final WildcardOptions wildcards = WildcardOptions.builder()
            .matchOpen(expandToOpenIndices)
            .matchClosed(expandToClosedIndices)
            .includeHidden(expandToHiddenIndices)
            .resolveAliases(ignoreAliases == false)
            .allowEmptyExpressions(allowNoIndices)
            .build();
        final GeneralOptions generalOptions = GeneralOptions.builder()
            .allowAliasToMultipleIndices(allowAliasesToMultipleIndices)
            .allowClosedIndices(forbidClosedIndices == false)
            .ignoreThrottled(ignoreThrottled)
            .build();
        return new IndicesOptions(
            ignoreUnavailable ? ConcreteTargetOptions.ALLOW_UNAVAILABLE_TARGETS : ConcreteTargetOptions.ERROR_WHEN_UNAVAILABLE_TARGETS,
            wildcards,
            generalOptions
        );
    }

    public static IndicesOptions fromRequest(RestRequest request, IndicesOptions defaultSettings) {
        if (request.hasParam(GeneralOptions.IGNORE_THROTTLED)) {
            DEPRECATION_LOGGER.warn(DeprecationCategory.API, "ignore_throttled_param", IGNORE_THROTTLED_DEPRECATION_MESSAGE);
        }

        return fromParameters(
            request.param(WildcardOptions.EXPAND_WILDCARDS),
            request.param(ConcreteTargetOptions.IGNORE_UNAVAILABLE),
            request.param(WildcardOptions.ALLOW_NO_INDICES),
            request.param(GeneralOptions.IGNORE_THROTTLED),
            defaultSettings
        );
    }

    public static IndicesOptions fromMap(Map<String, Object> map, IndicesOptions defaultSettings) {
        return fromParameters(
            map.containsKey(WildcardOptions.EXPAND_WILDCARDS) ? map.get(WildcardOptions.EXPAND_WILDCARDS) : map.get("expandWildcards"),
            map.containsKey(ConcreteTargetOptions.IGNORE_UNAVAILABLE)
                ? map.get(ConcreteTargetOptions.IGNORE_UNAVAILABLE)
                : map.get("ignoreUnavailable"),
            map.containsKey(WildcardOptions.ALLOW_NO_INDICES) ? map.get(WildcardOptions.ALLOW_NO_INDICES) : map.get("allowNoIndices"),
            map.containsKey(GeneralOptions.IGNORE_THROTTLED) ? map.get(GeneralOptions.IGNORE_THROTTLED) : map.get("ignoreThrottled"),
            defaultSettings
        );
    }

    /**
     * Returns true if the name represents a valid name for one of the indices option
     * false otherwise
     */
    public static boolean isIndicesOptions(String name) {
        return WildcardOptions.EXPAND_WILDCARDS.equals(name)
            || "expandWildcards".equals(name)
            || ConcreteTargetOptions.IGNORE_UNAVAILABLE.equals(name)
            || "ignoreUnavailable".equals(name)
            || GeneralOptions.IGNORE_THROTTLED.equals(name)
            || "ignoreThrottled".equals(name)
            || WildcardOptions.ALLOW_NO_INDICES.equals(name)
            || "allowNoIndices".equals(name);
    }

    public static IndicesOptions fromParameters(
        Object wildcardsString,
        Object ignoreUnavailableString,
        Object allowNoIndicesString,
        Object ignoreThrottled,
        IndicesOptions defaultSettings
    ) {
        if (wildcardsString == null && ignoreUnavailableString == null && allowNoIndicesString == null && ignoreThrottled == null) {
            return defaultSettings;
        }

        WildcardOptions wildcards = WildcardOptions.parseParameters(wildcardsString, allowNoIndicesString, defaultSettings.wildcardOptions);
        GeneralOptions generalOptions = GeneralOptions.parseParameter(ignoreThrottled, defaultSettings.generalOptions);

        // note that allowAliasesToMultipleIndices is not exposed, always true (only for internal use)
        return IndicesOptions.builder()
            .concreteTargetOptions(ConcreteTargetOptions.fromParameter(ignoreUnavailableString, defaultSettings.concreteTargetOptions))
            .wildcardOptions(wildcards)
            .generalOptions(generalOptions)
            .build();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        concreteTargetOptions.toXContent(builder, params);
        wildcardOptions.toXContent(builder, params);
        generalOptions.toXContent(builder, params);
        return builder;
    }

    private static final ParseField EXPAND_WILDCARDS_FIELD = new ParseField(WildcardOptions.EXPAND_WILDCARDS);
    private static final ParseField IGNORE_UNAVAILABLE_FIELD = new ParseField(ConcreteTargetOptions.IGNORE_UNAVAILABLE);
    private static final ParseField IGNORE_THROTTLED_FIELD = new ParseField(GeneralOptions.IGNORE_THROTTLED).withAllDeprecated();
    private static final ParseField ALLOW_NO_INDICES_FIELD = new ParseField(WildcardOptions.ALLOW_NO_INDICES);

    public static IndicesOptions fromXContent(XContentParser parser) throws IOException {
        return fromXContent(parser, null);
    }

    public static IndicesOptions fromXContent(XContentParser parser, @Nullable IndicesOptions defaults) throws IOException {
        boolean parsedWildcardStates = false;
        WildcardOptions.Builder wildcards = defaults == null ? null : WildcardOptions.builder(defaults.wildcardOptions());
        GeneralOptions.Builder generalOptions = GeneralOptions.builder()
            .ignoreThrottled(defaults != null && defaults.generalOptions().ignoreThrottled());
        Boolean allowNoIndices = defaults == null ? null : defaults.allowNoIndices();
        Boolean ignoreUnavailable = defaults == null ? null : defaults.ignoreUnavailable();
        Token token = parser.currentToken() == Token.START_OBJECT ? parser.currentToken() : parser.nextToken();
        String currentFieldName = null;
        if (token != Token.START_OBJECT) {
            throw new ElasticsearchParseException("expected START_OBJECT as the token but was " + token);
        }
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == Token.START_ARRAY) {
                if (EXPAND_WILDCARDS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    if (parsedWildcardStates == false) {
                        parsedWildcardStates = true;
                        wildcards = WildcardOptions.builder();
                        List<String> values = new ArrayList<>();
                        while ((token = parser.nextToken()) != Token.END_ARRAY) {
                            if (token.isValue()) {
                                values.add(parser.text());
                            } else {
                                throw new ElasticsearchParseException(
                                    "expected values within array for " + EXPAND_WILDCARDS_FIELD.getPreferredName()
                                );
                            }
                        }
                        wildcards.expandStates(values.toArray(new String[] {}));
                    } else {
                        throw new ElasticsearchParseException("already parsed " + WildcardOptions.EXPAND_WILDCARDS);
                    }
                } else {
                    throw new ElasticsearchParseException(
                        EXPAND_WILDCARDS_FIELD.getPreferredName() + " is the only field that is an array in IndicesOptions"
                    );
                }
            } else if (token.isValue()) {
                if (EXPAND_WILDCARDS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    if (parsedWildcardStates == false) {
                        parsedWildcardStates = true;
                        wildcards = WildcardOptions.builder();
                        wildcards.expandStates(new String[] { parser.text() });
                    } else {
                        throw new ElasticsearchParseException("already parsed " + WildcardOptions.EXPAND_WILDCARDS);
                    }
                } else if (IGNORE_UNAVAILABLE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    ignoreUnavailable = parser.booleanValue();
                } else if (ALLOW_NO_INDICES_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    allowNoIndices = parser.booleanValue();
                } else if (IGNORE_THROTTLED_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    generalOptions.ignoreThrottled(parser.booleanValue());
                } else {
                    throw new ElasticsearchParseException(
                        "could not read indices options. unexpected index option [" + currentFieldName + "]"
                    );
                }
            } else {
                throw new ElasticsearchParseException("could not read indices options. unexpected object field [" + currentFieldName + "]");
            }
        }

        if (wildcards == null) {
            throw new ElasticsearchParseException("indices options xcontent did not contain " + EXPAND_WILDCARDS_FIELD.getPreferredName());
        } else {
            if (allowNoIndices == null) {
                throw new ElasticsearchParseException(
                    "indices options xcontent did not contain " + ALLOW_NO_INDICES_FIELD.getPreferredName()
                );
            } else {
                wildcards.allowEmptyExpressions(allowNoIndices);
            }
        }
        if (ignoreUnavailable == null) {
            throw new ElasticsearchParseException(
                "indices options xcontent did not contain " + IGNORE_UNAVAILABLE_FIELD.getPreferredName()
            );
        }
        return IndicesOptions.builder()
            .concreteTargetOptions(new ConcreteTargetOptions(ignoreUnavailable))
            .wildcardOptions(wildcards)
            .generalOptions(generalOptions)
            .build();
    }

    /**
     * @return indices options that requires every specified index to exist, expands wildcards only to open indices and
     * allows that no indices are resolved from wildcard expressions (not returning an error).
     */
    public static IndicesOptions strictExpandOpen() {
        return STRICT_EXPAND_OPEN;
    }

    /**
     * @return indices options that requires every specified index to exist, expands wildcards only to open indices,
     * allows that no indices are resolved from wildcard expressions (not returning an error) and forbids the
     * use of closed indices by throwing an error.
     */
    public static IndicesOptions strictExpandOpenAndForbidClosed() {
        return STRICT_EXPAND_OPEN_FORBID_CLOSED;
    }

    /**
     * @return indices options that requires every specified index to exist, expands wildcards only to open indices,
     * allows that no indices are resolved from wildcard expressions (not returning an error),
     * forbids the use of closed indices by throwing an error and ignores indices that are throttled.
     */
    public static IndicesOptions strictExpandOpenAndForbidClosedIgnoreThrottled() {
        return STRICT_EXPAND_OPEN_FORBID_CLOSED_IGNORE_THROTTLED;
    }

    /**
     * @return indices option that requires every specified index to exist, expands wildcards to both open and closed
     * indices and allows that no indices are resolved from wildcard expressions (not returning an error).
     */
    public static IndicesOptions strictExpand() {
        return STRICT_EXPAND_OPEN_CLOSED;
    }

    /**
     * @return indices option that requires every specified index to exist, expands wildcards to both open and closed indices, includes
     * hidden indices, and allows that no indices are resolved from wildcard expressions (not returning an error).
     */
    public static IndicesOptions strictExpandHidden() {
        return STRICT_EXPAND_OPEN_CLOSED_HIDDEN;
    }

    /**
     * @return indices option that requires each specified index or alias to exist, doesn't expand wildcards.
     */
    public static IndicesOptions strictNoExpandForbidClosed() {
        return STRICT_NO_EXPAND_FORBID_CLOSED;
    }

    /**
     * @return indices option that requires each specified index or alias to exist, doesn't expand wildcards and
     * throws error if any of the aliases resolves to multiple indices
     */
    public static IndicesOptions strictSingleIndexNoExpandForbidClosed() {
        return STRICT_SINGLE_INDEX_NO_EXPAND_FORBID_CLOSED;
    }

    /**
     * @return indices options that ignores unavailable indices, expands wildcards only to open indices and
     * allows that no indices are resolved from wildcard expressions (not returning an error).
     */
    public static IndicesOptions lenientExpandOpen() {
        return LENIENT_EXPAND_OPEN;
    }

    /**
     * @return indices options that ignores unavailable indices, expands wildcards to open and hidden indices, and
     * allows that no indices are resolved from wildcard expressions (not returning an error).
     */
    public static IndicesOptions lenientExpandOpenHidden() {
        return LENIENT_EXPAND_OPEN_HIDDEN;
    }

    /**
     * @return indices options that ignores unavailable indices,  expands wildcards to both open and closed
     * indices and allows that no indices are resolved from wildcard expressions (not returning an error).
     */
    public static IndicesOptions lenientExpand() {
        return LENIENT_EXPAND_OPEN_CLOSED;
    }

    /**
     * @return indices options that ignores unavailable indices,  expands wildcards to all open and closed
     * indices and allows that no indices are resolved from wildcard expressions (not returning an error).
     */
    public static IndicesOptions lenientExpandHidden() {
        return LENIENT_EXPAND_OPEN_CLOSED_HIDDEN;
    }

    @Override
    public String toString() {
        return "IndicesOptions["
            + "ignore_unavailable="
            + ignoreUnavailable()
            + ", allow_no_indices="
            + allowNoIndices()
            + ", expand_wildcards_open="
            + expandWildcardsOpen()
            + ", expand_wildcards_closed="
            + expandWildcardsClosed()
            + ", expand_wildcards_hidden="
            + expandWildcardsHidden()
            + ", allow_aliases_to_multiple_indices="
            + allowAliasesToMultipleIndices()
            + ", forbid_closed_indices="
            + forbidClosedIndices()
            + ", ignore_aliases="
            + ignoreAliases()
            + ", ignore_throttled="
            + ignoreThrottled()
            + ']';
    }
}

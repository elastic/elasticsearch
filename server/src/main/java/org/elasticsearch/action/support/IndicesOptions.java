/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.support;

import joptsimple.internal.Strings;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
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
import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeStringArrayValue;

/**
 * Controls how to deal with unavailable concrete indices (closed or missing), how wildcard expressions are expanded
 * to actual indices (all, closed or open indices) and how to deal with wildcard expressions that resolve to no indices.
 */
public record IndicesOptions(ConcreteTargetOptions concreteTargetOptions, WildcardOptions wildcardOptions, GeneralOptions generalOptions)
    implements
        ToXContentFragment {

    public static IndicesOptions.Builder newBuilder() {
        return new Builder();
    }

    public static IndicesOptions.Builder newBuilder(IndicesOptions indicesOptions) {
        return new Builder(indicesOptions);
    }

    /**
     * Controls the way the target indices will be handled.
     * @param allowUnavailableTargets, if false when any of the concrete targets requested does not exist, throw an error
     */
    public record ConcreteTargetOptions(boolean allowUnavailableTargets) implements ToXContentFragment {
        public static final ConcreteTargetOptions ALLOW_UNAVAILABLE_TARGETS = new ConcreteTargetOptions(true);
        public static final ConcreteTargetOptions ERROR_WHEN_UNAVAILABLE_TARGETS = new ConcreteTargetOptions(false);

        public static ConcreteTargetOptions fromParameter(Object ignoreUnavailableString, ConcreteTargetOptions defaultOption) {
            if (ignoreUnavailableString == null && defaultOption != null) {
                return defaultOption;
            }
            return nodeBooleanValue(ignoreUnavailableString, "ignore_unavailable")
                ? ALLOW_UNAVAILABLE_TARGETS
                : ERROR_WHEN_UNAVAILABLE_TARGETS;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.field("ignore_unavailable", allowUnavailableTargets);
        }
    }

    /**
     * Controls the way the wildcard expressions will be resolved.
     * @param includeOpen, open indices will be included
     * @param includeClosed, closed indices will be included
     * @param removeHidden, hidden indices will be removed from the result. This is a post filter, it requires includeOpen or includeClosed
     *                      to have an effect
     * @param resolveAliases, aliases will be included in the result, if false we treat them like they do not exist
     * @param allowEmptyExpressions, when an expression does not result in any indices, if false it throws an error if true it treats it as
     *                               an empty result
     */
    public record WildcardOptions(
        boolean includeOpen,
        boolean includeClosed,
        boolean removeHidden,
        boolean resolveAliases,
        boolean allowEmptyExpressions
    ) implements Writeable, ToXContentFragment {

        public static final WildcardOptions DEFAULT_OPEN = WildcardOptions.newBuilder().build();
        public static final WildcardOptions DEFAULT_OPEN_HIDDEN = WildcardOptions.newBuilder().removeHidden(false).build();
        public static final WildcardOptions DEFAULT_OPEN_CLOSED = WildcardOptions.newBuilder().includeClosed(true).build();
        public static final WildcardOptions DEFAULT_OPEN_CLOSED_HIDDEN = WildcardOptions.newBuilder()
            .includeClosed(true)
            .removeHidden(false)
            .build();
        public static final WildcardOptions DEFAULT_NONE = WildcardOptions.newBuilder().none().build();

        public static WildcardOptions read(StreamInput in) throws IOException {
            return new WildcardOptions(in.readBoolean(), in.readBoolean(), in.readBoolean(), in.readBoolean(), in.readBoolean());
        }

        public static WildcardOptions parseParameters(Object expandWildcards, Object allowNoIndices, WildcardOptions defaultOptions) {
            if (expandWildcards == null && allowNoIndices == null) {
                return defaultOptions;
            }
            WildcardOptions.Builder builder = defaultOptions == null ? new Builder() : new Builder(defaultOptions);
            if (expandWildcards != null) {
                builder.none();
                builder.expandStates(nodeStringArrayValue(expandWildcards));
            }

            if (allowNoIndices != null) {
                builder.allowEmptyExpressions(nodeBooleanValue(allowNoIndices, "allow_no_indices"));
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
            List<String> legacyStates = new ArrayList<>(3);
            if (includeOpen()) {
                legacyStates.add("open");
            }
            if (includeClosed()) {
                legacyStates.add("closed");
            }
            if (removeHidden() == false) {
                legacyStates.add("hidden");
            }
            if (wildcardStatesAsUserInput) {
                if (legacyStates.isEmpty()) {
                    builder.field("expand_wildcards", "none");
                } else if (legacyStates.size() == 3) {
                    builder.field("expand_wildcards", "all");
                } else {
                    builder.field("expand_wildcards", Strings.join(legacyStates, ","));
                }
            } else {
                builder.startArray("expand_wildcards");
                for (String state : legacyStates) {
                    builder.value(state);
                }
                builder.endArray();
            }
            builder.field("allow_no_indices", allowEmptyExpressions());
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(includeOpen);
            out.writeBoolean(includeClosed);
            out.writeBoolean(removeHidden);
            out.writeBoolean(resolveAliases);
            out.writeBoolean(allowEmptyExpressions);
        }

        public static class Builder {
            private boolean includeOpen = true;
            private boolean includeClosed = false;
            private boolean removeHidden = true;
            private boolean resolveAliases = true;
            private boolean allowEmptyExpressions = true;

            Builder() {}

            Builder(WildcardOptions options) {
                includeOpen = options.includeOpen;
                includeClosed = options.includeClosed;
                removeHidden = options.removeHidden;
                resolveAliases = options.resolveAliases;
                allowEmptyExpressions = options.allowEmptyExpressions;
            }

            /**
             * Open indices will be included. Defaults to true.
             */
            public Builder includeOpen(boolean includeOpen) {
                this.includeOpen = includeOpen;
                return this;
            }

            /**
             * Closed indices will be included. Default to false.
             */
            public Builder includeClosed(boolean includeClosed) {
                this.includeClosed = includeClosed;
                return this;
            }

            /**
             * Hidden indices will be removed from the result. Defaults to true.
             */
            public Builder removeHidden(boolean removeHidden) {
                this.removeHidden = removeHidden;
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
            public Builder none() {
                includeOpen = false;
                includeClosed = false;
                removeHidden = true;
                return this;
            }

            /**
             * Maximises the resolution of indices, we will match open, closed and hidden targets.
             */
            public Builder all() {
                includeOpen = true;
                includeClosed = true;
                removeHidden = false;
                return this;
            }

            /**
             * Parses the list of wildcard states to expand as provided by the user.
             * Logs a warning when the option 'none' is used along with other options because the position in the list
             * changes the outcome.
             */
            public Builder expandStates(String[] expandStates) {
                // Calling none() simulates a user providing an empty set of states
                none();
                for (String expandState : expandStates) {
                    switch (expandState) {
                        case "open" -> includeOpen(true);
                        case "closed" -> includeClosed(true);
                        case "hidden" -> removeHidden(false);
                        case "all" -> all();
                        case "none" -> {
                            none();
                            if (expandStates.length > 1) {
                                DEPRECATION_LOGGER.warn(DeprecationCategory.API, "expand_wildcards", WILDCARD_NONE_DEPRECATION_MESSAGE);
                            }
                        }
                    }
                }
                return this;
            }

            public WildcardOptions build() {
                return new WildcardOptions(includeOpen, includeClosed, removeHidden, resolveAliases, allowEmptyExpressions);
            }
        }

        public static Builder newBuilder() {
            return new Builder();
        }

        public static Builder newBuilder(WildcardOptions wildcardOptions) {
            return new Builder(wildcardOptions);
        }
    }

    /**
     * These options apply on all indices that have been selected by the other Options. It can either filter the response or
     * define what type of indices or aliases are not allowed which will result in an error response.
     * @param allowAliasToMultipleIndices, allow aliases to multiple indices, true by default.
     * @param allowClosedIndices, allow closed indices, true by default.
     * @param removeThrottled, filters out throttled (aka frozen indices), defaults to true.
     */
    public record GeneralOptions(boolean allowAliasToMultipleIndices, boolean allowClosedIndices, @Deprecated boolean removeThrottled)
        implements
            Writeable,
            ToXContentFragment {

        public static final GeneralOptions DEFAULT = GeneralOptions.newBuilder().build();

        public static GeneralOptions read(StreamInput in) throws IOException {
            return new GeneralOptions(in.readBoolean(), in.readBoolean(), in.readBoolean());
        }

        public static GeneralOptions parseParameter(Object ignoreThrottled, GeneralOptions defaultOptions) {
            if (ignoreThrottled == null && defaultOptions != null) {
                return defaultOptions;
            }
            return (defaultOptions == null ? new Builder() : new Builder(defaultOptions)).removeThrottled(
                nodeBooleanValue(ignoreThrottled, "ignore_throttled")
            ).build();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.field("ignore_throttled", removeThrottled());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(allowAliasToMultipleIndices);
            out.writeBoolean(allowClosedIndices);
            out.writeBoolean(removeThrottled);
        }

        public static class Builder {
            private boolean allowAliasToMultipleIndices = true;
            private boolean allowClosedIndices = true;
            private boolean removeThrottled = false;

            public Builder() {}

            Builder(GeneralOptions options) {
                allowAliasToMultipleIndices = options.allowAliasToMultipleIndices;
                allowClosedIndices = options.allowClosedIndices;
                removeThrottled = options.removeThrottled;
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
            public Builder removeThrottled(boolean removeThrottled) {
                this.removeThrottled = removeThrottled;
                return this;
            }

            public GeneralOptions build() {
                return new GeneralOptions(allowAliasToMultipleIndices, allowClosedIndices, removeThrottled);
            }
        }

        public static Builder newBuilder() {
            return new Builder();
        }

        public static Builder newBuilder(GeneralOptions generalOptions) {
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
            return WildcardOptions.newBuilder()
                .includeOpen(states.contains(OPEN))
                .includeClosed(states.contains(CLOSED))
                .removeHidden(states.contains(HIDDEN) == false)
                .allowEmptyExpressions(allowNoIndices)
                .resolveAliases(ignoreAlias == false)
                .build();
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
        EXCLUDE_THROTTLED;
    }

    private static final DeprecationLogger DEPRECATION_LOGGER = DeprecationLogger.getLogger(IndicesOptions.class);
    private static final String IGNORE_THROTTLED_DEPRECATION_MESSAGE = "[ignore_throttled] parameter is deprecated "
        + "because frozen indices have been deprecated. Consider cold or frozen tiers in place of frozen indices.";

    private static final String WILDCARD_NONE_DEPRECATION_MESSAGE = "Combining the value 'none' with other options is deprecated "
        + "because it is order sensitive. Please revise the expression to work without the 'none' option or only use 'none'.";

    public static final IndicesOptions DEFAULT = IndicesOptions.newBuilder().build();

    public static final IndicesOptions STRICT_EXPAND_OPEN = DEFAULT;
    public static final IndicesOptions LENIENT_EXPAND_OPEN = IndicesOptions.newBuilder()
        .concreteTargetOptions(ConcreteTargetOptions.ALLOW_UNAVAILABLE_TARGETS)
        .build();
    public static final IndicesOptions LENIENT_EXPAND_OPEN_HIDDEN = IndicesOptions.newBuilder()
        .concreteTargetOptions(ConcreteTargetOptions.ALLOW_UNAVAILABLE_TARGETS)
        .wildcardOptions(WildcardOptions.DEFAULT_OPEN_HIDDEN)
        .build();
    public static final IndicesOptions LENIENT_EXPAND_OPEN_CLOSED = IndicesOptions.newBuilder()
        .concreteTargetOptions(ConcreteTargetOptions.ALLOW_UNAVAILABLE_TARGETS)
        .wildcardOptions(WildcardOptions.DEFAULT_OPEN_CLOSED)
        .build();
    public static final IndicesOptions LENIENT_EXPAND_OPEN_CLOSED_HIDDEN = IndicesOptions.newBuilder()
        .concreteTargetOptions(ConcreteTargetOptions.ALLOW_UNAVAILABLE_TARGETS)
        .wildcardOptions(WildcardOptions.DEFAULT_OPEN_CLOSED_HIDDEN)
        .build();
    public static final IndicesOptions STRICT_EXPAND_OPEN_CLOSED = IndicesOptions.newBuilder()
        .wildcardOptions(WildcardOptions.DEFAULT_OPEN_CLOSED)
        .build();
    public static final IndicesOptions STRICT_EXPAND_OPEN_CLOSED_HIDDEN = IndicesOptions.newBuilder()
        .wildcardOptions(WildcardOptions.DEFAULT_OPEN_CLOSED_HIDDEN)
        .build();
    public static final IndicesOptions STRICT_EXPAND_OPEN_FORBID_CLOSED = IndicesOptions.newBuilder()
        .generalOptions(GeneralOptions.newBuilder().allowClosedIndices(false))
        .build();
    public static final IndicesOptions STRICT_EXPAND_OPEN_HIDDEN_FORBID_CLOSED = IndicesOptions.newBuilder()
        .wildcardOptions(WildcardOptions.DEFAULT_OPEN_HIDDEN)
        .generalOptions(GeneralOptions.newBuilder().allowClosedIndices(false))
        .build();
    public static final IndicesOptions STRICT_EXPAND_OPEN_FORBID_CLOSED_IGNORE_THROTTLED = IndicesOptions.newBuilder()
        .generalOptions(GeneralOptions.newBuilder().removeThrottled(true).allowClosedIndices(false))
        .build();
    public static final IndicesOptions STRICT_SINGLE_INDEX_NO_EXPAND_FORBID_CLOSED = IndicesOptions.newBuilder()
        .wildcardOptions(WildcardOptions.DEFAULT_NONE)
        .generalOptions(GeneralOptions.newBuilder().allowAliasToMultipleIndices(false).allowClosedIndices(false))
        .build();
    public static final IndicesOptions STRICT_NO_EXPAND_FORBID_CLOSED = IndicesOptions.newBuilder()
        .wildcardOptions(WildcardOptions.DEFAULT_NONE)
        .generalOptions(GeneralOptions.newBuilder().allowClosedIndices(false))
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
        return wildcardOptions.includeOpen();
    }

    /**
     * @return Whether wildcard expressions should get expanded to closed indices
     */
    public boolean expandWildcardsClosed() {
        return wildcardOptions.includeClosed();
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
        return wildcardOptions.removeHidden() == false;
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
        return generalOptions().removeThrottled();
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
            backwardsCompatibleOptions.add(Option.EXCLUDE_THROTTLED);
        }
        if (ignoreUnavailable()) {
            backwardsCompatibleOptions.add(Option.ALLOW_UNAVAILABLE_CONCRETE_TARGETS);
        }
        out.writeEnumSet(backwardsCompatibleOptions);

        EnumSet<WildcardStates> states = EnumSet.noneOf(WildcardStates.class);
        if (wildcardOptions.includeOpen()) {
            states.add(WildcardStates.OPEN);
        }
        if (wildcardOptions.includeClosed) {
            states.add(WildcardStates.CLOSED);
        }
        if (wildcardOptions.removeHidden() == false) {
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
        GeneralOptions generalOptions = GeneralOptions.newBuilder()
            .allowClosedIndices(options.contains(Option.ERROR_WHEN_CLOSED_INDICES) == false)
            .allowAliasToMultipleIndices(options.contains(Option.ERROR_WHEN_ALIASES_TO_MULTIPLE_INDICES) == false)
            .removeThrottled(options.contains(Option.EXCLUDE_THROTTLED))
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
        private ConcreteTargetOptions concreteTargetOptions = ConcreteTargetOptions.ERROR_WHEN_UNAVAILABLE_TARGETS;
        private WildcardOptions wildcardOptions = WildcardOptions.DEFAULT_OPEN;
        private GeneralOptions generalOptions = GeneralOptions.DEFAULT;

        Builder() {}

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
        final WildcardOptions wildcards = WildcardOptions.newBuilder()
            .includeOpen(expandToOpenIndices)
            .includeClosed(expandToClosedIndices)
            .removeHidden(expandToHiddenIndices == false)
            .resolveAliases(ignoreAliases == false)
            .allowEmptyExpressions(allowNoIndices)
            .build();
        final GeneralOptions generalOptions = GeneralOptions.newBuilder()
            .allowAliasToMultipleIndices(allowAliasesToMultipleIndices)
            .allowClosedIndices(forbidClosedIndices == false)
            .removeThrottled(ignoreThrottled)
            .build();
        return new IndicesOptions(
            ignoreUnavailable ? ConcreteTargetOptions.ALLOW_UNAVAILABLE_TARGETS : ConcreteTargetOptions.ERROR_WHEN_UNAVAILABLE_TARGETS,
            wildcards,
            generalOptions
        );
    }

    public static IndicesOptions fromRequest(RestRequest request, IndicesOptions defaultSettings) {
        if (request.hasParam("ignore_throttled")) {
            DEPRECATION_LOGGER.warn(DeprecationCategory.API, "ignore_throttled_param", IGNORE_THROTTLED_DEPRECATION_MESSAGE);
        }

        return fromParameters(
            request.param("expand_wildcards"),
            request.param("ignore_unavailable"),
            request.param("allow_no_indices"),
            request.param("ignore_throttled"),
            defaultSettings
        );
    }

    public static IndicesOptions fromMap(Map<String, Object> map, IndicesOptions defaultSettings) {
        return fromParameters(
            map.containsKey("expand_wildcards") ? map.get("expand_wildcards") : map.get("expandWildcards"),
            map.containsKey("ignore_unavailable") ? map.get("ignore_unavailable") : map.get("ignoreUnavailable"),
            map.containsKey("allow_no_indices") ? map.get("allow_no_indices") : map.get("allowNoIndices"),
            map.containsKey("ignore_throttled") ? map.get("ignore_throttled") : map.get("ignoreThrottled"),
            defaultSettings
        );
    }

    /**
     * Returns true if the name represents a valid name for one of the indices option
     * false otherwise
     */
    public static boolean isIndicesOptions(String name) {
        return "expand_wildcards".equals(name)
            || "expandWildcards".equals(name)
            || "ignore_unavailable".equals(name)
            || "ignoreUnavailable".equals(name)
            || "ignore_throttled".equals(name)
            || "ignoreThrottled".equals(name)
            || "allow_no_indices".equals(name)
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
        return fromOptions(
            ConcreteTargetOptions.fromParameter(ignoreUnavailableString, defaultSettings.concreteTargetOptions).allowUnavailableTargets(),
            wildcards.allowEmptyExpressions(),
            wildcards.includeOpen(),
            wildcards.includeClosed(),
            wildcards.removeHidden() == false,
            generalOptions.allowAliasToMultipleIndices(),
            generalOptions.allowClosedIndices() == false,
            wildcards.resolveAliases() == false,
            generalOptions.removeThrottled()
        );
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        concreteTargetOptions.toXContent(builder, params);
        wildcardOptions.toXContent(builder, params);
        generalOptions.toXContent(builder, params);
        return builder;
    }

    private static final ParseField EXPAND_WILDCARDS_FIELD = new ParseField("expand_wildcards");
    private static final ParseField IGNORE_UNAVAILABLE_FIELD = new ParseField("ignore_unavailable");
    private static final ParseField IGNORE_THROTTLED_FIELD = new ParseField("ignore_throttled").withAllDeprecated();
    private static final ParseField ALLOW_NO_INDICES_FIELD = new ParseField("allow_no_indices");

    public static IndicesOptions fromXContent(XContentParser parser) throws IOException {
        return fromXContent(parser, null);
    }

    public static IndicesOptions fromXContent(XContentParser parser, @Nullable IndicesOptions defaults) throws IOException {
        boolean parsedWildcardStates = false;
        WildcardOptions.Builder wildcardsBuilder = defaults == null ? null : WildcardOptions.newBuilder(defaults.wildcardOptions());
        GeneralOptions.Builder generalBuilder = GeneralOptions.newBuilder()
            .removeThrottled(defaults != null && defaults.generalOptions().removeThrottled());
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
                        wildcardsBuilder = WildcardOptions.newBuilder();
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
                        wildcardsBuilder.expandStates(values.toArray(new String[] {}));
                    } else {
                        throw new ElasticsearchParseException("already parsed expand_wildcards");
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
                        wildcardsBuilder = WildcardOptions.newBuilder();
                        wildcardsBuilder.expandStates(new String[] { parser.text() });
                    } else {
                        throw new ElasticsearchParseException("already parsed expand_wildcards");
                    }
                } else if (IGNORE_UNAVAILABLE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    ignoreUnavailable = parser.booleanValue();
                } else if (ALLOW_NO_INDICES_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    allowNoIndices = parser.booleanValue();
                } else if (IGNORE_THROTTLED_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    generalBuilder.removeThrottled(parser.booleanValue());
                } else {
                    throw new ElasticsearchParseException(
                        "could not read indices options. unexpected index option [" + currentFieldName + "]"
                    );
                }
            } else {
                throw new ElasticsearchParseException("could not read indices options. unexpected object field [" + currentFieldName + "]");
            }
        }

        if (wildcardsBuilder == null) {
            throw new ElasticsearchParseException("indices options xcontent did not contain " + EXPAND_WILDCARDS_FIELD.getPreferredName());
        } else {
            if (allowNoIndices == null) {
                throw new ElasticsearchParseException(
                    "indices options xcontent did not contain " + ALLOW_NO_INDICES_FIELD.getPreferredName()
                );
            } else {
                wildcardsBuilder.allowEmptyExpressions(allowNoIndices);
            }
        }
        if (ignoreUnavailable == null) {
            throw new ElasticsearchParseException(
                "indices options xcontent did not contain " + IGNORE_UNAVAILABLE_FIELD.getPreferredName()
            );
        }

        WildcardOptions wildcardOptions = wildcardsBuilder.build();
        GeneralOptions generalOptions = generalBuilder.build();
        return IndicesOptions.fromOptions(
            ignoreUnavailable,
            wildcardOptions.allowEmptyExpressions(),
            wildcardOptions.includeOpen(),
            wildcardOptions.includeClosed(),
            wildcardOptions.removeHidden() == false,
            generalOptions.allowAliasToMultipleIndices(),
            generalOptions.allowClosedIndices() == false,
            wildcardOptions.resolveAliases() == false,
            generalOptions.removeThrottled()
        );
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

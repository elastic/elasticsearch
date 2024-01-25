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
import org.elasticsearch.TransportVersions;
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
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeStringArrayValue;

/**
 * Controls how to deal with unavailable concrete indices (closed or missing), how wildcard expressions are expanded
 * to actual indices (all, closed or open indices) and how to deal with wildcard expressions that resolve to no indices.
 */
public record IndicesOptions(EnumSet<Option> options, WildcardOptions wildcardOptions, GeneralOptions generalOptions)
    implements
        ToXContentFragment {

    public IndicesOptions(EnumSet<Option> options, WildcardOptions wildcardOptions) {
        this(options, wildcardOptions, GeneralOptions.DEFAULT);
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
    ) implements Writeable {

        public static final WildcardOptions DEFAULT_OPEN = new WildcardOptions.Builder().build();
        public static final WildcardOptions DEFAULT_OPEN_HIDDEN = new WildcardOptions.Builder().removeHidden(false).build();
        public static final WildcardOptions DEFAULT_OPEN_CLOSED = new WildcardOptions.Builder().includeClosed(true).build();
        public static final WildcardOptions DEFAULT_OPEN_CLOSED_HIDDEN = new WildcardOptions.Builder().includeClosed(true)
            .removeHidden(false)
            .build();
        public static final WildcardOptions DEFAULT_NONE = new WildcardOptions.Builder().none().build();

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

        public static XContentBuilder toXContent(WildcardOptions options, XContentBuilder builder) throws IOException {
            return toXContent(options, builder, false);
        }

        /**
         * This converter to XContent only includes the fields a user can interact, internal options like the resolveAliases
         * are not added.
         * @param wildcardStatesAsUserInput, some parts of the code expect the serialization of the expand_wildcards field
         *                                   to be a comma separated string that matches the allowed user input, this includes
         *                                   all the states along with the values 'all' and 'none'.
         */
        public static XContentBuilder toXContent(WildcardOptions options, XContentBuilder builder, boolean wildcardStatesAsUserInput)
            throws IOException {
            List<String> legacyStates = new ArrayList<>(3);
            if (options.includeOpen()) {
                legacyStates.add("open");
            }
            if (options.includeClosed()) {
                legacyStates.add("closed");
            }
            if (options.removeHidden() == false) {
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
            builder.field("allow_no_indices", options.allowEmptyExpressions());
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

            public Builder() {}

            public Builder(WildcardOptions options) {
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
    }

    /**
     * This class is maintained for backwards compatibility purposes. We use it for serialisation before {@link WildcardOptions}
     * was introduced.
     */
    private enum WildcardStates {
        OPEN,
        CLOSED,
        HIDDEN;

        static EnumSet<WildcardStates> fromWildcardOptions(WildcardOptions options) {
            EnumSet<WildcardStates> states = EnumSet.noneOf(WildcardStates.class);
            if (options.includeOpen()) {
                states.add(OPEN);
            }
            if (options.includeClosed) {
                states.add(CLOSED);
            }
            if (options.removeHidden() == false) {
                states.add(HIDDEN);
            }
            return states;
        }

        static WildcardOptions toWildcardOptions(EnumSet<WildcardStates> states, boolean allowNoIndices, boolean ignoreAlias) {
            return new WildcardOptions.Builder().includeOpen(states.contains(OPEN))
                .includeClosed(states.contains(CLOSED))
                .removeHidden(states.contains(HIDDEN) == false)
                .allowEmptyExpressions(allowNoIndices)
                .resolveAliases(ignoreAlias == false)
                .build();
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
            Writeable {

        public static final GeneralOptions DEFAULT = new GeneralOptions.Builder().build();

        public static GeneralOptions read(StreamInput in) throws IOException {
            return new GeneralOptions(in.readBoolean(), in.readBoolean(), in.readBoolean());
        }

        public static GeneralOptions parseParameters(Object ignoreThrottled, GeneralOptions defaultOptions) {
            if (ignoreThrottled == null) {
                return defaultOptions;
            }
            return (defaultOptions == null ? new Builder() : new Builder(defaultOptions)).removeThrottled(
                nodeBooleanValue(ignoreThrottled, "ignore_throttled")
            ).build();
        }

        public static XContentBuilder toXContent(GeneralOptions options, XContentBuilder builder) throws IOException {
            builder.field("ignore_throttled", options.removeThrottled());
            return builder;
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

            public Builder(GeneralOptions options) {
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
    }

    public enum Option {
        IGNORE_UNAVAILABLE,
        /**
         * Please use {@link WildcardOptions#resolveAliases}
         */
        @Deprecated
        DEPRECATED__IGNORE_ALIASES,
        /**
         * Please use {@link WildcardOptions#allowEmptyExpressions}
         */
        @Deprecated
        DEPRECATED__ALLOW_NO_INDICES,
        /**
         * Please use {@link GeneralOptions#allowAliasToMultipleIndices}
         */
        @Deprecated
        DEPRECATED__FORBID_ALIASES_TO_MULTIPLE_INDICES,
        /**
         * Please use {@link GeneralOptions#allowClosedIndices}
         */
        @Deprecated
        DEPRECATED__FORBID_CLOSED_INDICES,
        /**
         * Please use {@link GeneralOptions#ignoreThrottled}
         */
        @Deprecated
        DEPRECATED__IGNORE_THROTTLED;

        public static final EnumSet<Option> NONE = EnumSet.noneOf(Option.class);

        /**
         * These are the values that can still be used from this enum. There are replacement for the ones
         */
        public static final EnumSet<Option> VALUES_IN_USE = Arrays.stream(Option.values())
            .filter(option -> option.name().startsWith("DEPRECATED_") == false)
            .collect(Collectors.toCollection(() -> EnumSet.noneOf(Option.class)));
    }

    private static final DeprecationLogger DEPRECATION_LOGGER = DeprecationLogger.getLogger(IndicesOptions.class);
    private static final String IGNORE_THROTTLED_DEPRECATION_MESSAGE = "[ignore_throttled] parameter is deprecated "
        + "because frozen indices have been deprecated. Consider cold or frozen tiers in place of frozen indices.";

    private static final String WILDCARD_NONE_DEPRECATION_MESSAGE = "Combining the value 'none' with other options is deprecated "
        + "because it is order sensitive. Please revise the expression to work without the 'none' option or only use 'none'.";

    public static final IndicesOptions STRICT_EXPAND_OPEN = new IndicesOptions(EnumSet.noneOf(Option.class), WildcardOptions.DEFAULT_OPEN);
    public static final IndicesOptions LENIENT_EXPAND_OPEN = new IndicesOptions(
        EnumSet.of(Option.IGNORE_UNAVAILABLE),
        WildcardOptions.DEFAULT_OPEN
    );
    public static final IndicesOptions LENIENT_EXPAND_OPEN_HIDDEN = new IndicesOptions(
        EnumSet.of(Option.IGNORE_UNAVAILABLE),
        WildcardOptions.DEFAULT_OPEN_HIDDEN
    );
    public static final IndicesOptions LENIENT_EXPAND_OPEN_CLOSED = new IndicesOptions(
        EnumSet.of(Option.IGNORE_UNAVAILABLE),
        WildcardOptions.DEFAULT_OPEN_CLOSED
    );
    public static final IndicesOptions LENIENT_EXPAND_OPEN_CLOSED_HIDDEN = new IndicesOptions(
        EnumSet.of(Option.IGNORE_UNAVAILABLE),
        WildcardOptions.DEFAULT_OPEN_CLOSED_HIDDEN
    );
    public static final IndicesOptions STRICT_EXPAND_OPEN_CLOSED = new IndicesOptions(
        EnumSet.noneOf(Option.class),
        WildcardOptions.DEFAULT_OPEN_CLOSED
    );
    public static final IndicesOptions STRICT_EXPAND_OPEN_CLOSED_HIDDEN = new IndicesOptions(
        EnumSet.noneOf(Option.class),
        WildcardOptions.DEFAULT_OPEN_CLOSED_HIDDEN
    );
    public static final IndicesOptions STRICT_EXPAND_OPEN_FORBID_CLOSED = new IndicesOptions(
        EnumSet.noneOf(Option.class),
        WildcardOptions.DEFAULT_OPEN,
        new GeneralOptions.Builder().allowClosedIndices(false).build()
    );
    public static final IndicesOptions STRICT_EXPAND_OPEN_HIDDEN_FORBID_CLOSED = new IndicesOptions(
        EnumSet.noneOf(Option.class),
        WildcardOptions.DEFAULT_OPEN_HIDDEN,
        new GeneralOptions.Builder().allowClosedIndices(false).build()
    );
    public static final IndicesOptions STRICT_EXPAND_OPEN_FORBID_CLOSED_IGNORE_THROTTLED = new IndicesOptions(
        EnumSet.noneOf(Option.class),
        WildcardOptions.DEFAULT_OPEN,
        new GeneralOptions.Builder().removeThrottled(true).allowClosedIndices(false).build()
    );
    public static final IndicesOptions STRICT_SINGLE_INDEX_NO_EXPAND_FORBID_CLOSED = new IndicesOptions(
        EnumSet.noneOf(Option.class),
        WildcardOptions.DEFAULT_NONE,
        new GeneralOptions.Builder().allowAliasToMultipleIndices(false).allowClosedIndices(false).build()
    );
    public static final IndicesOptions STRICT_NO_EXPAND_FORBID_CLOSED = new IndicesOptions(
        EnumSet.noneOf(Option.class),
        WildcardOptions.DEFAULT_NONE,
        new GeneralOptions.Builder().allowClosedIndices(false).build()
    );

    /**
     * @return Whether specified concrete indices should be ignored when unavailable (missing or closed)
     */
    public boolean ignoreUnavailable() {
        return options.contains(Option.IGNORE_UNAVAILABLE);
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

    /**
     * @return the {@link WildcardOptions} that these indices options will expand to
     */
    public WildcardOptions wildcardOptions() {
        return wildcardOptions;
    }

    /**
     * @return the {@link GeneralOptions} that these indices options will expand to
     */
    public GeneralOptions generalOptions() {
        return generalOptions;
    }

    /**
     * @return a copy of the {@link Option}s that these indices options will use
     */
    public EnumSet<Option> options() {
        return EnumSet.copyOf(options);
    }

    public void writeIndicesOptions(StreamOutput out) throws IOException {
        if (out.getTransportVersion().onOrAfter(TransportVersions.GROUP_INDICES_OPTIONS)) {
            out.writeEnumSet(options);
            wildcardOptions.writeTo(out);
            generalOptions.writeTo(out);
        } else {
            EnumSet<Option> backwardsCompatibleOptions = options.clone();
            if (allowNoIndices()) {
                backwardsCompatibleOptions.add(Option.DEPRECATED__ALLOW_NO_INDICES);
            }
            if (ignoreAliases()) {
                backwardsCompatibleOptions.add(Option.DEPRECATED__IGNORE_ALIASES);
            }
            if (allowAliasesToMultipleIndices() == false) {
                backwardsCompatibleOptions.add(Option.DEPRECATED__FORBID_ALIASES_TO_MULTIPLE_INDICES);
            }
            if (forbidClosedIndices()) {
                backwardsCompatibleOptions.add(Option.DEPRECATED__FORBID_CLOSED_INDICES);
            }
            if (ignoreThrottled()) {
                backwardsCompatibleOptions.add(Option.DEPRECATED__IGNORE_THROTTLED);
            }
            out.writeEnumSet(backwardsCompatibleOptions);
            out.writeEnumSet(WildcardStates.fromWildcardOptions(wildcardOptions));
        }
    }

    public static IndicesOptions readIndicesOptions(StreamInput in) throws IOException {
        EnumSet<Option> options = in.readEnumSet(Option.class);
        WildcardOptions wildcardOptions;
        GeneralOptions generalOptions;
        if (in.getTransportVersion().onOrAfter(TransportVersions.GROUP_INDICES_OPTIONS)) {
            wildcardOptions = WildcardOptions.read(in);
            generalOptions = GeneralOptions.read(in);
        } else {
            EnumSet<WildcardStates> states = in.readEnumSet(WildcardStates.class);
            wildcardOptions = WildcardStates.toWildcardOptions(
                states,
                options.contains(Option.DEPRECATED__ALLOW_NO_INDICES),
                options.contains(Option.DEPRECATED__IGNORE_ALIASES)
            );
            generalOptions = new GeneralOptions.Builder().allowClosedIndices(
                options.contains(Option.DEPRECATED__FORBID_CLOSED_INDICES) == false
            )
                .allowAliasToMultipleIndices(options.contains(Option.DEPRECATED__FORBID_ALIASES_TO_MULTIPLE_INDICES) == false)
                .removeThrottled(options.contains(Option.DEPRECATED__IGNORE_THROTTLED))
                .build();
            options = options.stream()
                .filter(option -> option.name().startsWith("DEPRECATED_") == false)
                .collect(Collectors.toCollection(() -> EnumSet.noneOf(Option.class)));
        }
        return new IndicesOptions(options, wildcardOptions, generalOptions);
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
        final EnumSet<Option> opts = EnumSet.noneOf(Option.class);
        final WildcardOptions wildcards = new WildcardOptions.Builder().includeOpen(expandToOpenIndices)
            .includeClosed(expandToClosedIndices)
            .removeHidden(expandToHiddenIndices == false)
            .resolveAliases(ignoreAliases == false)
            .allowEmptyExpressions(allowNoIndices)
            .build();
        final GeneralOptions generalOptions = new GeneralOptions.Builder().allowAliasToMultipleIndices(allowAliasesToMultipleIndices)
            .allowClosedIndices(forbidClosedIndices == false)
            .removeThrottled(ignoreThrottled)
            .build();

        if (ignoreUnavailable) {
            opts.add(Option.IGNORE_UNAVAILABLE);
        }
        return new IndicesOptions(opts, wildcards, generalOptions);
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
        GeneralOptions generalOptions = GeneralOptions.parseParameters(ignoreThrottled, defaultSettings.generalOptions);

        // note that allowAliasesToMultipleIndices is not exposed, always true (only for internal use)
        return fromOptions(
            nodeBooleanValue(ignoreUnavailableString, "ignore_unavailable", defaultSettings.ignoreUnavailable()),
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
        WildcardOptions.toXContent(wildcardOptions, builder);
        builder.field("ignore_unavailable", ignoreUnavailable());
        GeneralOptions.toXContent(generalOptions, builder);
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
        WildcardOptions.Builder wildcardsBuilder = defaults == null ? null : new WildcardOptions.Builder(defaults.wildcardOptions());
        GeneralOptions.Builder generalBuilder = new GeneralOptions.Builder().removeThrottled(
            defaults != null && defaults.generalOptions().removeThrottled()
        );
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
                        wildcardsBuilder = new WildcardOptions.Builder();
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
                        wildcardsBuilder = new WildcardOptions.Builder();
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

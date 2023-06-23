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
import java.util.EnumSet;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeStringArrayValue;

/**
 * Controls how to deal with unavailable concrete indices (closed or missing), how wildcard expressions are expanded
 * to actual indices (all, closed or open indices) and how to deal with wildcard expressions that resolve to no indices.
 */
public record IndicesOptions(EnumSet<Option> options, EnumSet<WildcardStates> expandWildcards) implements ToXContentFragment {

    public enum WildcardStates {
        OPEN,
        CLOSED,
        HIDDEN;

        public static final EnumSet<WildcardStates> NONE = EnumSet.noneOf(WildcardStates.class);

        public static EnumSet<WildcardStates> parseParameter(Object value, EnumSet<WildcardStates> defaultStates) {
            if (value == null) {
                return defaultStates;
            }

            EnumSet<WildcardStates> states = EnumSet.noneOf(WildcardStates.class);
            String[] wildcards = nodeStringArrayValue(value);
            // TODO why do we let patterns like "none,all" or "open,none,closed" get used. The location of 'none' in the array changes the
            // meaning of the resulting value
            for (String wildcard : wildcards) {
                updateSetForValue(states, wildcard);
            }

            return states;
        }

        public static XContentBuilder toXContent(EnumSet<WildcardStates> states, XContentBuilder builder) throws IOException {
            if (states.isEmpty()) {
                builder.field("expand_wildcards", "none");
            } else if (states.containsAll(EnumSet.allOf(WildcardStates.class))) {
                builder.field("expand_wildcards", "all");
            } else {
                builder.field(
                    "expand_wildcards",
                    states.stream().map(state -> state.toString().toLowerCase(Locale.ROOT)).collect(Collectors.joining(","))
                );
            }
            return builder;
        }

        private static void updateSetForValue(EnumSet<WildcardStates> states, String wildcard) {
            switch (wildcard) {
                case "open" -> states.add(OPEN);
                case "closed" -> states.add(CLOSED);
                case "hidden" -> states.add(HIDDEN);
                case "none" -> states.clear();
                case "all" -> states.addAll(EnumSet.allOf(WildcardStates.class));
                default -> throw new IllegalArgumentException("No valid expand wildcard value [" + wildcard + "]");
            }
        }
    }

    public enum Option {
        IGNORE_UNAVAILABLE,
        IGNORE_ALIASES,
        ALLOW_NO_INDICES,
        FORBID_ALIASES_TO_MULTIPLE_INDICES,
        FORBID_CLOSED_INDICES,
        IGNORE_THROTTLED;

        public static final EnumSet<Option> NONE = EnumSet.noneOf(Option.class);
    }

    private static final DeprecationLogger DEPRECATION_LOGGER = DeprecationLogger.getLogger(IndicesOptions.class);
    private static final String IGNORE_THROTTLED_DEPRECATION_MESSAGE = "[ignore_throttled] parameter is deprecated "
        + "because frozen indices have been deprecated. Consider cold or frozen tiers in place of frozen indices.";

    public static final IndicesOptions STRICT_EXPAND_OPEN = new IndicesOptions(
        EnumSet.of(Option.ALLOW_NO_INDICES),
        EnumSet.of(WildcardStates.OPEN)
    );
    public static final IndicesOptions LENIENT_EXPAND_OPEN = new IndicesOptions(
        EnumSet.of(Option.ALLOW_NO_INDICES, Option.IGNORE_UNAVAILABLE),
        EnumSet.of(WildcardStates.OPEN)
    );
    public static final IndicesOptions LENIENT_EXPAND_OPEN_HIDDEN = new IndicesOptions(
        EnumSet.of(Option.ALLOW_NO_INDICES, Option.IGNORE_UNAVAILABLE),
        EnumSet.of(WildcardStates.OPEN, WildcardStates.HIDDEN)
    );
    public static final IndicesOptions LENIENT_EXPAND_OPEN_CLOSED = new IndicesOptions(
        EnumSet.of(Option.ALLOW_NO_INDICES, Option.IGNORE_UNAVAILABLE),
        EnumSet.of(WildcardStates.OPEN, WildcardStates.CLOSED)
    );
    public static final IndicesOptions LENIENT_EXPAND_OPEN_CLOSED_HIDDEN = new IndicesOptions(
        EnumSet.of(Option.ALLOW_NO_INDICES, Option.IGNORE_UNAVAILABLE),
        EnumSet.of(WildcardStates.OPEN, WildcardStates.CLOSED, WildcardStates.HIDDEN)
    );
    public static final IndicesOptions STRICT_EXPAND_OPEN_CLOSED = new IndicesOptions(
        EnumSet.of(Option.ALLOW_NO_INDICES),
        EnumSet.of(WildcardStates.OPEN, WildcardStates.CLOSED)
    );
    public static final IndicesOptions STRICT_EXPAND_OPEN_CLOSED_HIDDEN = new IndicesOptions(
        EnumSet.of(Option.ALLOW_NO_INDICES),
        EnumSet.of(WildcardStates.OPEN, WildcardStates.CLOSED, WildcardStates.HIDDEN)
    );
    public static final IndicesOptions STRICT_EXPAND_OPEN_FORBID_CLOSED = new IndicesOptions(
        EnumSet.of(Option.ALLOW_NO_INDICES, Option.FORBID_CLOSED_INDICES),
        EnumSet.of(WildcardStates.OPEN)
    );
    public static final IndicesOptions STRICT_EXPAND_OPEN_HIDDEN_FORBID_CLOSED = new IndicesOptions(
        EnumSet.of(Option.ALLOW_NO_INDICES, Option.FORBID_CLOSED_INDICES),
        EnumSet.of(WildcardStates.OPEN, WildcardStates.HIDDEN)
    );
    public static final IndicesOptions STRICT_EXPAND_OPEN_FORBID_CLOSED_IGNORE_THROTTLED = new IndicesOptions(
        EnumSet.of(Option.ALLOW_NO_INDICES, Option.FORBID_CLOSED_INDICES, Option.IGNORE_THROTTLED),
        EnumSet.of(WildcardStates.OPEN)
    );
    public static final IndicesOptions STRICT_SINGLE_INDEX_NO_EXPAND_FORBID_CLOSED = new IndicesOptions(
        EnumSet.of(Option.FORBID_ALIASES_TO_MULTIPLE_INDICES, Option.FORBID_CLOSED_INDICES),
        EnumSet.noneOf(WildcardStates.class)
    );
    public static final IndicesOptions STRICT_NO_EXPAND_FORBID_CLOSED = new IndicesOptions(
        EnumSet.of(Option.FORBID_CLOSED_INDICES),
        EnumSet.noneOf(WildcardStates.class)
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
        return options.contains(Option.ALLOW_NO_INDICES);
    }

    /**
     * @return Whether wildcard expressions should get expanded to open indices
     */
    public boolean expandWildcardsOpen() {
        return expandWildcards.contains(WildcardStates.OPEN);
    }

    /**
     * @return Whether wildcard expressions should get expanded to closed indices
     */
    public boolean expandWildcardsClosed() {
        return expandWildcards.contains(WildcardStates.CLOSED);
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
        return expandWildcards.contains(WildcardStates.HIDDEN);
    }

    /**
     * @return Whether execution on closed indices is allowed.
     */
    public boolean forbidClosedIndices() {
        return options.contains(Option.FORBID_CLOSED_INDICES);
    }

    /**
     * @return whether aliases pointing to multiple indices are allowed
     */
    public boolean allowAliasesToMultipleIndices() {
        // true is default here, for bw comp we keep the first 16 values
        // in the array same as before + the default value for the new flag
        return options.contains(Option.FORBID_ALIASES_TO_MULTIPLE_INDICES) == false;
    }

    /**
     * @return whether aliases should be ignored (when resolving a wildcard)
     */
    public boolean ignoreAliases() {
        return options.contains(Option.IGNORE_ALIASES);
    }

    /**
     * @return whether indices that are marked as throttled should be ignored
     */
    public boolean ignoreThrottled() {
        return options.contains(Option.IGNORE_THROTTLED);
    }

    /**
     * @return a copy of the {@link WildcardStates} that these indices options will expand to
     */
    public EnumSet<WildcardStates> expandWildcards() {
        return EnumSet.copyOf(expandWildcards);
    }

    /**
     * @return a copy of the {@link Option}s that these indices options will use
     */
    public EnumSet<Option> options() {
        return EnumSet.copyOf(options);
    }

    public void writeIndicesOptions(StreamOutput out) throws IOException {
        out.writeEnumSet(options);
        out.writeEnumSet(expandWildcards);
    }

    public static IndicesOptions readIndicesOptions(StreamInput in) throws IOException {
        EnumSet<Option> options = in.readEnumSet(Option.class);
        EnumSet<WildcardStates> states = in.readEnumSet(WildcardStates.class);
        return new IndicesOptions(options, states);
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
        final EnumSet<WildcardStates> wildcards = EnumSet.noneOf(WildcardStates.class);

        if (ignoreUnavailable) {
            opts.add(Option.IGNORE_UNAVAILABLE);
        }
        if (allowNoIndices) {
            opts.add(Option.ALLOW_NO_INDICES);
        }
        if (expandToOpenIndices) {
            wildcards.add(WildcardStates.OPEN);
        }
        if (expandToClosedIndices) {
            wildcards.add(WildcardStates.CLOSED);
        }
        if (expandToHiddenIndices) {
            wildcards.add(WildcardStates.HIDDEN);
        }
        if (allowAliasesToMultipleIndices == false) {
            opts.add(Option.FORBID_ALIASES_TO_MULTIPLE_INDICES);
        }
        if (forbidClosedIndices) {
            opts.add(Option.FORBID_CLOSED_INDICES);
        }
        if (ignoreAliases) {
            opts.add(Option.IGNORE_ALIASES);
        }
        if (ignoreThrottled) {
            opts.add(Option.IGNORE_THROTTLED);
        }
        return new IndicesOptions(opts, wildcards);
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

        EnumSet<WildcardStates> wildcards = WildcardStates.parseParameter(wildcardsString, defaultSettings.expandWildcards);

        // note that allowAliasesToMultipleIndices is not exposed, always true (only for internal use)
        return fromOptions(
            nodeBooleanValue(ignoreUnavailableString, "ignore_unavailable", defaultSettings.ignoreUnavailable()),
            nodeBooleanValue(allowNoIndicesString, "allow_no_indices", defaultSettings.allowNoIndices()),
            wildcards.contains(WildcardStates.OPEN),
            wildcards.contains(WildcardStates.CLOSED),
            wildcards.contains(WildcardStates.HIDDEN),
            defaultSettings.allowAliasesToMultipleIndices(),
            defaultSettings.forbidClosedIndices(),
            defaultSettings.ignoreAliases(),
            nodeBooleanValue(ignoreThrottled, "ignore_throttled", defaultSettings.ignoreThrottled())
        );
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startArray("expand_wildcards");
        for (WildcardStates expandWildcard : expandWildcards) {
            builder.value(expandWildcard.toString().toLowerCase(Locale.ROOT));
        }
        builder.endArray();
        builder.field("ignore_unavailable", ignoreUnavailable());
        builder.field("allow_no_indices", allowNoIndices());
        builder.field("ignore_throttled", ignoreThrottled());
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
        EnumSet<WildcardStates> wildcardStates = defaults == null ? null : defaults.expandWildcards();
        Boolean allowNoIndices = defaults == null ? null : defaults.allowNoIndices();
        Boolean ignoreUnavailable = defaults == null ? null : defaults.ignoreUnavailable();
        boolean ignoreThrottled = defaults == null ? false : defaults.ignoreThrottled();
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
                        wildcardStates = EnumSet.noneOf(WildcardStates.class);
                        while ((token = parser.nextToken()) != Token.END_ARRAY) {
                            if (token.isValue()) {
                                WildcardStates.updateSetForValue(wildcardStates, parser.text());
                            } else {
                                throw new ElasticsearchParseException(
                                    "expected values within array for " + EXPAND_WILDCARDS_FIELD.getPreferredName()
                                );
                            }
                        }
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
                        wildcardStates = EnumSet.noneOf(WildcardStates.class);
                        WildcardStates.updateSetForValue(wildcardStates, parser.text());
                    } else {
                        throw new ElasticsearchParseException("already parsed expand_wildcards");
                    }
                } else if (IGNORE_UNAVAILABLE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    ignoreUnavailable = parser.booleanValue();
                } else if (ALLOW_NO_INDICES_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    allowNoIndices = parser.booleanValue();
                } else if (IGNORE_THROTTLED_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    ignoreThrottled = parser.booleanValue();
                } else {
                    throw new ElasticsearchParseException(
                        "could not read indices options. unexpected index option [" + currentFieldName + "]"
                    );
                }
            } else {
                throw new ElasticsearchParseException("could not read indices options. unexpected object field [" + currentFieldName + "]");
            }
        }

        if (wildcardStates == null) {
            throw new ElasticsearchParseException("indices options xcontent did not contain " + EXPAND_WILDCARDS_FIELD.getPreferredName());
        }
        if (ignoreUnavailable == null) {
            throw new ElasticsearchParseException(
                "indices options xcontent did not contain " + IGNORE_UNAVAILABLE_FIELD.getPreferredName()
            );
        }
        if (allowNoIndices == null) {
            throw new ElasticsearchParseException("indices options xcontent did not contain " + ALLOW_NO_INDICES_FIELD.getPreferredName());
        }

        return IndicesOptions.fromOptions(
            ignoreUnavailable,
            allowNoIndices,
            wildcardStates.contains(WildcardStates.OPEN),
            wildcardStates.contains(WildcardStates.CLOSED),
            wildcardStates.contains(WildcardStates.HIDDEN),
            true,
            false,
            false,
            ignoreThrottled
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

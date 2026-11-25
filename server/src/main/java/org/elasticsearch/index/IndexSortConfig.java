/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.apache.lucene.search.BinarySortField;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.SortedSetSortField;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.DataStreamTimestampFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.RoutingFieldMapper;
import org.elasticsearch.index.mapper.SliceKeyFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.sort.SortOrder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;

/**
 * Holds all the information that is used to build the sort order of an index.
 *
 * The index sort settings are <b>final</b> and can be defined only at index creation.
 * These settings are divided in four lists that are merged during the initialization of this class:
 * <ul>
 *     <li>`index.sort.field`: the field or a list of field to use for the sort</li>
 *     <li>`index.sort.order` the {@link SortOrder} to use for the field or a list of {@link SortOrder}
 *          for each field defined in `index.sort.field`.
 *     </li>
 *     <li>`index.sort.mode`: the {@link MultiValueMode} to use for the field or a list of orders
 *          for each field defined in `index.sort.field`.
 *     </li>
 *     <li>`index.sort.missing`: the missing value to use for the field or a list of missing values
 *          for each field defined in `index.sort.field`
 *     </li>
 * </ul>
 *
 * Alternatively the sort can be configured with the single `index.sort` expression setting, for example
 * `MV_MAX(@timestamp) DESC NULLS LAST, host.name`. It is stored as is, and the four settings above then default to
 * empty rather than to the index mode defaults. It can't be combined with them.
 *
**/
public final class IndexSortConfig {

    private static final DeprecationLogger DEPRECATION_LOGGER = DeprecationLogger.getLogger(IndexSortConfig.class);

    /**
     * Expression that configures all index sort fields at once, as a comma-separated list of
     * {@code key [ASC|DESC] [NULLS FIRST|NULLS LAST]} entries where {@code key} is a field, {@code MV_MIN(field)} or
     * {@code MV_MAX(field)}. Like in ES|QL, {@code DESC} defaults to {@code NULLS FIRST}, unlike {@code index.sort.missing}, which
     * defaults to {@code _last} for both orders.
     * <p>
     * It is stored as is, so that reading back the settings returns what was written. When set, {@link IndexSortConfig} is built
     * from it directly and the {@code index.sort.*} settings are not consulted. It has no default to report, which keeps
     * {@code include_defaults} from emitting an {@code index.sort} value next to the nested {@code index.sort.*} settings.
     */
    public static final Setting<String> INDEX_SORT_SETTING = new Setting<>("index.sort", "", s -> s, new Setting.Validator<String>() {
        @Override
        public void validate(String value) {
            if (value.isEmpty()) {
                return;
            }
            parseSortExpression(value);
        }
    }, Setting.Property.IndexScope, Setting.Property.Final, Setting.Property.ServerlessPublic) {
        @Override
        public void diff(Settings.Builder builder, Settings source, Settings defaultSettings) {
            // Input only: never report the empty default, see the Javadoc above.
        }
    };

    /**
     * The list of field names
     */
    public static final Setting<List<String>> INDEX_SORT_FIELD_SETTING = Setting.stringListSettingWithDefaultProvider(
        "index.sort.field",
        IndexSortConfigDefaults::getDefaultSortFields,
        Setting.Property.IndexScope,
        Setting.Property.Final,
        Setting.Property.ServerlessPublic
    );

    /**
     * The {@link SortOrder} for each specified sort field (ie. <b>asc</b> or <b>desc</b>).
     */
    public static final Setting<List<SortOrder>> INDEX_SORT_ORDER_SETTING = Setting.listSetting(
        "index.sort.order",
        IndexSortConfigDefaults::getDefaultSortOrder,
        IndexSortConfig::parseOrderMode,
        Setting.Property.IndexScope,
        Setting.Property.Final,
        Setting.Property.ServerlessPublic
    );

    /**
     * The {@link MultiValueMode} for each specified sort field (ie. <b>max</b> or <b>min</b>).
     */
    public static final Setting<List<MultiValueMode>> INDEX_SORT_MODE_SETTING = Setting.listSetting(
        "index.sort.mode",
        IndexSortConfigDefaults::getDefaultSortMode,
        IndexSortConfig::parseMultiValueMode,
        Setting.Property.IndexScope,
        Setting.Property.Final,
        Setting.Property.ServerlessPublic
    );

    /**
     * The missing value for each specified sort field (ie. <b>_first</b> or <b>_last</b>)
     */
    public static final Setting<List<String>> INDEX_SORT_MISSING_SETTING = Setting.listSetting(
        "index.sort.missing",
        IndexSortConfigDefaults::getDefaultSortMissing,
        IndexSortConfig::validateMissingValue,
        Setting.Property.IndexScope,
        Setting.Property.Final,
        Setting.Property.ServerlessPublic
    );

    public static class IndexSortConfigDefaults {
        public record SortDefault(List<String> fields, List<String> order, List<String> mode, List<String> missing) {
            public SortDefault {
                assert fields.size() == order.size();
                assert fields.size() == mode.size();
                assert fields.size() == missing.size();
            }
        }

        public static final SortDefault NO_SORT, TIME_SERIES_SORT, TIMESTAMP_SORT, HOSTNAME_TIMESTAMP_SORT, HOSTNAME_TIMESTAMP_BWC_SORT,
            MESSAGE_PATTERN_TIMESTAMP_SORT, HOSTNAME_MESSAGE_PATTERN_TIMESTAMP_SORT;

        static {
            NO_SORT = new SortDefault(Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList());

            TIME_SERIES_SORT = new SortDefault(
                List.of(TimeSeriesIdFieldMapper.NAME, DataStreamTimestampFieldMapper.DEFAULT_PATH),
                List.of("asc", "desc"),
                List.of("min", "max"),
                List.of("_last", "_last")
            );

            TIMESTAMP_SORT = new SortDefault(
                List.of(DataStreamTimestampFieldMapper.DEFAULT_PATH),
                List.of("desc"),
                List.of("max"),
                List.of("_last")
            );

            HOSTNAME_TIMESTAMP_SORT = new SortDefault(
                List.of(IndexMode.HOST_NAME, DataStreamTimestampFieldMapper.DEFAULT_PATH),
                List.of("asc", "desc"),
                List.of("min", "max"),
                List.of("_last", "_last")
            );

            MESSAGE_PATTERN_TIMESTAMP_SORT = new SortDefault(
                List.of("message.template_id", DataStreamTimestampFieldMapper.DEFAULT_PATH),
                List.of("asc", "desc"),
                List.of("min", "max"),
                List.of("_last", "_last")
            );

            HOSTNAME_MESSAGE_PATTERN_TIMESTAMP_SORT = new SortDefault(
                List.of(IndexMode.HOST_NAME, "message.template_id", DataStreamTimestampFieldMapper.DEFAULT_PATH),
                List.of("asc", "asc", "desc"),
                List.of("min", "min", "max"),
                List.of("_last", "_last", "_last")
            );

            // Older indexes use ascending ordering for host name and timestamp.
            HOSTNAME_TIMESTAMP_BWC_SORT = new SortDefault(
                List.of(IndexMode.HOST_NAME, DataStreamTimestampFieldMapper.DEFAULT_PATH),
                List.of("asc", "asc"),
                List.of("min", "min"),
                List.of("_last", "_last")
            );

        }

        static SortDefault getSortDefault(Settings settings) {
            if (settings.isEmpty()) {
                return NO_SORT;
            }

            // The index.sort expression replaces the index.sort.* settings, so they have no index mode defaults to report.
            if (settings.get(INDEX_SORT_SETTING.getKey(), "").isEmpty() == false) {
                return NO_SORT;
            }

            // Can't use IndexSettings.MODE.get(settings) here because the validation logic for IndexSettings.MODE uses the default value
            // of index.sort.*, which causes infinite recursion (since we're already in the default value provider for those settings).
            // So we need to get the mode while bypassing the validation.
            String indexMode = settings.get(IndexSettings.MODE.getKey());
            if (indexMode != null) {
                indexMode = indexMode.toLowerCase(Locale.ROOT);
            }

            if (IndexMode.isTsdbName(indexMode)) {
                return TIME_SERIES_SORT;
            } else if (IndexMode.LOGSDB.getName().equals(indexMode) || IndexMode.LOGSDB_COLUMNAR.getName().equals(indexMode)) {
                var version = IndexMetadata.SETTING_INDEX_VERSION_CREATED.get(settings);
                if (version.onOrAfter(IndexVersions.LOGSB_OPTIONAL_SORTING_ON_HOST_NAME)
                    || version.between(
                        IndexVersions.LOGSB_OPTIONAL_SORTING_ON_HOST_NAME_BACKPORT,
                        IndexVersions.UPGRADE_TO_LUCENE_10_0_0
                    )) {

                    boolean sortOnHostName = settings.getAsBoolean(IndexSettings.LOGSDB_SORT_ON_HOST_NAME.getKey(), false);
                    boolean sortOnMessageTemplate = settings.getAsBoolean(IndexSettings.LOGSDB_SORT_ON_MESSAGE_TEMPLATE.getKey(), false);
                    if (sortOnHostName && sortOnMessageTemplate) {
                        return HOSTNAME_MESSAGE_PATTERN_TIMESTAMP_SORT;
                    } else if (sortOnHostName) {
                        return HOSTNAME_TIMESTAMP_SORT;
                    } else if (sortOnMessageTemplate) {
                        return MESSAGE_PATTERN_TIMESTAMP_SORT;
                    } else {
                        return TIMESTAMP_SORT;
                    }
                } else {
                    return HOSTNAME_TIMESTAMP_BWC_SORT;
                }
            }

            return NO_SORT;
        }

        public static List<String> getDefaultSortFields(Settings settings) {
            return getSortDefault(settings).fields();
        }

        public static List<String> getDefaultSortOrder(Settings settings) {
            if (settings.hasValue(INDEX_SORT_FIELD_SETTING.getKey()) == false) {
                return getSortDefault(settings).order();
            }

            List<String> sortFields = settings.getAsList(INDEX_SORT_FIELD_SETTING.getKey());
            List<String> order = new ArrayList<>(sortFields.size());
            for (int i = 0; i < sortFields.size(); ++i) {
                order.add("asc");
            }
            return order;
        }

        public static List<String> getDefaultSortMode(Settings settings) {
            if (settings.hasValue(INDEX_SORT_FIELD_SETTING.getKey()) == false) {
                return getSortDefault(settings).mode();
            }

            List<String> sortFields = settings.getAsList(INDEX_SORT_FIELD_SETTING.getKey());
            List<String> sortOrder = settings.getAsList(INDEX_SORT_ORDER_SETTING.getKey(), null);

            List<String> mode = new ArrayList<>(sortFields.size());
            for (int i = 0; i < sortFields.size(); ++i) {
                if (sortOrder != null && sortOrder.get(i).equals(SortOrder.DESC.toString())) {
                    mode.add("max");
                } else {
                    mode.add("min");
                }
            }
            return mode;
        }

        public static List<String> getDefaultSortMissing(Settings settings) {
            if (settings.hasValue(INDEX_SORT_FIELD_SETTING.getKey()) == false) {
                return getSortDefault(settings).missing();
            }

            List<String> sortFields = settings.getAsList(INDEX_SORT_FIELD_SETTING.getKey());
            List<String> missing = new ArrayList<>(sortFields.size());
            for (int i = 0; i < sortFields.size(); ++i) {
                // _last is the default per IndexFieldData.XFieldComparatorSource.Nested#sortMissingLast
                missing.add("_last");
            }
            return missing;
        }
    }

    /**
     * One entry of an {@link #INDEX_SORT_SETTING} expression, with the order, multi-value mode and missing value already resolved
     * to their ES|QL defaults.
     */
    public record SortEntry(String field, SortOrder order, MultiValueMode mode, String missing) {}

    /**
     * Parses an {@link #INDEX_SORT_SETTING} expression into its sort entries.
     */
    public static List<SortEntry> parseSortExpression(String expression) {
        if (expression.isBlank()) {
            throw new IllegalArgumentException("setting [" + INDEX_SORT_SETTING.getKey() + "] must not be blank");
        }
        return new SortExpressionParser(expression).parse();
    }

    /**
     * Hand written parser for the {@link #INDEX_SORT_SETTING} syntax, since the ES|QL grammar lives in an x-pack plugin that
     * {@code server} can't depend on. Accepts a strict subset of the ES|QL {@code SORT} syntax, so the same text keeps its
     * meaning when it is later parsed by ES|QL. Anything beyond the supported subset is rejected.
     */
    private static final class SortExpressionParser {
        /** Each dot-separated part of an unquoted name follows the ES|QL UNQUOTED_IDENTIFIER rule, anything else needs backticks. */
        private static final Pattern UNQUOTED_PART = Pattern.compile("[a-z][a-z0-9_]*|[_@][a-z0-9_]+", Pattern.CASE_INSENSITIVE);
        /** Keywords in ES|QL expressions, which ES|QL never reads as field names when unquoted, including as any part of a dotted name. */
        private static final Set<String> RESERVED_WORDS = Set.of(
            "and",
            "asc",
            "by",
            "desc",
            "false",
            "first",
            "in",
            "is",
            "last",
            "like",
            "not",
            "null",
            "nulls",
            "on",
            "or",
            "rlike",
            "true",
            "where",
            "with"
        );

        /**
         * Identity of a sort entry, used to detect duplicates. Keeps the function and field separate so a quoted field that is
         * literally named {@code MV_MAX(a)} is not confused with {@code MV_MAX} applied to field {@code a}.
         *
         * @param function the upper-cased multi-value function, or {@code null} when the field is sorted directly
         */
        private record SortKey(String function, String field) {
            @Override
            public String toString() {
                return function == null ? field : function + "(" + field + ")";
            }
        }

        private final String expression;
        private int pos = 0;

        SortExpressionParser(String expression) {
            this.expression = expression;
        }

        List<SortEntry> parse() {
            List<SortEntry> entries = new ArrayList<>();
            Set<SortKey> seenKeys = new HashSet<>();
            while (true) {
                int entryStart = pos;
                String mode = null;
                String function = null;
                skipWhitespace();
                if (atEnd() || peek() == ',') {
                    throw error("contains an empty sort entry");
                }
                boolean quoted = peek() == '`';
                String field = readFieldName();
                if (quoted == false) {
                    skipWhitespace();
                    if (isAt('(')) {
                        function = field.toUpperCase(Locale.ROOT);
                        mode = switch (function) {
                            case "MV_MIN" -> "min";
                            case "MV_MAX" -> "max";
                            default -> throw error("contains unsupported function [" + field + "], expected one of [MV_MIN, MV_MAX]");
                        };
                        pos++;
                        skipWhitespace();
                        field = readFieldName();
                        skipWhitespace();
                        if (isAt(')') == false) {
                            throw error("is missing a closing parenthesis for [" + expression.substring(entryStart, pos).trim() + "]");
                        }
                        pos++;
                    }
                }
                SortKey key = new SortKey(function, field);
                if (seenKeys.add(key) == false) {
                    throw error("contains sort key [" + key + "] more than once");
                }

                String order = null;
                String nulls = null;
                while (true) {
                    skipWhitespace();
                    if (atEnd() || peek() == ',') {
                        break;
                    }
                    String word = readWord();
                    switch (word.toUpperCase(Locale.ROOT)) {
                        case "ASC", "DESC" -> {
                            if (order != null || nulls != null) {
                                throw error("has an unexpected [" + word + "], the order must come once and before NULLS");
                            }
                            order = word.toLowerCase(Locale.ROOT);
                        }
                        case "NULLS" -> {
                            if (nulls != null) {
                                throw error("specifies NULLS more than once");
                            }
                            skipWhitespace();
                            String position = atEnd() ? "" : readWord();
                            nulls = switch (position.toUpperCase(Locale.ROOT)) {
                                case "FIRST" -> "_first";
                                case "LAST" -> "_last";
                                default -> throw error("expects FIRST or LAST after NULLS but found [" + position + "]");
                            };
                        }
                        default -> throw error(
                            "contains unexpected token [" + word + "], expected one of [ASC, DESC, NULLS FIRST, NULLS LAST]"
                        );
                    }
                }
                if (order == null) {
                    order = "asc";
                }
                if (mode == null) {
                    mode = order.equals("desc") ? "max" : "min";
                }
                if (nulls == null) {
                    // Like ES|QL, null sorts as larger than any value, so DESC puts nulls first.
                    nulls = order.equals("desc") ? "_first" : "_last";
                }
                entries.add(new SortEntry(field, parseOrderMode(order), parseMultiValueMode(mode), nulls));

                if (atEnd()) {
                    return entries;
                }
                pos++; // the comma
            }
        }

        /** Reads a field name, either unquoted or in backticks where a doubled backtick is a literal one, as in ES|QL. */
        private String readFieldName() {
            if (atEnd()) {
                throw error("is missing a field name");
            }
            if (peek() == '`') {
                StringBuilder name = new StringBuilder();
                pos++;
                while (true) {
                    if (atEnd()) {
                        throw error("has an unterminated backtick quoted field name");
                    }
                    char c = expression.charAt(pos++);
                    if (c != '`') {
                        name.append(c);
                    } else if (isAt('`')) {
                        name.append('`');
                        pos++;
                    } else {
                        break;
                    }
                }
                if (name.isEmpty()) {
                    throw error("contains an empty quoted field name");
                }
                return name.toString();
            }
            String name = readWord();
            for (String part : name.split("\\.", -1)) {
                if (UNQUOTED_PART.matcher(part).matches() == false) {
                    throw error("has invalid unquoted field name [" + name + "], use backticks to quote it");
                }
                if (RESERVED_WORDS.contains(part.toLowerCase(Locale.ROOT))) {
                    throw error("has reserved word [" + part + "] in field name [" + name + "], use backticks to quote it");
                }
            }
            return name;
        }

        /** Reads a run of characters that are not whitespace, a comma, a parenthesis or a backtick. */
        private String readWord() {
            int start = pos;
            while (atEnd() == false && isWordChar(peek())) {
                pos++;
            }
            if (start == pos) {
                throw error("has unexpected character [" + peek() + "]");
            }
            return expression.substring(start, pos);
        }

        private static boolean isWordChar(char c) {
            return Character.isWhitespace(c) == false && c != ',' && c != '(' && c != ')' && c != '`';
        }

        private void skipWhitespace() {
            while (atEnd() == false && Character.isWhitespace(peek())) {
                pos++;
            }
        }

        private boolean atEnd() {
            return pos >= expression.length();
        }

        private boolean isAt(char c) {
            return atEnd() == false && peek() == c;
        }

        private char peek() {
            return expression.charAt(pos);
        }

        private IllegalArgumentException error(String message) {
            return new IllegalArgumentException("setting [" + INDEX_SORT_SETTING.getKey() + "] " + message + " in [" + expression + "]");
        }
    }

    private static String validateMissingValue(String missing) {
        if ("_last".equals(missing) == false && "_first".equals(missing) == false) {
            throw new IllegalArgumentException("Illegal missing value:[" + missing + "], " + "must be one of [_last, _first]");
        }
        return missing;
    }

    private static SortOrder parseOrderMode(String value) {
        try {
            return SortOrder.fromString(value);
        } catch (Exception e) {
            throw new IllegalArgumentException("Illegal sort order:" + value);
        }
    }

    private static MultiValueMode parseMultiValueMode(String value) {
        MultiValueMode mode = MultiValueMode.fromString(value);
        if (mode != MultiValueMode.MAX && mode != MultiValueMode.MIN) {
            throw new IllegalArgumentException(
                "Illegal index sort mode:[" + mode + "], " + "must be one of [" + MultiValueMode.MAX + ", " + MultiValueMode.MIN + "]"
            );
        }
        return mode;
    }

    private static void checkSizeMismatch(String firstKey, List<?> first, String secondKey, List<?> second) {
        if (first.size() != second.size()) {
            throw new IllegalArgumentException(firstKey + ":" + first + " " + secondKey + ":" + second + ", size mismatch");
        }
    }

    /**
     * The sort fields of an index, from {@link #INDEX_SORT_SETTING} if it is set and from {@link #INDEX_SORT_FIELD_SETTING}
     * (or its default) otherwise, one per sort entry so a field can repeat, for example {@code MV_MIN(a), MV_MAX(a)}. Doesn't
     * include the slice key.
     */
    public static List<String> sortFields(Settings settings) {
        String expression = settings.get(INDEX_SORT_SETTING.getKey(), "");
        if (expression.isEmpty()) {
            return INDEX_SORT_FIELD_SETTING.get(settings);
        }
        return parseSortExpression(expression).stream().map(SortEntry::field).toList();
    }

    private static void validateSortSettings(Settings settings) {
        boolean hasSortExpression = settings.get(INDEX_SORT_SETTING.getKey(), "").isEmpty() == false;
        if (hasSortExpression) {
            for (Setting<?> setting : new Setting<?>[] {
                INDEX_SORT_FIELD_SETTING,
                INDEX_SORT_ORDER_SETTING,
                INDEX_SORT_MODE_SETTING,
                INDEX_SORT_MISSING_SETTING }) {
                if (setting.exists(settings)) {
                    throw new IllegalArgumentException(
                        "setting [" + INDEX_SORT_SETTING.getKey() + "] cannot be combined with [" + setting.getKey() + "]"
                    );
                }
            }
        }

        if (INDEX_SORT_FIELD_SETTING.exists(settings) == false) {
            for (Setting<?> setting : new Setting<?>[] { INDEX_SORT_ORDER_SETTING, INDEX_SORT_MODE_SETTING, INDEX_SORT_MISSING_SETTING }) {
                if (setting.exists(settings)) {
                    throw new IllegalArgumentException(
                        "setting [" + setting.getKey() + "] requires [" + INDEX_SORT_FIELD_SETTING.getKey() + "] to be configured"
                    );
                }
            }
        }

        if (IndexSettings.SLICE_ENABLED.get(settings) && (INDEX_SORT_FIELD_SETTING.exists(settings) || hasSortExpression)) {
            for (String field : sortFields(settings)) {
                if (field.equals(RoutingFieldMapper.NAME)
                    || field.equals(SliceIndexing.FIELD_NAME)
                    || field.equals(SliceIndexing.SLICE_KEY_FIELD_NAME)
                    || field.equals(SliceIndexing.SLICE_HASH_FIELD_NAME)) {
                    throw new IllegalArgumentException(
                        "setting ["
                            + (hasSortExpression ? INDEX_SORT_SETTING.getKey() : INDEX_SORT_FIELD_SETTING.getKey())
                            + "] must not contain ["
                            + RoutingFieldMapper.NAME
                            + "], ["
                            + SliceIndexing.FIELD_NAME
                            + "], ["
                            + SliceIndexing.SLICE_KEY_FIELD_NAME
                            + "] or ["
                            + SliceIndexing.SLICE_HASH_FIELD_NAME
                            + "] when ["
                            + IndexSettings.SLICE_ENABLED.getKey()
                            + "] is true"
                    );
                }
            }
        }

        if (hasSortExpression) {
            return;
        }

        List<String> fields = INDEX_SORT_FIELD_SETTING.get(settings);

        var order = INDEX_SORT_ORDER_SETTING.get(settings);
        checkSizeMismatch(INDEX_SORT_FIELD_SETTING.getKey(), fields, INDEX_SORT_ORDER_SETTING.getKey(), order);

        var mode = INDEX_SORT_MODE_SETTING.get(settings);
        checkSizeMismatch(INDEX_SORT_FIELD_SETTING.getKey(), fields, INDEX_SORT_MODE_SETTING.getKey(), mode);

        var missing = INDEX_SORT_MISSING_SETTING.get(settings);
        checkSizeMismatch(INDEX_SORT_FIELD_SETTING.getKey(), fields, INDEX_SORT_MISSING_SETTING.getKey(), missing);
    }

    // visible for tests
    final FieldSortSpec[] sortSpecs;
    private final IndexVersion indexCreatedVersion;
    private final String indexName;
    private final IndexMode indexMode;
    private final boolean sliceEnabled;

    public IndexSortConfig(IndexSettings indexSettings) {
        final Settings settings = indexSettings.getSettings();
        this.indexCreatedVersion = indexSettings.getIndexVersionCreated();
        this.indexName = indexSettings.getIndex().getName();
        this.indexMode = indexSettings.getMode();
        this.sliceEnabled = indexSettings.isSliceEnabled();

        validateSortSettings(settings);

        List<String> fields;
        List<SortOrder> orders;
        List<MultiValueMode> modes;
        List<String> missingValues;
        String expression = settings.get(INDEX_SORT_SETTING.getKey(), "");
        if (expression.isEmpty()) {
            fields = new ArrayList<>(INDEX_SORT_FIELD_SETTING.get(settings));
            orders = new ArrayList<>(INDEX_SORT_ORDER_SETTING.get(settings));
            modes = new ArrayList<>(INDEX_SORT_MODE_SETTING.get(settings));
            missingValues = new ArrayList<>(INDEX_SORT_MISSING_SETTING.get(settings));
        } else {
            List<SortEntry> entries = parseSortExpression(expression);
            fields = new ArrayList<>(entries.stream().map(SortEntry::field).toList());
            orders = new ArrayList<>(entries.stream().map(SortEntry::order).toList());
            modes = new ArrayList<>(entries.stream().map(SortEntry::mode).toList());
            missingValues = new ArrayList<>(entries.stream().map(SortEntry::missing).toList());
        }
        if (indexSettings.isSliceEnabled()) {
            prependSliceKeySort(fields, orders, modes, missingValues);
        }
        sortSpecs = fields.stream().map(FieldSortSpec::new).toArray(FieldSortSpec[]::new);
        for (int i = 0; i < sortSpecs.length; i++) {
            sortSpecs[i].order = orders.get(i);
        }
        for (int i = 0; i < sortSpecs.length; i++) {
            sortSpecs[i].mode = modes.get(i);
        }
        for (int i = 0; i < sortSpecs.length; i++) {
            sortSpecs[i].missingValue = missingValues.get(i);
        }
    }

    /**
     * Slice-enabled indices are sorted on the hash-prefixed slice key first, ascending with missing values (tombstones)
     * last, so every slice occupies a contiguous doc-id range laid out by hash prefix.
     */
    private static void prependSliceKeySort(
        List<String> fields,
        List<SortOrder> orders,
        List<MultiValueMode> modes,
        List<String> missingValues
    ) {
        fields.addFirst(SliceIndexing.SLICE_KEY_FIELD_NAME);
        orders.addFirst(SortOrder.ASC);
        modes.addFirst(MultiValueMode.MIN);
        missingValues.addFirst("_last");
    }

    /**
     * Returns true if the index should be sorted
     */
    public boolean hasIndexSort() {
        return sortSpecs.length > 0;
    }

    public boolean hasPrimarySortOnField(String field) {
        return sortSpecs.length > 0 && sortSpecs[0].field.equals(field);
    }

    public boolean containsDescendingTimestampSort() {
        for (FieldSortSpec sort : sortSpecs) {
            if (sort.order == SortOrder.DESC && sort.field.equals("@timestamp")) {
                return true;
            }
        }
        return false;
    }

    /**
     * Builds the {@link Sort} order from the settings for this index
     * or returns null if this index has no sort.
     */
    public Sort buildIndexSort(
        Function<String, MappedFieldType> fieldTypeLookup,
        BiFunction<MappedFieldType, Supplier<SearchLookup>, IndexFieldData<?>> fieldDataLookup
    ) {
        if (hasIndexSort() == false) {
            return null;
        }

        final SortField[] sortFields = new SortField[sortSpecs.length];
        for (int i = 0; i < sortSpecs.length; i++) {
            FieldSortSpec sortSpec = sortSpecs[i];
            // This is only necessary because of the logic of "{}" empty mappings.
            // metadata fields are not eagerly loaded as mapped fields, so if we have slice enabled, and no mapped fields, we get an empty
            // lookup and must resolve the slice key field type directly.
            final MappedFieldType ft = Optional.ofNullable(fieldTypeLookup.apply(sortSpec.field))
                .orElseGet(
                    () -> sliceEnabled && SliceIndexing.SLICE_KEY_FIELD_NAME.equals(sortSpec.field) ? SliceKeyFieldMapper.FIELD_TYPE : null
                );
            if (ft == null) {
                String err = "unknown index sort field:[" + sortSpec.field + "]";
                if (this.indexMode.isTsdb()) {
                    err += " required by [" + IndexSettings.MODE.getKey() + "=" + this.indexMode.getName() + "]";
                }
                throw new IllegalArgumentException(err);
            }
            if (Objects.equals(ft.name(), sortSpec.field) == false) {
                if (this.indexCreatedVersion.onOrAfter(IndexVersions.V_7_13_0)) {
                    throw new IllegalArgumentException("Cannot use alias [" + sortSpec.field + "] as an index sort field");
                } else {
                    DEPRECATION_LOGGER.warn(
                        DeprecationCategory.MAPPINGS,
                        "index-sort-aliases",
                        "Index sort for index ["
                            + indexName
                            + "] defined on field ["
                            + sortSpec.field
                            + "] which resolves to field ["
                            + ft.name()
                            + "]. "
                            + "You will not be able to define an index sort over aliased fields in new indexes"
                    );
                }
            }
            boolean reverse = sortSpec.order == null ? false : (sortSpec.order == SortOrder.DESC);
            MultiValueMode mode = sortSpec.mode;
            if (mode == null) {
                mode = reverse ? MultiValueMode.MAX : MultiValueMode.MIN;
            }
            IndexFieldData<?> fieldData;
            try {
                fieldData = fieldDataLookup.apply(ft, () -> {
                    throw new UnsupportedOperationException("index sorting not supported on runtime field [" + ft.name() + "]");
                });
            } catch (Exception e) {
                throw new IllegalArgumentException("docvalues not found for index sort field:[" + sortSpec.field + "]", e);
            }
            if (fieldData == null) {
                throw new IllegalArgumentException("docvalues not found for index sort field:[" + sortSpec.field + "]");
            }
            sortFields[i] = fieldData.indexSort(this.indexCreatedVersion, sortSpec.missingValue, mode, reverse);
            validateIndexSortField(sortFields[i]);
        }
        return new Sort(sortFields);
    }

    private static void validateIndexSortField(SortField sortField) {
        SortField.Type type = getSortFieldType(sortField);
        // Lucene rejects sort fields without an IndexSorter (e.g. custom comparator sources) when the IndexWriter is opened
        if (ALLOWED_INDEX_SORT_TYPES.contains(type) == false || sortField.getIndexSorter() == null) {
            throw new IllegalArgumentException("invalid index sort field:[" + sortField.getField() + "]");
        }
    }

    public boolean hasSortOnField(final String fieldName) {
        for (FieldSortSpec sortSpec : sortSpecs) {
            if (sortSpec.field.equals(fieldName)) {
                return true;
            }
        }
        return false;
    }

    public static class FieldSortSpec {
        final String field;
        SortOrder order;
        MultiValueMode mode;
        String missingValue;

        FieldSortSpec(String field) {
            this.field = field;
        }

        public String getField() {
            return field;
        }

        public SortOrder getOrder() {
            return order;
        }
    }

    /** We only allow index sorting on these types */
    private static final EnumSet<SortField.Type> ALLOWED_INDEX_SORT_TYPES = EnumSet.of(
        SortField.Type.STRING,
        SortField.Type.LONG,
        SortField.Type.INT,
        SortField.Type.DOUBLE,
        SortField.Type.FLOAT
    );

    public static SortField.Type getSortFieldType(SortField sortField) {
        if (sortField instanceof SortedSetSortField) {
            return SortField.Type.STRING;
        } else if (sortField instanceof BinarySortField) {
            return SortField.Type.STRING;
        } else if (sortField instanceof SortedNumericSortField) {
            return ((SortedNumericSortField) sortField).getNumericType();
        } else if (sortField.getComparatorSource() instanceof IndexFieldData.XFieldComparatorSource fcs) {
            return fcs.sortType();
        } else {
            return sortField.getType();
        }
    }
}

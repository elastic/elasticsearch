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
import org.elasticsearch.core.Nullable;
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
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

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
     * Expression that configures all index sort fields at once, in the ES|QL {@code SORT} syntax that the {@link IndexSortParser}
     * reads: a comma-separated list of {@code key [ASC|DESC] [NULLS FIRST|NULLS LAST]} entries where {@code key} is a field,
     * {@code MV_MIN(field)}, {@code MV_MAX(field)} or an expression that computes the key. Like in ES|QL, {@code DESC} defaults to
     * {@code NULLS FIRST}, unlike {@code index.sort.missing}, which defaults to {@code _last} for both orders.
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
            if (value.isBlank()) {
                throw new IllegalArgumentException("setting [" + INDEX_SORT_SETTING.getKey() + "] must not be blank");
            }
            // Without the parser, the index fails to be created instead
            if (IndexSortParser.get() != null) {
                parseSortExpression(value);
            }
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
     *
     * @param field the field to sort on, or {@code null} for a computed key
     * @param computedKey the text of a key computed by an expression, anything but a field or {@code MV_MIN} or {@code MV_MAX} of
     *                    one, such as {@code DATE_TRUNC(1 hour, @timestamp)}
     * @param computedKeyFields the fields that the computed key reads
     */
    public record SortEntry(
        @Nullable String field,
        SortOrder order,
        MultiValueMode mode,
        String missing,
        @Nullable String computedKey,
        Set<String> computedKeyFields
    ) {
        public static SortEntry onField(String field, SortOrder order, MultiValueMode mode, String missing) {
            return new SortEntry(field, order, mode, missing, null, Set.of());
        }

        public static SortEntry onComputedKey(String key, SortOrder order, String missing, Set<String> fields) {
            return new SortEntry(null, order, order == SortOrder.DESC ? MultiValueMode.MAX : MultiValueMode.MIN, missing, key, fields);
        }
    }

    /**
     * Parses an {@link #INDEX_SORT_SETTING} expression into its sort entries, with the {@link IndexSortParser}.
     */
    public static List<SortEntry> parseSortExpression(String expression) {
        return parser().parse(expression);
    }

    private static IndexSortParser parser() {
        IndexSortParser parser = IndexSortParser.get();
        if (parser == null) {
            throw new IllegalArgumentException("setting [" + INDEX_SORT_SETTING.getKey() + "] requires the ES|QL plugin");
        }
        return parser;
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
     * include the slice key or computed keys, see {@link #computedKeyFields()}.
     */
    public static List<String> sortFields(Settings settings) {
        String expression = settings.get(INDEX_SORT_SETTING.getKey(), "");
        if (expression.isEmpty()) {
            return INDEX_SORT_FIELD_SETTING.get(settings);
        }
        return parseSortExpression(expression).stream().map(SortEntry::field).filter(Objects::nonNull).toList();
    }

    /**
     * Checks the sort settings together. Only checks the fields of an {@link #INDEX_SORT_SETTING} expression if the
     * {@link IndexSortParser} is loaded, see {@link IndexSettings#getIndexSortConfig()}.
     */
    static void validateSortSettings(Settings settings) {
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

        if (IndexSettings.SLICE_ENABLED.get(settings)
            && (INDEX_SORT_FIELD_SETTING.exists(settings) || (hasSortExpression && IndexSortParser.get() != null))) {
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
    private final IndexSettings indexSettings;
    private final Set<String> computedKeyFields;
    private final IndexVersion indexCreatedVersion;
    private final String indexName;
    private final IndexMode indexMode;
    private final boolean sliceEnabled;

    public IndexSortConfig(IndexSettings indexSettings) {
        this.indexSettings = indexSettings;
        final Settings settings = indexSettings.getSettings();
        this.indexCreatedVersion = indexSettings.getIndexVersionCreated();
        this.indexName = indexSettings.getIndex().getName();
        this.indexMode = indexSettings.getMode();
        this.sliceEnabled = indexSettings.isSliceEnabled();

        List<String> fields;
        List<SortOrder> orders;
        List<MultiValueMode> modes;
        List<String> missingValues;
        List<String> computedKeys;
        String expression = settings.get(INDEX_SORT_SETTING.getKey(), "");
        if (expression.isEmpty()) {
            fields = new ArrayList<>(INDEX_SORT_FIELD_SETTING.get(settings));
            orders = new ArrayList<>(INDEX_SORT_ORDER_SETTING.get(settings));
            modes = new ArrayList<>(INDEX_SORT_MODE_SETTING.get(settings));
            missingValues = new ArrayList<>(INDEX_SORT_MISSING_SETTING.get(settings));
            computedKeys = new ArrayList<>(Collections.nCopies(fields.size(), null));
            computedKeyFields = Set.of();
        } else {
            List<SortEntry> entries = parseSortExpression(expression);
            // A computed key has no field, its spec is named after its text
            fields = new ArrayList<>(entries.stream().map(e -> e.computedKey() != null ? e.computedKey() : e.field()).toList());
            orders = new ArrayList<>(entries.stream().map(SortEntry::order).toList());
            modes = new ArrayList<>(entries.stream().map(SortEntry::mode).toList());
            missingValues = new ArrayList<>(entries.stream().map(SortEntry::missing).toList());
            computedKeys = new ArrayList<>(entries.stream().map(SortEntry::computedKey).toList());
            computedKeyFields = entries.stream().flatMap(e -> e.computedKeyFields().stream()).collect(Collectors.toUnmodifiableSet());
            if (computedKeys.stream().anyMatch(Objects::nonNull) && indexCreatedVersion.before(IndexVersions.INDEX_SORT_COMPUTED_KEYS)) {
                throw new IllegalArgumentException(
                    "setting ["
                        + INDEX_SORT_SETTING.getKey()
                        + "] can't have computed keys on an index created before all nodes support them"
                );
            }
        }
        if (indexSettings.isSliceEnabled()) {
            prependSliceKeySort(fields, orders, modes, missingValues);
            computedKeys.addFirst(null);
        }
        sortSpecs = fields.stream().map(FieldSortSpec::new).toArray(FieldSortSpec[]::new);
        for (int i = 0; i < sortSpecs.length; i++) {
            sortSpecs[i].order = orders.get(i);
            sortSpecs[i].computedKey = computedKeys.get(i);
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
        return sortSpecs.length > 0 && sortSpecs[0].sortsOnField(field);
    }

    public boolean containsDescendingTimestampSort() {
        for (FieldSortSpec sort : sortSpecs) {
            if (sort.order == SortOrder.DESC && sort.sortsOnField("@timestamp")) {
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
            if (sortSpec.computedKey != null) {
                boolean reverse = sortSpec.order == SortOrder.DESC;
                boolean nullsFirst = "_first".equals(sortSpec.missingValue);
                sortFields[i] = parser().resolve(sortSpec.computedKey, reverse, nullsFirst, indexSettings, fieldTypeLookup);
                validateIndexSortField(sortFields[i]);
                continue;
            }
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

    /** The fields that the computed keys of this sort read. The mapping must keep them sortable like {@link #sortFields}. */
    public Set<String> computedKeyFields() {
        return computedKeyFields;
    }

    public boolean hasSortOnField(final String fieldName) {
        for (FieldSortSpec sortSpec : sortSpecs) {
            if (sortSpec.sortsOnField(fieldName)) {
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
        /** The text of a computed key, see {@link SortEntry#computedKey()}, in which case {@link #field} is that text too. */
        @Nullable
        String computedKey;

        FieldSortSpec(String field) {
            this.field = field;
        }

        /** Whether this sorts on the field, which a computed key named like the field doesn't. */
        boolean sortsOnField(String fieldName) {
            return computedKey == null && field.equals(fieldName);
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

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

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
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.sort.SortOrder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

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
**/
public final class IndexSortConfig {

    private static final DeprecationLogger DEPRECATION_LOGGER = DeprecationLogger.getLogger(IndexSortConfig.class);

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
        public static final FieldSortSpec[] TIME_SERIES_SORT, HOSTNAME_TIMESTAMP_BWC_SORT;

        private static final FieldSortSpec HOSTNAME_SPEC, MESSAGE_PATTERN_SPEC, TIMESTAMP_SPEC;

        static {
            TIMESTAMP_SPEC = new FieldSortSpec(DataStreamTimestampFieldMapper.DEFAULT_PATH);
            TIMESTAMP_SPEC.order = SortOrder.DESC;
            TIME_SERIES_SORT = new FieldSortSpec[] { new FieldSortSpec(TimeSeriesIdFieldMapper.NAME), TIMESTAMP_SPEC };

            HOSTNAME_SPEC = new FieldSortSpec(IndexMode.HOST_NAME);
            HOSTNAME_SPEC.order = SortOrder.ASC;
            HOSTNAME_SPEC.missingValue = "_last";
            HOSTNAME_SPEC.mode = MultiValueMode.MIN;

            MESSAGE_PATTERN_SPEC = new FieldSortSpec("message.template_id");

            // Older indexes use ascending ordering for host name and timestamp.
            HOSTNAME_TIMESTAMP_BWC_SORT = new FieldSortSpec[] {
                new FieldSortSpec(IndexMode.HOST_NAME),
                new FieldSortSpec(DataStreamTimestampFieldMapper.DEFAULT_PATH) };
        }

        public static FieldSortSpec[] getDefaultSortSpecs(Settings settings) {
            if (settings.isEmpty()) {
                return new FieldSortSpec[0];
            }

            // Can't use IndexSettings.MODE.get(settings) here because the validation logic for IndexSettings.MODE uses the default value
            // of index.sort.*, which causes infinite recursion (since we're already in the default value provider for those settings).
            // So we need to get the mode while bypassing the validation.
            String indexMode = settings.get(IndexSettings.MODE.getKey());
            if (indexMode != null) {
                indexMode = indexMode.toLowerCase(Locale.ROOT);
            }

            if (IndexMode.TIME_SERIES.getName().equals(indexMode)) {
                return TIME_SERIES_SORT;
            } else if (IndexMode.LOGSDB.getName().equals(indexMode)) {
                var version = IndexMetadata.SETTING_INDEX_VERSION_CREATED.get(settings);
                if (version.onOrAfter(IndexVersions.LOGSB_OPTIONAL_SORTING_ON_HOST_NAME)
                    || version.between(
                        IndexVersions.LOGSB_OPTIONAL_SORTING_ON_HOST_NAME_BACKPORT,
                        IndexVersions.UPGRADE_TO_LUCENE_10_0_0
                    )) {

                    List<FieldSortSpec> sortSpecs = new ArrayList<>(3);
                    if (IndexSettings.LOGSDB_SORT_ON_HOST_NAME.get(settings)) {
                        sortSpecs.add(HOSTNAME_SPEC);
                    }
                    if (IndexSettings.LOGSDB_SORT_ON_MESSAGE_TEMPLATE.get(settings)) {
                        sortSpecs.add(MESSAGE_PATTERN_SPEC);
                    }
                    sortSpecs.add(TIMESTAMP_SPEC);

                    return sortSpecs.toArray(FieldSortSpec[]::new);
                } else {
                    return HOSTNAME_TIMESTAMP_BWC_SORT;
                }
            }

            return new FieldSortSpec[0];
        }

        public static FieldSortSpec[] getSortSpecs(Settings settings) {
            if (INDEX_SORT_FIELD_SETTING.exists(settings) == false) {
                return IndexSortConfigDefaults.getDefaultSortSpecs(settings);
            }

            List<String> fields = INDEX_SORT_FIELD_SETTING.get(settings);
            FieldSortSpec[] sortSpecs = fields.stream().map(FieldSortSpec::new).toArray(FieldSortSpec[]::new);

            // Need to populate `order` because the default value of `mode` depends on it
            if (INDEX_SORT_ORDER_SETTING.exists(settings)) {
                List<SortOrder> orders = INDEX_SORT_ORDER_SETTING.get(settings);
                for (int i = 0; i < sortSpecs.length; i++) {
                    sortSpecs[i].order = orders.get(i);
                }
            }

            return sortSpecs;
        }

        public static List<String> getDefaultSortFields(Settings settings) {
            return Arrays.stream(getDefaultSortSpecs(settings)).map(sortSpec -> sortSpec.field).toList();
        }

        public static List<String> getDefaultSortOrder(Settings settings) {
            return Arrays.stream(getSortSpecs(settings))
                .map(sortSpec -> sortSpec.order != null ? sortSpec.order : SortOrder.ASC)
                .map(Enum::toString)
                .toList();
        }

        public static List<String> getDefaultSortMode(Settings settings) {
            return Arrays.stream(getSortSpecs(settings)).map(sortSpec -> {
                if (sortSpec.mode != null) {
                    return sortSpec.mode;
                } else if (sortSpec.order == SortOrder.DESC) {
                    return MultiValueMode.MAX;
                } else {
                    return MultiValueMode.MIN;
                }
            }).map(order -> order.toString().toLowerCase(Locale.ROOT)).toList();
        }

        public static List<String> getDefaultSortMissing(Settings settings) {
            // _last is the default per IndexFieldData.XFieldComparatorSource.Nested#sortMissingLast
            return Arrays.stream(getSortSpecs(settings))
                .map(sortSpec -> sortSpec.missingValue != null ? sortSpec.missingValue : "_last")
                .toList();
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

    private static void validateSortSettings(Settings settings) {
        if (INDEX_SORT_FIELD_SETTING.exists(settings) == false) {
            for (Setting<?> setting : new Setting<?>[] { INDEX_SORT_ORDER_SETTING, INDEX_SORT_MODE_SETTING, INDEX_SORT_MISSING_SETTING }) {
                if (setting.exists(settings)) {
                    throw new IllegalArgumentException(
                        "setting [" + setting.getKey() + "] requires [" + INDEX_SORT_FIELD_SETTING.getKey() + "] to be configured"
                    );
                }
            }
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

    public IndexSortConfig(IndexSettings indexSettings) {
        final Settings settings = indexSettings.getSettings();
        this.indexCreatedVersion = indexSettings.getIndexVersionCreated();
        this.indexName = indexSettings.getIndex().getName();
        this.indexMode = indexSettings.getMode();

        validateSortSettings(settings);

        List<String> fields = INDEX_SORT_FIELD_SETTING.get(settings);
        sortSpecs = fields.stream().map(FieldSortSpec::new).toArray(FieldSortSpec[]::new);

        List<SortOrder> orders = INDEX_SORT_ORDER_SETTING.get(settings);
        for (int i = 0; i < sortSpecs.length; i++) {
            sortSpecs[i].order = orders.get(i);
        }

        List<MultiValueMode> modes = INDEX_SORT_MODE_SETTING.get(settings);
        for (int i = 0; i < sortSpecs.length; i++) {
            sortSpecs[i].mode = modes.get(i);
        }

        List<String> missingValues = INDEX_SORT_MISSING_SETTING.get(settings);
        for (int i = 0; i < sortSpecs.length; i++) {
            sortSpecs[i].missingValue = missingValues.get(i);
        }
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
            final MappedFieldType ft = fieldTypeLookup.apply(sortSpec.field);
            if (ft == null) {
                String err = "unknown index sort field:[" + sortSpec.field + "]";
                if (this.indexMode == IndexMode.TIME_SERIES) {
                    err += " required by [" + IndexSettings.MODE.getKey() + "=time_series]";
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
        if (ALLOWED_INDEX_SORT_TYPES.contains(type) == false) {
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
        } else if (sortField instanceof SortedNumericSortField) {
            return ((SortedNumericSortField) sortField).getNumericType();
        } else if (sortField.getComparatorSource() instanceof IndexFieldData.XFieldComparatorSource fcs) {
            return fcs.sortType();
        } else {
            return sortField.getType();
        }
    }
}

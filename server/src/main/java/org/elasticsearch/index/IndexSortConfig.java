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

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
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
    public static final Setting<List<String>> INDEX_SORT_FIELD_SETTING = Setting.stringListSetting(
        "index.sort.field",
        Setting.Property.IndexScope,
        Setting.Property.Final,
        Setting.Property.ServerlessPublic
    );

    /**
     * The {@link SortOrder} for each specified sort field (ie. <b>asc</b> or <b>desc</b>).
     */
    public static final Setting<List<SortOrder>> INDEX_SORT_ORDER_SETTING = Setting.listSetting(
        "index.sort.order",
        Collections.emptyList(),
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
        Collections.emptyList(),
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
        Collections.emptyList(),
        IndexSortConfig::validateMissingValue,
        Setting.Property.IndexScope,
        Setting.Property.Final,
        Setting.Property.ServerlessPublic
    );

    public static final FieldSortSpec[] TIME_SERIES_SORT, TIMESTAMP_SORT, HOSTNAME_TIMESTAMP_SORT, HOSTNAME_TIMESTAMP_BWC_SORT;
    static {
        FieldSortSpec timeStampSpec = new FieldSortSpec(DataStreamTimestampFieldMapper.DEFAULT_PATH);
        timeStampSpec.order = SortOrder.DESC;
        TIME_SERIES_SORT = new FieldSortSpec[] { new FieldSortSpec(TimeSeriesIdFieldMapper.NAME), timeStampSpec };
        TIMESTAMP_SORT = new FieldSortSpec[] { timeStampSpec };

        FieldSortSpec hostnameSpec = new FieldSortSpec(IndexMode.HOST_NAME);
        hostnameSpec.order = SortOrder.ASC;
        hostnameSpec.missingValue = "_last";
        hostnameSpec.mode = MultiValueMode.MIN;
        HOSTNAME_TIMESTAMP_SORT = new FieldSortSpec[] { hostnameSpec, timeStampSpec };

        // Older indexes use ascending ordering for host name and timestamp.
        HOSTNAME_TIMESTAMP_BWC_SORT = new FieldSortSpec[] {
            new FieldSortSpec(IndexMode.HOST_NAME),
            new FieldSortSpec(DataStreamTimestampFieldMapper.DEFAULT_PATH) };
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

        if (indexMode == IndexMode.TIME_SERIES) {
            sortSpecs = TIME_SERIES_SORT;
            return;
        }

        List<String> fields = INDEX_SORT_FIELD_SETTING.get(settings);
        if (indexMode == IndexMode.LOGSDB && INDEX_SORT_FIELD_SETTING.exists(settings) == false) {
            if (INDEX_SORT_ORDER_SETTING.exists(settings)) {
                var order = INDEX_SORT_ORDER_SETTING.get(settings);
                throw new IllegalArgumentException("index.sort.fields:" + fields + " index.sort.order:" + order + ", size mismatch");
            }
            if (INDEX_SORT_MODE_SETTING.exists(settings)) {
                var mode = INDEX_SORT_MODE_SETTING.get(settings);
                throw new IllegalArgumentException("index.sort.fields:" + fields + " index.sort.mode:" + mode + ", size mismatch");
            }
            if (INDEX_SORT_MISSING_SETTING.exists(settings)) {
                var missing = INDEX_SORT_MISSING_SETTING.get(settings);
                throw new IllegalArgumentException("index.sort.fields:" + fields + " index.sort.missing:" + missing + ", size mismatch");
            }
            var version = indexSettings.getIndexVersionCreated();
            if (version.onOrAfter(IndexVersions.LOGSB_OPTIONAL_SORTING_ON_HOST_NAME_BACKPORT)) {
                sortSpecs = (IndexSettings.LOGSDB_SORT_ON_HOST_NAME.get(settings)) ? HOSTNAME_TIMESTAMP_SORT : TIMESTAMP_SORT;
            } else {
                sortSpecs = HOSTNAME_TIMESTAMP_BWC_SORT;
            }
            return;
        }
        sortSpecs = fields.stream().map(FieldSortSpec::new).toArray(FieldSortSpec[]::new);

        if (INDEX_SORT_ORDER_SETTING.exists(settings)) {
            List<SortOrder> orders = INDEX_SORT_ORDER_SETTING.get(settings);
            if (orders.size() != sortSpecs.length) {
                throw new IllegalArgumentException("index.sort.field:" + fields + " index.sort.order:" + orders + ", size mismatch");
            }
            for (int i = 0; i < sortSpecs.length; i++) {
                sortSpecs[i].order = orders.get(i);
            }
        }

        if (INDEX_SORT_MODE_SETTING.exists(settings)) {
            List<MultiValueMode> modes = INDEX_SORT_MODE_SETTING.get(settings);
            if (modes.size() != sortSpecs.length) {
                throw new IllegalArgumentException("index.sort.field:" + fields + " index.sort.mode:" + modes + ", size mismatch");
            }
            for (int i = 0; i < sortSpecs.length; i++) {
                sortSpecs[i].mode = modes.get(i);
            }
        }

        if (INDEX_SORT_MISSING_SETTING.exists(settings)) {
            List<String> missingValues = INDEX_SORT_MISSING_SETTING.get(settings);
            if (missingValues.size() != sortSpecs.length) {
                throw new IllegalArgumentException(
                    "index.sort.field:" + fields + " index.sort.missing:" + missingValues + ", size mismatch"
                );
            }
            for (int i = 0; i < sortSpecs.length; i++) {
                sortSpecs[i].missingValue = missingValues.get(i);
            }
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
            sortFields[i] = fieldData.sortField(this.indexCreatedVersion, sortSpec.missingValue, mode, null, reverse);
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
        } else {
            return sortField.getType();
        }
    }
}

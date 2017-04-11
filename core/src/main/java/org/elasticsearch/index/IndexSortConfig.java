/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index;

import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.SortedSetSortField;
import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.sort.SortOrder;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.function.Function;

/**
 * Holds all the information that is used to build the sort order of an index.
 *
 * The index sort settings are <b>final</b> and can be defined only at index creation.
 */
public final class IndexSortConfig {
    /**
     * The list of field names
     */
    public static final Setting<List<String>> INDEX_SORT_FIELD_SETTING =
        Setting.listSetting("index.sort.field", Collections.emptyList(),
            Function.identity(), Setting.Property.IndexScope, Setting.Property.Final);

    /**
     * The {@link SortOrder} for each specified sort field (ie. <b>asc</b> or <b>desc</b>).
     */
    public static final Setting<List<SortOrder>> INDEX_SORT_ORDER_SETTING =
        Setting.listSetting("index.sort.order", Collections.emptyList(),
            IndexSortConfig::parseOrderMode, Setting.Property.IndexScope, Setting.Property.Final);

    private static SortOrder parseOrderMode(String value) {
        try {
            return SortOrder.fromString(value);
        } catch (Exception e) {
            throw new IllegalArgumentException("Illegal sort order:" + value);
        }
    }

    /**
     * The {@link MultiValueMode} for each specified sort field (ie. <b>max</b> or <b>min</b>).
     */
    public static final Setting<List<MultiValueMode>> INDEX_SORT_MODE_SETTING =
        Setting.listSetting("index.sort.mode", Collections.emptyList(),
            IndexSortConfig::parseMultiValueMode, Setting.Property.IndexScope, Setting.Property.Final);

    private static MultiValueMode parseMultiValueMode(String value) {
        MultiValueMode mode = MultiValueMode.fromString(value);
        if (mode != MultiValueMode.MAX && mode != MultiValueMode.MIN) {
            throw new IllegalArgumentException("Illegal index sort mode:[" + mode + "], " +
                "must be one of [" + MultiValueMode.MAX + ", " + MultiValueMode.MIN + "]");
        }
        return mode;
    }

    /**
     * The missing value for each specified sort field (ie. <b>_first</b> or <b>_last</b>)
     */
    public static final Setting<List<String>> INDEX_SORT_MISSING_SETTING =
        Setting.listSetting("index.sort.missing", Collections.emptyList(),
            IndexSortConfig::validateMissingValue, Setting.Property.IndexScope, Setting.Property.Final);

    private static String validateMissingValue(String missing) {
        if ("_last".equals(missing) == false && "_first".equals(missing) == false) {
            throw new IllegalArgumentException("Illegal missing value:[" + missing + "], " +
                "must be one of [_last, _first]");
        }
        return missing;
    }

    final String[] fields;
    final SortOrder[] orders;
    final MultiValueMode[] modes;
    final String[] missingValues;

    public IndexSortConfig(IndexSettings indexSettings) {
        final Settings settings = indexSettings.getSettings();
        if (INDEX_SORT_FIELD_SETTING.exists(settings)) {
            fields = INDEX_SORT_FIELD_SETTING.get(settings)
                .toArray(new String[0]);
        } else {
            fields = new String[0];
        }
        if (fields.length > 0 && indexSettings.getIndexVersionCreated().before(Version.V_6_0_0_alpha1_UNRELEASED)) {
            throw new IllegalArgumentException("unsupported index.version.created:" + indexSettings.getIndexVersionCreated() +
                ", can't set index.sort on versions prior to " + Version.V_6_0_0_alpha1_UNRELEASED);
        }

        if (INDEX_SORT_ORDER_SETTING.exists(settings)) {
            orders = INDEX_SORT_ORDER_SETTING.get(settings)
                .toArray(new SortOrder[fields.length]);
            if (orders.length != fields.length) {
                throw new IllegalArgumentException("index.sort.field:" + Arrays.toString(fields) +
                    " index.sort.order:" + Arrays.toString(orders) + ", size mismatch");
            }
        } else {
            orders = new SortOrder[fields.length];
        }

        if (INDEX_SORT_MODE_SETTING.exists(settings)) {
            modes = INDEX_SORT_MODE_SETTING.get(settings)
                .toArray(new MultiValueMode[fields.length]);
            if (modes.length != fields.length) {
                throw new IllegalArgumentException("index.sort.field:" + Arrays.toString(fields) +
                    " index.sort.mode:" + Arrays.toString(modes) + ", size mismatch");
            }
        } else {
            modes = new MultiValueMode[fields.length];
        }

        if (INDEX_SORT_MISSING_SETTING.exists(settings)) {
            missingValues = INDEX_SORT_MISSING_SETTING.get(settings)
                .toArray(new String[fields.length]);
            if (missingValues.length != fields.length) {
                throw new IllegalArgumentException("index.sort.field:" + Arrays.toString(fields) +
                    " index.sort.missing:" + Arrays.toString(missingValues) + ", size mismatch");
            }
        } else {
            missingValues = new String[fields.length];
        }
    }


    /**
     * Returns true if the index should be sorted
     */
    public boolean hasIndexSort() {
        return fields.length > 0;
    }

    /**
     * Builds the {@link Sort} order from the settings for this index
     * or returns null if this index has no sort.
     */
    public Sort buildIndexSort(Function<String, MappedFieldType> fieldTypeLookup,
                               Function<MappedFieldType, IndexFieldData<?>> fieldDataLookup) {
        if (hasIndexSort() == false) {
            return null;
        }

        final SortField[] sortFields = new SortField[fields.length];
        for (int i = 0; i < fields.length; i++) {
            final MappedFieldType ft = fieldTypeLookup.apply(fields[i]);
            if (ft == null) {
                throw new IllegalArgumentException("unknown index sort field:[" + fields[i] + "]");
            }
            boolean reverse = orders[i] == null ? false : (orders[i] == SortOrder.DESC);
            MultiValueMode mode =
                modes[i] == null ?
                    reverse ? MultiValueMode.MAX : MultiValueMode.MIN :
                    modes[i];
            IndexFieldData<?> fieldData;
            try {
                fieldData = fieldDataLookup.apply(ft);
            } catch (Exception e) {
                throw new IllegalArgumentException("can't load docvalues for index sort field:[" + fields[i] + "]");
            }
            if (fieldData == null) {
                throw new IllegalArgumentException("fielddata not found for index sort field:[" + fields[i] + "]");
            }
            sortFields[i] = fieldData.sortField(missingValues[i], mode, null, reverse);
            validateIndexSortField(sortFields[i]);
        }
        return new Sort(sortFields);
    }

    private void validateIndexSortField(SortField sortField) {
        SortField.Type type = getSortFieldType(sortField);
        if (ALLOWED_INDEX_SORT_TYPES.contains(type) == false) {
            throw new IllegalArgumentException("invalid index sort field:[" + sortField.getField() + "]");
        }
    }

    /** We only allow index sorting on these types */
    private static final EnumSet<SortField.Type> ALLOWED_INDEX_SORT_TYPES = EnumSet.of(SortField.Type.STRING,
        SortField.Type.LONG,
        SortField.Type.INT,
        SortField.Type.DOUBLE,
        SortField.Type.FLOAT);

    static SortField.Type getSortFieldType(SortField sortField) {
        if (sortField instanceof SortedSetSortField) {
            return SortField.Type.STRING;
        } else if (sortField instanceof SortedNumericSortField) {
            return ((SortedNumericSortField) sortField).getNumericType();
        } else {
            return sortField.getType();
        }
    }
}

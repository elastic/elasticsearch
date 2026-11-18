/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.IndexType;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.RoutingFieldMapper;
import org.elasticsearch.index.mapper.SliceKeyFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.index.IndexVersionUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.elasticsearch.index.IndexSettingsTests.newIndexMeta;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class IndexSortSettingsTests extends ESTestCase {

    private static IndexSettings indexSettings(Settings settings) {
        return new IndexSettings(newIndexMeta("test", settings), Settings.EMPTY);
    }

    private static IndexSettings indexSettings(Settings settings, IndexVersion indexVersion) {
        return new IndexSettings(newIndexMeta("test", settings, indexVersion), Settings.EMPTY);
    }

    public void testNoIndexSort() {
        IndexSettings indexSettings = indexSettings(Settings.EMPTY);
        assertFalse(indexSettings.getIndexSortConfig().hasIndexSort());
    }

    public void testSliceEnabledAddsSliceKeyPrimarySort() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        IndexSettings indexSettings = indexSettings(Settings.builder().put(IndexSettings.SLICE_ENABLED.getKey(), true).build());
        IndexSortConfig config = indexSettings.getIndexSortConfig();
        assertTrue(config.hasIndexSort());
        assertThat(config.sortSpecs.length, equalTo(1));

        assertThat(config.sortSpecs[0].field, equalTo(SliceIndexing.SLICE_KEY_FIELD_NAME));
        assertThat(config.sortSpecs[0].order, equalTo(SortOrder.ASC));
        assertThat(config.sortSpecs[0].missingValue, equalTo("_last"));
        assertThat(config.sortSpecs[0].mode, equalTo(MultiValueMode.MIN));
    }

    public void testSliceEnabledBuildIndexSortWithImplicitSliceKeyPrimarySort() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        IndexSettings indexSettings = indexSettings(Settings.builder().put(IndexSettings.SLICE_ENABLED.getKey(), true).build());

        Sort sort = buildIndexSort(indexSettings, Map.of(SliceIndexing.SLICE_KEY_FIELD_NAME, SliceKeyFieldMapper.FIELD_TYPE));
        assertThat(sort.getSort(), arrayWithSize(1));
        assertThat(sort.getSort()[0].getField(), equalTo(SliceIndexing.SLICE_KEY_FIELD_NAME));
    }

    public void testSliceEnabledBuildIndexSortWithoutSliceKeyFieldLookup() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        IndexSettings indexSettings = indexSettings(Settings.builder().put(IndexSettings.SLICE_ENABLED.getKey(), true).build());

        // Metadata fields are not in the lookup before mappings load; the config resolves the slice key type itself.
        Sort sort = buildIndexSort(indexSettings, Map.of());
        assertThat(sort.getSort(), arrayWithSize(1));
        assertThat(sort.getSort()[0].getField(), equalTo(SliceIndexing.SLICE_KEY_FIELD_NAME));
    }

    public void testSliceEnabledPrependsSliceKeyToExplicitIndexSort() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        Settings settings = Settings.builder()
            .put(IndexSettings.SLICE_ENABLED.getKey(), true)

            .put("index.sort.field", "field1")
            .put("index.sort.order", "desc")
            .put("index.sort.mode", "max")
            .put("index.sort.missing", "_first")
            .build();
        IndexSettings indexSettings = indexSettings(settings);
        IndexSortConfig config = indexSettings.getIndexSortConfig();
        assertTrue(config.hasIndexSort());
        assertThat(config.sortSpecs.length, equalTo(2));

        assertThat(config.sortSpecs[0].field, equalTo(SliceIndexing.SLICE_KEY_FIELD_NAME));
        assertThat(config.sortSpecs[1].field, equalTo("field1"));
        assertThat(config.sortSpecs[0].order, equalTo(SortOrder.ASC));
        assertThat(config.sortSpecs[1].order, equalTo(SortOrder.DESC));
        assertThat(config.sortSpecs[0].missingValue, equalTo("_last"));
        assertThat(config.sortSpecs[1].missingValue, equalTo("_first"));
        assertThat(config.sortSpecs[0].mode, equalTo(MultiValueMode.MIN));
        assertThat(config.sortSpecs[1].mode, equalTo(MultiValueMode.MAX));
    }

    public void testSliceEnabledRejectsRoutingInUserProvidedIndexSort() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        Settings settings = Settings.builder()
            .put(IndexSettings.SLICE_ENABLED.getKey(), true)

            .putList("index.sort.field", RoutingFieldMapper.NAME, "field1")
            .putList("index.sort.order", "desc", "asc")
            .putList("index.sort.mode", "max", "min")
            .putList("index.sort.missing", "_first", "_last")
            .build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> indexSettings(settings));
        assertThat(e.getMessage(), containsString("must not contain [_routing], [_slice], [_slice_key] or [_slice_hash]"));
    }

    public void testSliceEnabledRejectsSliceAliasInUserProvidedIndexSort() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        Settings settings = Settings.builder()
            .put(IndexSettings.SLICE_ENABLED.getKey(), true)

            .putList("index.sort.field", SliceIndexing.FIELD_NAME, "field1")
            .putList("index.sort.order", "desc", "asc")
            .putList("index.sort.mode", "max", "min")
            .putList("index.sort.missing", "_first", "_last")
            .build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> indexSettings(settings));
        assertThat(e.getMessage(), containsString("must not contain [_routing], [_slice], [_slice_key] or [_slice_hash]"));
    }

    public void testSliceEnabledRejectsSliceKeyInUserProvidedIndexSort() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        String field = randomFrom(SliceIndexing.SLICE_KEY_FIELD_NAME, SliceIndexing.SLICE_HASH_FIELD_NAME);
        Settings settings = Settings.builder()
            .put(IndexSettings.SLICE_ENABLED.getKey(), true)
            .putList("index.sort.field", field, "field1")
            .putList("index.sort.order", "asc", "asc")
            .putList("index.sort.mode", "min", "min")
            .putList("index.sort.missing", "_last", "_last")
            .build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> indexSettings(settings));
        assertThat(e.getMessage(), containsString("must not contain [_routing], [_slice], [_slice_key] or [_slice_hash]"));
    }

    public void testSliceEnabledPrependsSliceKeyToIndexModeDefaultSort() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.LOGSDB.getName())
            .put(IndexSettings.LOGSDB_SORT_ON_HOST_NAME.getKey(), true)
            .put(IndexSettings.SLICE_ENABLED.getKey(), true)

            .build();
        IndexSettings indexSettings = indexSettings(settings);
        IndexSortConfig config = indexSettings.getIndexSortConfig();
        assertTrue(config.hasIndexSort());
        assertThat(config.sortSpecs.length, equalTo(3));

        assertThat(config.sortSpecs[0].field, equalTo(SliceIndexing.SLICE_KEY_FIELD_NAME));
        assertThat(config.sortSpecs[1].field, equalTo("host.name"));
        assertThat(config.sortSpecs[2].field, equalTo("@timestamp"));
        assertThat(config.sortSpecs[0].order, equalTo(SortOrder.ASC));
        assertThat(config.sortSpecs[1].order, equalTo(SortOrder.ASC));
        assertThat(config.sortSpecs[2].order, equalTo(SortOrder.DESC));
        assertThat(config.sortSpecs[0].missingValue, equalTo("_last"));
        assertThat(config.sortSpecs[1].missingValue, equalTo("_last"));
        assertThat(config.sortSpecs[2].missingValue, equalTo("_last"));
        assertThat(config.sortSpecs[0].mode, equalTo(MultiValueMode.MIN));
        assertThat(config.sortSpecs[1].mode, equalTo(MultiValueMode.MIN));
        assertThat(config.sortSpecs[2].mode, equalTo(MultiValueMode.MAX));
    }

    public void testSimpleIndexSort() {
        Settings settings = Settings.builder()
            .put("index.sort.field", "field1")
            .put("index.sort.order", "asc")
            .put("index.sort.mode", "max")
            .put("index.sort.missing", "_last")
            .build();
        IndexSettings indexSettings = indexSettings(settings);
        IndexSortConfig config = indexSettings.getIndexSortConfig();
        assertTrue(config.hasIndexSort());
        assertThat(config.sortSpecs.length, equalTo(1));

        assertThat(config.sortSpecs[0].field, equalTo("field1"));
        assertThat(config.sortSpecs[0].order, equalTo(SortOrder.ASC));
        assertThat(config.sortSpecs[0].missingValue, equalTo("_last"));
        assertThat(config.sortSpecs[0].mode, equalTo(MultiValueMode.MAX));
    }

    public void testIndexSortWithArrays() {
        Settings settings = Settings.builder()
            .putList("index.sort.field", "field1", "field2")
            .putList("index.sort.order", "asc", "desc")
            .putList("index.sort.missing", "_last", "_first")
            .build();
        IndexSettings indexSettings = indexSettings(settings);
        IndexSortConfig config = indexSettings.getIndexSortConfig();
        assertTrue(config.hasIndexSort());
        assertThat(config.sortSpecs.length, equalTo(2));

        assertThat(config.sortSpecs[0].field, equalTo("field1"));
        assertThat(config.sortSpecs[1].field, equalTo("field2"));
        assertThat(config.sortSpecs[0].order, equalTo(SortOrder.ASC));
        assertThat(config.sortSpecs[1].order, equalTo(SortOrder.DESC));
        assertThat(config.sortSpecs[0].missingValue, equalTo("_last"));
        assertThat(config.sortSpecs[1].missingValue, equalTo("_first"));
        assertThat(config.sortSpecs[0].mode, equalTo(MultiValueMode.MIN));
        assertThat(config.sortSpecs[1].mode, equalTo(MultiValueMode.MAX));
    }

    public void testSortExpressionIsParsed() {
        Settings settings = Settings.builder().put("index.sort", "agent.name,  mv_min(@timestamp) desc nulls last").build();
        IndexSortConfig config = indexSettings(settings).getIndexSortConfig();
        assertThat(config.sortSpecs.length, equalTo(2));
        assertThat(config.sortSpecs[0].field, equalTo("agent.name"));
        assertThat(config.sortSpecs[0].order, equalTo(SortOrder.ASC));
        assertThat(config.sortSpecs[0].mode, equalTo(MultiValueMode.MIN));
        assertThat(config.sortSpecs[0].missingValue, equalTo("_last"));
        assertThat(config.sortSpecs[1].field, equalTo("@timestamp"));
        assertThat(config.sortSpecs[1].order, equalTo(SortOrder.DESC));
        assertThat(config.sortSpecs[1].mode, equalTo(MultiValueMode.MIN));
        assertThat(config.sortSpecs[1].missingValue, equalTo("_last"));
    }

    public void testSortExpressionDoesNotChangeSortSettingDefaults() {
        Settings settings = Settings.builder().put("index.sort", "field1 DESC, field2").build();
        assertThat(IndexSortConfig.INDEX_SORT_FIELD_SETTING.exists(settings), equalTo(false));
        assertThat(IndexSortConfig.INDEX_SORT_FIELD_SETTING.get(settings), equalTo(List.of()));
        assertThat(IndexSortConfig.INDEX_SORT_ORDER_SETTING.get(settings), equalTo(List.of()));
        assertThat(IndexSortConfig.INDEX_SORT_MODE_SETTING.get(settings), equalTo(List.of()));
        assertThat(IndexSortConfig.INDEX_SORT_MISSING_SETTING.get(settings), equalTo(List.of()));
        assertThat(IndexSortConfig.sortFields(settings), equalTo(List.of("field1", "field2")));
        assertThat(
            IndexSortConfig.sortFields(Settings.builder().putList("index.sort.field", "a", "b").build()),
            equalTo(List.of("a", "b"))
        );
    }

    public void testSortExpressionDefaultsModeFromOrder() {
        IndexSortConfig config = indexSettings(Settings.builder().put("index.sort", "@timestamp DESC").build()).getIndexSortConfig();
        assertThat(config.sortSpecs[0].order, equalTo(SortOrder.DESC));
        assertThat(config.sortSpecs[0].mode, equalTo(MultiValueMode.MAX));
        assertThat(config.sortSpecs[0].missingValue, equalTo("_first"));
    }

    /**
     * Like in ES|QL, a descending field without {@code NULLS} puts missing values first. This differs from
     * {@code index.sort.missing}, which defaults to {@code _last}.
     */
    public void testSortExpressionDescWithoutNullsDefaultsToNullsFirst() {
        IndexSortConfig config = indexSettings(
            Settings.builder().put("index.sort", "a, b ASC, c DESC, d DESC NULLS LAST, e ASC NULLS FIRST").build()
        ).getIndexSortConfig();
        assertThat(config.sortSpecs[0].missingValue, equalTo("_last"));
        assertThat(config.sortSpecs[1].missingValue, equalTo("_last"));
        assertThat(config.sortSpecs[2].missingValue, equalTo("_first"));
        assertThat(config.sortSpecs[3].missingValue, equalTo("_last"));
        assertThat(config.sortSpecs[4].missingValue, equalTo("_first"));
    }

    public void testSortExpressionModeOverridesOrderDefault() {
        IndexSortConfig config = indexSettings(Settings.builder().put("index.sort", "MV_MAX(field1) ASC, MV_MIN(field2) DESC").build())
            .getIndexSortConfig();
        assertThat(config.sortSpecs[0].order, equalTo(SortOrder.ASC));
        assertThat(config.sortSpecs[0].mode, equalTo(MultiValueMode.MAX));
        assertThat(config.sortSpecs[1].order, equalTo(SortOrder.DESC));
        assertThat(config.sortSpecs[1].mode, equalTo(MultiValueMode.MIN));
    }

    public void testSortExpressionBuildsSameLuceneSort() {
        MappedFieldType field1 = new DateFieldMapper.DateFieldType("field1");
        MappedFieldType field2 = new DateFieldMapper.DateFieldType("field2");
        Sort fromExpression = buildIndexSort(
            indexSettings(Settings.builder().put("index.sort", "field1 DESC, MV_MAX(field2)").build()),
            field1,
            field2
        );
        Sort fromSettings = buildIndexSort(
            indexSettings(
                Settings.builder()
                    .putList("index.sort.field", "field1", "field2")
                    .putList("index.sort.order", "desc", "asc")
                    .putList("index.sort.mode", "max", "max")
                    .putList("index.sort.missing", "_first", "_last")
                    .build()
            ),
            field1,
            field2
        );
        assertThat(fromExpression, equalTo(fromSettings));
    }

    public void testSliceEnabledRejectsReservedFieldsInSortExpression() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        String field = randomFrom(
            RoutingFieldMapper.NAME,
            SliceIndexing.FIELD_NAME,
            SliceIndexing.SLICE_KEY_FIELD_NAME,
            SliceIndexing.SLICE_HASH_FIELD_NAME
        );
        Settings settings = Settings.builder()
            .put(IndexSettings.SLICE_ENABLED.getKey(), true)
            .put("index.sort", field + " DESC, field1")
            .build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> indexSettings(settings));
        assertThat(
            e.getMessage(),
            containsString("setting [index.sort] must not contain [_routing], [_slice], [_slice_key] or [_slice_hash]")
        );
    }

    public void testSliceEnabledPrependsSliceKeyToSortExpression() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        Settings settings = Settings.builder()
            .put(IndexSettings.SLICE_ENABLED.getKey(), true)
            .put("index.sort", "field1 DESC, field2")
            .build();
        IndexSortConfig config = indexSettings(settings).getIndexSortConfig();
        assertThat(config.sortSpecs.length, equalTo(3));
        assertThat(config.sortSpecs[0].field, equalTo(SliceIndexing.SLICE_KEY_FIELD_NAME));
        assertThat(config.sortSpecs[1].field, equalTo("field1"));
        assertThat(config.sortSpecs[1].order, equalTo(SortOrder.DESC));
        assertThat(config.sortSpecs[1].mode, equalTo(MultiValueMode.MAX));
        assertThat(config.sortSpecs[1].missingValue, equalTo("_first"));
        assertThat(config.sortSpecs[2].field, equalTo("field2"));
        assertThat(config.sortSpecs[2].order, equalTo(SortOrder.ASC));
    }

    public void testSortExpressionWhitespaceAndCase() {
        Settings settings = Settings.builder().put("index.sort", "a\tDESC,\n b  NULLS   FIRST ,  Mv_Max( c )   aSc ").build();
        IndexSortConfig config = indexSettings(settings).getIndexSortConfig();
        assertThat(config.sortSpecs.length, equalTo(3));
        assertThat(config.sortSpecs[0].field, equalTo("a"));
        assertThat(config.sortSpecs[0].order, equalTo(SortOrder.DESC));
        assertThat(config.sortSpecs[1].field, equalTo("b"));
        assertThat(config.sortSpecs[1].missingValue, equalTo("_first"));
        assertThat(config.sortSpecs[2].field, equalTo("c"));
        assertThat(config.sortSpecs[2].order, equalTo(SortOrder.ASC));
        assertThat(config.sortSpecs[2].mode, equalTo(MultiValueMode.MAX));
    }

    public void testSortExpressionDuplicateCheckIsOnTheWholeKey() {
        IndexSortConfig config = indexSettings(Settings.builder().put("index.sort", "MV_MIN(field1) DESC, field1, MV_MAX(field1)").build())
            .getIndexSortConfig();
        assertThat(config.sortSpecs.length, equalTo(3));
        assertThat(config.sortSpecs[0].mode, equalTo(MultiValueMode.MIN));
        assertThat(config.sortSpecs[1].mode, equalTo(MultiValueMode.MIN));
        assertThat(config.sortSpecs[2].mode, equalTo(MultiValueMode.MAX));
        assertThat(
            IndexSortConfig.sortFields(Settings.builder().put("index.sort", "MV_MIN(a), a DESC, b").build()),
            equalTo(List.of("a", "a", "b"))
        );
    }

    public void testSortExpressionBackticks() {
        IndexSortConfig config = indexSettings(Settings.builder().put("index.sort", "`my field` DESC, MV_MAX(`a,b``c)`)").build())
            .getIndexSortConfig();
        assertThat(config.sortSpecs[0].field, equalTo("my field"));
        assertThat(config.sortSpecs[0].order, equalTo(SortOrder.DESC));
        assertThat(config.sortSpecs[1].field, equalTo("a,b`c)"));
        assertThat(config.sortSpecs[1].mode, equalTo(MultiValueMode.MAX));
        // A name is only a function when it is followed by a parenthesis, and quoted names are never functions.
        IndexSortConfig plainNames = indexSettings(Settings.builder().put("index.sort", "mv_max, `mv_min`").build()).getIndexSortConfig();
        assertThat(plainNames.sortSpecs[0].field, equalTo("mv_max"));
        assertThat(plainNames.sortSpecs[1].field, equalTo("mv_min"));
        IndexSortConfig quoted = indexSettings(Settings.builder().put("index.sort", "`pod-name`, `null` DESC").build())
            .getIndexSortConfig();
        assertThat(quoted.sortSpecs[0].field, equalTo("pod-name"));
        assertThat(quoted.sortSpecs[1].field, equalTo("null"));
    }

    public void testEmptySortExpressionHasNoSort() {
        assertFalse(indexSettings(Settings.builder().put("index.sort", "").build()).getIndexSortConfig().hasIndexSort());
    }

    public void testSortExpressionCannotBeCombinedWithSortSettings() {
        for (String other : List.of("index.sort.field", "index.sort.order", "index.sort.mode", "index.sort.missing")) {
            String value = switch (other) {
                case "index.sort.field" -> "field1";
                case "index.sort.order" -> "asc";
                case "index.sort.mode" -> "min";
                case "index.sort.missing" -> "_last";
                default -> throw new AssertionError(other);
            };
            Settings settings = Settings.builder().put("index.sort", "field1").put(other, value).build();
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> indexSettings(settings));
            assertThat(e.getMessage(), containsString("setting [index.sort] cannot be combined with [" + other + "]"));
        }
    }

    public void testInvalidSortExpressions() {
        assertInvalidSortExpression("  ", "must not be blank");
        assertInvalidSortExpression("field1,", "empty sort entry");
        assertInvalidSortExpression("field1, , field2", "empty sort entry");
        assertInvalidSortExpression("field1 SIDEWAYS", "unexpected token [SIDEWAYS]");
        assertInvalidSortExpression("field1 ASC DESC", "unexpected [DESC]");
        assertInvalidSortExpression("field1 NULLS FIRST ASC", "unexpected [ASC]");
        assertInvalidSortExpression("field1 NULLS FIRST NULLS LAST", "NULLS more than once");
        assertInvalidSortExpression("field1 NULLS", "expects FIRST or LAST after NULLS but found []");
        assertInvalidSortExpression("field1 NULLS SOMETIMES", "expects FIRST or LAST after NULLS but found [SOMETIMES]");
        assertInvalidSortExpression("field1 MV_FIRST_MAX", "unexpected token [MV_FIRST_MAX]");
        assertInvalidSortExpression("field1 MISSING_LAST", "unexpected token [MISSING_LAST]");
        assertInvalidSortExpression("ABS(field1)", "unsupported function [ABS]");
        assertInvalidSortExpression("MV_MAX(field1", "missing a closing parenthesis");
        assertInvalidSortExpression("MV_MAX()", "unexpected character [)]");
        assertInvalidSortExpression("MV_MAX(field1) DESC)", "unexpected character [)]");
        assertInvalidSortExpression("`field1", "unterminated backtick");
        assertInvalidSortExpression("``", "empty quoted field name");
        assertInvalidSortExpression("field1 field2", "unexpected token [field2]");
        assertInvalidSortExpression("MV_MAX(field1), mv_max(field1) DESC", "sort key [MV_MAX(field1)] more than once");
        assertInvalidSortExpression("field1, field1 DESC", "sort key [field1] more than once");
        // ES|QL reads these as expressions or keywords, not as a single field name.
        assertInvalidSortExpression("pod-name DESC", "invalid unquoted field name [pod-name]");
        assertInvalidSortExpression("a+b", "invalid unquoted field name [a+b]");
        assertInvalidSortExpression("1x", "invalid unquoted field name [1x]");
        assertInvalidSortExpression("a..b", "invalid unquoted field name [a..b]");
        assertInvalidSortExpression("a.", "invalid unquoted field name [a.]");
        assertInvalidSortExpression("foo*", "invalid unquoted field name [foo*]");
        assertInvalidSortExpression("null", "reserved word [null]");
        assertInvalidSortExpression("asc", "reserved word [asc]");
        assertInvalidSortExpression("true DESC", "reserved word [true]");
        assertInvalidSortExpression("MV_MAX(null)", "reserved word [null]");
    }

    private static void assertInvalidSortExpression(String expression, String expectedMessage) {
        Settings settings = Settings.builder().put("index.sort", expression).build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> IndexSortConfig.INDEX_SORT_SETTING.get(settings));
        assertThat(e.getMessage(), containsString(expectedMessage));
    }

    public void testSortExpressionIsNotReportedAsDefault() {
        Settings.Builder diff = Settings.builder();
        IndexSortConfig.INDEX_SORT_SETTING.diff(diff, Settings.EMPTY, Settings.EMPTY);
        assertThat(diff.build().isEmpty(), equalTo(true));
    }

    public void testInvalidIndexSort() {
        final Settings settings = Settings.builder().put("index.sort.field", "field1").put("index.sort.order", "asc, desc").build();
        IllegalArgumentException exc = expectThrows(IllegalArgumentException.class, () -> indexSettings(settings));
        assertThat(exc.getMessage(), containsString("index.sort.field:[field1] index.sort.order:[asc, desc], size mismatch"));
    }

    public void testInvalidIndexSortWithArray() {
        final Settings settings = Settings.builder()
            .put("index.sort.field", "field1")
            .putList("index.sort.order", new String[] { "asc", "desc" })
            .build();
        IllegalArgumentException exc = expectThrows(IllegalArgumentException.class, () -> indexSettings(settings));
        assertThat(exc.getMessage(), containsString("index.sort.field:[field1] index.sort.order:[asc, desc], size mismatch"));
    }

    public void testInvalidOrder() {
        final Settings settings = Settings.builder().put("index.sort.field", "field1").put("index.sort.order", "invalid").build();
        IllegalArgumentException exc = expectThrows(IllegalArgumentException.class, () -> indexSettings(settings));
        assertThat(exc.getMessage(), containsString("Illegal sort order:invalid"));
    }

    public void testInvalidMode() {
        final Settings settings = Settings.builder().put("index.sort.field", "field1").put("index.sort.mode", "invalid").build();
        IllegalArgumentException exc = expectThrows(IllegalArgumentException.class, () -> indexSettings(settings));
        assertThat(exc.getMessage(), containsString("Illegal sort mode: invalid"));
    }

    public void testInvalidMissing() {
        final Settings settings = Settings.builder().put("index.sort.field", "field1").put("index.sort.missing", "default").build();
        IllegalArgumentException exc = expectThrows(IllegalArgumentException.class, () -> indexSettings(settings));
        assertThat(exc.getMessage(), containsString("Illegal missing value:[default]," + " must be one of [_last, _first]"));
    }

    public void testIndexSortingNoDocValues() {
        IndexSettings indexSettings = indexSettings(Settings.builder().put("index.sort.field", "field").build());
        MappedFieldType fieldType = new MappedFieldType("field", IndexType.NONE, false, Collections.emptyMap()) {
            @Override
            public String typeName() {
                return null;
            }

            @Override
            public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
                fieldDataContext.lookupSupplier().get();
                return null;
            }

            @Override
            public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Query termQuery(Object value, SearchExecutionContext context) {
                throw new UnsupportedOperationException();
            }
        };
        Exception iae = expectThrows(IllegalArgumentException.class, () -> buildIndexSort(indexSettings, fieldType));
        assertEquals("docvalues not found for index sort field:[field]", iae.getMessage());
        assertThat(iae.getCause(), instanceOf(UnsupportedOperationException.class));
        assertEquals("index sorting not supported on runtime field [field]", iae.getCause().getMessage());
    }

    public void testSortingAgainstAliases() {
        IndexSettings indexSettings = indexSettings(Settings.builder().put("index.sort.field", "field").build());
        MappedFieldType aliased = new KeywordFieldMapper.KeywordFieldType("aliased");
        Exception e = expectThrows(IllegalArgumentException.class, () -> buildIndexSort(indexSettings, Map.of("field", aliased)));
        assertEquals("Cannot use alias [field] as an index sort field", e.getMessage());
    }

    public void testSortingAgainstAliasesPre713() {
        IndexSettings indexSettings = indexSettings(
            Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersions.V_7_12_0).put("index.sort.field", "field").build()
        );
        MappedFieldType aliased = new KeywordFieldMapper.KeywordFieldType("aliased");
        Sort sort = buildIndexSort(indexSettings, Map.of("field", aliased));
        assertThat(sort.getSort(), arrayWithSize(1));
        assertThat(sort.getSort()[0].getField(), equalTo("aliased"));
        assertWarnings(
            "Index sort for index [test] defined on field [field] which resolves to field [aliased]. "
                + "You will not be able to define an index sort over aliased fields in new indexes"
        );
    }

    public void testSortMissingValueDateNanoFieldPre714() {
        MappedFieldType tsField = new DateFieldMapper.DateFieldType("@timestamp", true, DateFieldMapper.Resolution.NANOSECONDS);
        var indexSettingsBuilder = Settings.builder();
        indexSettingsBuilder.put("index.sort.field", "@timestamp");
        indexSettingsBuilder.put("index.sort.order", "desc");

        // test with index version 7.13 and before
        var pre714Versions = Stream.concat(Stream.of(IndexVersions.V_7_13_0), randomVersionsBefore(IndexVersions.V_7_13_0)).toList();
        for (var version : pre714Versions) {
            indexSettingsBuilder.put(IndexMetadata.SETTING_VERSION_CREATED, version);
            Sort sort = buildIndexSort(indexSettings(indexSettingsBuilder.build()), Map.of("@timestamp", tsField));
            assertThat(sort.getSort(), arrayWithSize(1));
            assertThat(sort.getSort()[0].getField(), equalTo("@timestamp"));
            assertThat(sort.getSort()[0].getMissingValue(), equalTo(Long.MIN_VALUE));
        }

        // now test with index version 7.14 and after
        var post713Versions = Stream.concat(Stream.of(IndexVersions.V_7_14_0), randomVersionsAfter(IndexVersions.V_7_14_0)).toList();
        for (var version : post713Versions) {
            indexSettingsBuilder.put(IndexMetadata.SETTING_VERSION_CREATED, version);
            Sort sort = buildIndexSort(indexSettings(indexSettingsBuilder.build()), Map.of("@timestamp", tsField));
            assertThat(sort.getSort(), arrayWithSize(1));
            assertThat(sort.getSort()[0].getField(), equalTo("@timestamp"));
            assertThat(sort.getSort()[0].getMissingValue(), equalTo(0L));
        }

        // asc order has not changed behaviour in any version
        indexSettingsBuilder.put("index.sort.order", "asc");
        var allVersions = Stream.concat(post713Versions.stream(), pre714Versions.stream()).toList();
        for (var version : allVersions) {
            indexSettingsBuilder.put(IndexMetadata.SETTING_VERSION_CREATED, version);
            Sort sort = buildIndexSort(indexSettings(indexSettingsBuilder.build()), Map.of("@timestamp", tsField));
            assertThat(sort.getSort(), arrayWithSize(1));
            assertThat(sort.getSort()[0].getField(), equalTo("@timestamp"));
            assertThat(sort.getSort()[0].getMissingValue(), equalTo(DateUtils.MAX_NANOSECOND));
        }

        // ensure no change in behaviour when a missing value is set
        indexSettingsBuilder.put("index.sort.missing", "_first");
        for (var version : allVersions) {
            indexSettingsBuilder.put(IndexMetadata.SETTING_VERSION_CREATED, version);
            Sort sort = buildIndexSort(indexSettings(indexSettingsBuilder.build()), Map.of("@timestamp", tsField));
            assertThat(sort.getSort()[0].getMissingValue(), equalTo(0L));
        }
        indexSettingsBuilder.put("index.sort.missing", "_last");
        for (var version : allVersions) {
            indexSettingsBuilder.put(IndexMetadata.SETTING_VERSION_CREATED, version);
            Sort sort = buildIndexSort(indexSettings(indexSettingsBuilder.build()), Map.of("@timestamp", tsField));
            assertThat(sort.getSort()[0].getMissingValue(), equalTo(Long.MAX_VALUE));
        }
    }

    public void testTimeSeriesMode() {
        IndexSettings indexSettings = indexSettings(
            Settings.builder()
                .put(IndexSettings.MODE.getKey(), "time_series")
                .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "some_dimension")
                .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2021-04-28T00:00:00Z")
                .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2021-04-29T00:00:00Z")
                .build()
        );
        Sort sort = buildIndexSort(indexSettings, TimeSeriesIdFieldMapper.FIELD_TYPE, new DateFieldMapper.DateFieldType("@timestamp"));
        assertThat(sort.getSort(), arrayWithSize(2));
        assertThat(sort.getSort()[0].getField(), equalTo("_tsid"));
        assertThat(sort.getSort()[1].getField(), equalTo("@timestamp"));
    }

    public void testTimeSeriesModeNoTimestamp() {
        IndexSettings indexSettings = indexSettings(
            Settings.builder()
                .put(IndexSettings.MODE.getKey(), "time_series")
                .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "some_dimension")
                .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2021-04-28T00:00:00Z")
                .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2021-04-29T00:00:00Z")
                .build()
        );
        Exception e = expectThrows(IllegalArgumentException.class, () -> buildIndexSort(indexSettings, TimeSeriesIdFieldMapper.FIELD_TYPE));
        assertThat(e.getMessage(), equalTo("unknown index sort field:[@timestamp] required by [index.mode=time_series]"));
    }

    public void testLogsdbIndexSortWithArrays() {
        Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), "logsdb")
            .putList("index.sort.field", "field1", "field2")
            .putList("index.sort.order", "asc", "desc")
            .putList("index.sort.missing", "_last", "_first")
            .build();
        IndexSettings indexSettings = indexSettings(settings);
        IndexSortConfig config = indexSettings.getIndexSortConfig();
        assertTrue(config.hasIndexSort());
        assertThat(config.sortSpecs.length, equalTo(2));

        assertThat(config.sortSpecs[0].field, equalTo("field1"));
        assertThat(config.sortSpecs[1].field, equalTo("field2"));
        assertThat(config.sortSpecs[0].order, equalTo(SortOrder.ASC));
        assertThat(config.sortSpecs[1].order, equalTo(SortOrder.DESC));
        assertThat(config.sortSpecs[0].missingValue, equalTo("_last"));
        assertThat(config.sortSpecs[1].missingValue, equalTo("_first"));
        assertThat(config.sortSpecs[0].mode, equalTo(MultiValueMode.MIN));
        assertThat(config.sortSpecs[1].mode, equalTo(MultiValueMode.MAX));
    }

    public void testLogsdbInvalidIndexSortOrder() {
        final Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), "logsdb")
            .putList("index.sort.order", new String[] { "asc", "desc" })
            .build();
        IllegalArgumentException exc = expectThrows(IllegalArgumentException.class, () -> indexSettings(settings));
        assertThat(exc.getMessage(), containsString("setting [index.sort.order] requires [index.sort.field] to be configured"));
    }

    public void testLogsdbInvalidIndexSortMode() {
        final Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), "logsdb")
            .putList("index.sort.mode", new String[] { "max" })
            .build();
        IllegalArgumentException exc = expectThrows(IllegalArgumentException.class, () -> indexSettings(settings));
        assertThat(exc.getMessage(), containsString("setting [index.sort.mode] requires [index.sort.field] to be configured"));
    }

    public void testLogsdbInvalidIndexSortMissing() {
        final Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), "logsdb")
            .putList("index.sort.missing", new String[] { "_last", "_last" })
            .build();
        IllegalArgumentException exc = expectThrows(IllegalArgumentException.class, () -> indexSettings(settings));
        assertThat(exc.getMessage(), containsString("setting [index.sort.missing] requires [index.sort.field] to be configured"));
    }

    public void testLogsdbIndexSortWithHostname() {
        Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), "logsdb")
            .put(IndexSettings.LOGSDB_SORT_ON_HOST_NAME.getKey(), true)
            .build();
        IndexSettings indexSettings = indexSettings(settings);
        IndexSortConfig config = indexSettings.getIndexSortConfig();
        assertTrue(config.hasIndexSort());
        assertThat(config.sortSpecs.length, equalTo(2));

        assertThat(config.sortSpecs[0].field, equalTo("host.name"));
        assertThat(config.sortSpecs[1].field, equalTo("@timestamp"));
        assertThat(config.sortSpecs[0].order, equalTo(SortOrder.ASC));
        assertThat(config.sortSpecs[1].order, equalTo(SortOrder.DESC));
        assertThat(config.sortSpecs[0].missingValue, equalTo("_last"));
        assertThat(config.sortSpecs[1].missingValue, equalTo("_last"));
        assertThat(config.sortSpecs[0].mode, equalTo(MultiValueMode.MIN));
        assertThat(config.sortSpecs[1].mode, equalTo(MultiValueMode.MAX));
    }

    public void testLogsdbIndexSortWithMessage() {
        Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), "logsdb")
            .put(IndexSettings.LOGSDB_SORT_ON_MESSAGE_TEMPLATE.getKey(), true)
            .build();
        IndexSettings indexSettings = indexSettings(settings);
        IndexSortConfig config = indexSettings.getIndexSortConfig();
        assertTrue(config.hasIndexSort());
        assertThat(config.sortSpecs.length, equalTo(2));

        assertThat(config.sortSpecs[0].field, equalTo("message.template_id"));
        assertThat(config.sortSpecs[1].field, equalTo("@timestamp"));
        assertThat(config.sortSpecs[0].order, equalTo(SortOrder.ASC));
        assertThat(config.sortSpecs[1].order, equalTo(SortOrder.DESC));
        assertThat(config.sortSpecs[0].missingValue, equalTo("_last"));
        assertThat(config.sortSpecs[1].missingValue, equalTo("_last"));
        assertThat(config.sortSpecs[0].mode, equalTo(MultiValueMode.MIN));
        assertThat(config.sortSpecs[1].mode, equalTo(MultiValueMode.MAX));
    }

    public void testLogsdbIndexSortWithMessageAndHostname() {
        Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), "logsdb")
            .put(IndexSettings.LOGSDB_SORT_ON_HOST_NAME.getKey(), true)
            .put(IndexSettings.LOGSDB_SORT_ON_MESSAGE_TEMPLATE.getKey(), true)
            .build();
        IndexSettings indexSettings = indexSettings(settings);
        IndexSortConfig config = indexSettings.getIndexSortConfig();
        assertTrue(config.hasIndexSort());
        assertThat(config.sortSpecs.length, equalTo(3));

        assertThat(config.sortSpecs[0].field, equalTo("host.name"));
        assertThat(config.sortSpecs[1].field, equalTo("message.template_id"));
        assertThat(config.sortSpecs[2].field, equalTo("@timestamp"));
        assertThat(config.sortSpecs[0].order, equalTo(SortOrder.ASC));
        assertThat(config.sortSpecs[1].order, equalTo(SortOrder.ASC));
        assertThat(config.sortSpecs[2].order, equalTo(SortOrder.DESC));
        assertThat(config.sortSpecs[0].missingValue, equalTo("_last"));
        assertThat(config.sortSpecs[1].missingValue, equalTo("_last"));
        assertThat(config.sortSpecs[2].missingValue, equalTo("_last"));
        assertThat(config.sortSpecs[0].mode, equalTo(MultiValueMode.MIN));
        assertThat(config.sortSpecs[1].mode, equalTo(MultiValueMode.MIN));
        assertThat(config.sortSpecs[2].mode, equalTo(MultiValueMode.MAX));
    }

    public void testLogsdbIndexSortTimestampOnly() {
        Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), "logsdb")
            .put(IndexSettings.LOGSDB_SORT_ON_HOST_NAME.getKey(), false)
            .build();
        IndexSettings indexSettings = indexSettings(settings);
        IndexSortConfig config = indexSettings.getIndexSortConfig();
        assertTrue(config.hasIndexSort());
        assertThat(config.sortSpecs.length, equalTo(1));

        assertThat(config.sortSpecs[0].field, equalTo("@timestamp"));
        assertThat(config.sortSpecs[0].order, equalTo(SortOrder.DESC));
        assertThat(config.sortSpecs[0].missingValue, equalTo("_last"));
        assertThat(config.sortSpecs[0].mode, equalTo(MultiValueMode.MAX));
    }

    public void testLogsdbIndexSortTimestampBWC() {
        Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), "logsdb").build();
        IndexSettings indexSettings = indexSettings(
            settings,
            IndexVersionUtils.getPreviousVersion(IndexVersions.LOGSB_OPTIONAL_SORTING_ON_HOST_NAME)
        );
        IndexSortConfig config = indexSettings.getIndexSortConfig();
        assertTrue(config.hasIndexSort());
        assertThat(config.sortSpecs.length, equalTo(2));

        assertThat(config.sortSpecs[0].field, equalTo("host.name"));
        assertThat(config.sortSpecs[1].field, equalTo("@timestamp"));
        assertThat(config.sortSpecs[0].order, equalTo(SortOrder.ASC));
        assertThat(config.sortSpecs[1].order, equalTo(SortOrder.ASC));
        assertThat(config.sortSpecs[0].missingValue, equalTo("_last"));
        assertThat(config.sortSpecs[1].missingValue, equalTo("_last"));
        assertThat(config.sortSpecs[0].mode, equalTo(MultiValueMode.MIN));
        assertThat(config.sortSpecs[1].mode, equalTo(MultiValueMode.MIN));
    }

    private Sort buildIndexSort(IndexSettings indexSettings, MappedFieldType... mfts) {
        Map<String, MappedFieldType> lookup = Maps.newMapWithExpectedSize(mfts.length);
        for (MappedFieldType mft : mfts) {
            assertNull(lookup.put(mft.name(), mft));
        }
        return buildIndexSort(indexSettings, lookup);
    }

    private Sort buildIndexSort(IndexSettings indexSettings, Map<String, MappedFieldType> lookup) {
        IndexSortConfig config = indexSettings.getIndexSortConfig();
        assertTrue(config.hasIndexSort());
        IndicesFieldDataCache cache = new IndicesFieldDataCache(indexSettings.getSettings(), null);
        NoneCircuitBreakerService circuitBreakerService = new NoneCircuitBreakerService();
        IndexFieldDataService indexFieldDataService = new IndexFieldDataService(indexSettings, cache, circuitBreakerService);
        return config.buildIndexSort(
            lookup::get,
            (ft, s) -> indexFieldDataService.getForField(
                ft,
                new FieldDataContext("test", indexSettings, s, Set::of, () -> false, MappedFieldType.FielddataOperation.SEARCH)
            )
        );
    }

    /* Returns a stream of versions before the given version */
    Stream<IndexVersion> randomVersionsBefore(IndexVersion indexVersion) {
        var versions = IndexVersions.getAllVersions().stream().filter(v -> v.before(indexVersion)).toList();
        List<IndexVersion> ret = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            ret.add(randomValueOtherThanMany(ret::contains, () -> randomFrom(versions)));
        }
        return ret.stream();
    }

    /* Returns a stream of versions after the given version */
    Stream<IndexVersion> randomVersionsAfter(IndexVersion indexVersion) {
        var versions = IndexVersions.getAllVersions().stream().filter(v -> v.after(indexVersion)).toList();
        List<IndexVersion> ret = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            ret.add(randomValueOtherThanMany(ret::contains, () -> randomFrom(versions)));
        }
        return ret.stream();
    }
}

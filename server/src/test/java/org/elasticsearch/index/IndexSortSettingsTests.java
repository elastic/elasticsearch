/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.index.IndexSettingsTests.newIndexMeta;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class IndexSortSettingsTests extends ESTestCase {

    private static IndexSettings indexSettings(Settings settings) {
        return new IndexSettings(newIndexMeta("test", settings), Settings.EMPTY);
    }

    public void testNoIndexSort() {
        IndexSettings indexSettings = indexSettings(Settings.EMPTY);
        assertFalse(indexSettings.getIndexSortConfig().hasIndexSort());
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
        assertNull(config.sortSpecs[0].mode);
        assertNull(config.sortSpecs[1].mode);
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
        MappedFieldType fieldType = new MappedFieldType("field", false, false, false, TextSearchInfo.NONE, Collections.emptyMap()) {
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
                new FieldDataContext("test", indexSettings, s, Set::of, MappedFieldType.FielddataOperation.SEARCH)
            )
        );
    }
}

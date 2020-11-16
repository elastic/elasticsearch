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

import org.apache.lucene.search.Query;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.RuntimeFieldType;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.function.Supplier;

import static org.elasticsearch.common.settings.Settings.Builder.EMPTY_SETTINGS;
import static org.elasticsearch.index.IndexSettingsTests.newIndexMeta;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class IndexSortSettingsTests extends ESTestCase {
    private static IndexSettings indexSettings(Settings settings) {
        return new IndexSettings(newIndexMeta("test", settings), Settings.EMPTY);
    }

    public void testNoIndexSort() {
        IndexSettings indexSettings = indexSettings(EMPTY_SETTINGS);
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
        final Settings settings = Settings.builder()
            .put("index.sort.field", "field1")
            .put("index.sort.order", "asc, desc")
            .build();
        IllegalArgumentException exc =
            expectThrows(IllegalArgumentException.class, () -> indexSettings(settings));
        assertThat(exc.getMessage(), containsString("index.sort.field:[field1] index.sort.order:[asc, desc], size mismatch"));
    }

    public void testInvalidIndexSortWithArray() {
        final Settings settings = Settings.builder()
            .put("index.sort.field", "field1")
            .putList("index.sort.order", new String[] {"asc", "desc"})
            .build();
        IllegalArgumentException exc =
            expectThrows(IllegalArgumentException.class, () -> indexSettings(settings));
        assertThat(exc.getMessage(),
            containsString("index.sort.field:[field1] index.sort.order:[asc, desc], size mismatch"));
    }

    public void testInvalidOrder() {
        final Settings settings = Settings.builder()
            .put("index.sort.field", "field1")
            .put("index.sort.order", "invalid")
            .build();
        IllegalArgumentException exc =
            expectThrows(IllegalArgumentException.class, () -> indexSettings(settings));
        assertThat(exc.getMessage(), containsString("Illegal sort order:invalid"));
    }

    public void testInvalidMode() {
        final Settings settings = Settings.builder()
            .put("index.sort.field", "field1")
            .put("index.sort.mode", "invalid")
            .build();
        IllegalArgumentException exc =
            expectThrows(IllegalArgumentException.class, () -> indexSettings(settings));
        assertThat(exc.getMessage(), containsString("Illegal sort mode: invalid"));
    }

    public void testInvalidMissing() {
        final Settings settings = Settings.builder()
            .put("index.sort.field", "field1")
            .put("index.sort.missing", "default")
            .build();
        IllegalArgumentException exc =
            expectThrows(IllegalArgumentException.class, () -> indexSettings(settings));
        assertThat(exc.getMessage(), containsString("Illegal missing value:[default]," +
            " must be one of [_last, _first]"));
    }

    public void testIndexSorting() {
        IndexSettings indexSettings = indexSettings(Settings.builder().put("index.sort.field", "field").build());
        IndexSortConfig config = indexSettings.getIndexSortConfig();
        assertTrue(config.hasIndexSort());
        IndicesFieldDataCache cache = new IndicesFieldDataCache(Settings.EMPTY, null);
        NoneCircuitBreakerService circuitBreakerService = new NoneCircuitBreakerService();
        final IndexFieldDataService indexFieldDataService = new IndexFieldDataService(indexSettings, cache, circuitBreakerService, null);
        MappedFieldType fieldType = new RuntimeFieldType("field", Collections.emptyMap()) {
            @Override
            public ValueFetcher valueFetcher(QueryShardContext context, SearchLookup searchLookup, String format) {
                throw new UnsupportedOperationException();
            }

            @Override
            public String typeName() {
                return null;
            }

            @Override
            public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
                searchLookup.get();
                return null;
            }

            @Override
            public Query termQuery(Object value, QueryShardContext context) {
                throw new UnsupportedOperationException();
            }

            @Override
            protected void doXContentBody(XContentBuilder builder, boolean includeDefaults) {
                throw new UnsupportedOperationException();
            }
        };
        IllegalArgumentException iae = expectThrows(
            IllegalArgumentException.class,
            () -> config.buildIndexSort(
                field -> fieldType,
                (ft, searchLookupSupplier) -> indexFieldDataService.getForField(ft, "index", searchLookupSupplier)
            )
        );
        assertEquals("docvalues not found for index sort field:[field]", iae.getMessage());
        assertThat(iae.getCause(), instanceOf(UnsupportedOperationException.class));
        assertEquals("index sorting not supported on runtime field [field]", iae.getCause().getMessage());
    }
}

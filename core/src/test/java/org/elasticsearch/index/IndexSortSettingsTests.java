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

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.common.settings.Settings.Builder.EMPTY_SETTINGS;
import static org.elasticsearch.index.IndexSettingsTests.newIndexMeta;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class IndexSortSettingsTests extends ESTestCase {
    private static IndexSettings indexSettings(Settings settings) {
        return indexSettings(settings, null);
    }

    private static IndexSettings indexSettings(Settings settings, Version version) {
        final Settings newSettings;
        if (version != null) {
            newSettings = Settings.builder()
                .put(settings)
                .put(IndexMetaData.SETTING_VERSION_CREATED, version)
                .build();
        } else {
            newSettings = settings;
        }
        return new IndexSettings(newIndexMeta("test", newSettings), Settings.EMPTY);
    }

    public void testNoIndexSort() throws IOException {
        IndexSettings indexSettings = indexSettings(EMPTY_SETTINGS);
        assertFalse(indexSettings.getIndexSortConfig().hasIndexSort());
    }

    public void testSimpleIndexSort() throws IOException {
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

    public void testIndexSortWithArrays() throws IOException {
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

    public void testInvalidIndexSort() throws IOException {
        final Settings settings = Settings.builder()
            .put("index.sort.field", "field1")
            .put("index.sort.order", "asc, desc")
            .build();
        IllegalArgumentException exc =
            expectThrows(IllegalArgumentException.class, () -> indexSettings(settings));
        assertThat(exc.getMessage(), containsString("index.sort.field:[field1] index.sort.order:[asc, desc], size mismatch"));
    }

    public void testInvalidIndexSortWithArray() throws IOException {
        final Settings settings = Settings.builder()
            .put("index.sort.field", "field1")
            .putList("index.sort.order", new String[] {"asc", "desc"})
            .build();
        IllegalArgumentException exc =
            expectThrows(IllegalArgumentException.class, () -> indexSettings(settings));
        assertThat(exc.getMessage(),
            containsString("index.sort.field:[field1] index.sort.order:[asc, desc], size mismatch"));
    }

    public void testInvalidOrder() throws IOException {
        final Settings settings = Settings.builder()
            .put("index.sort.field", "field1")
            .put("index.sort.order", "invalid")
            .build();
        IllegalArgumentException exc =
            expectThrows(IllegalArgumentException.class, () -> indexSettings(settings));
        assertThat(exc.getMessage(), containsString("Illegal sort order:invalid"));
    }

    public void testInvalidMode() throws IOException {
        final Settings settings = Settings.builder()
            .put("index.sort.field", "field1")
            .put("index.sort.mode", "invalid")
            .build();
        IllegalArgumentException exc =
            expectThrows(IllegalArgumentException.class, () -> indexSettings(settings));
        assertThat(exc.getMessage(), containsString("Illegal sort mode: invalid"));
    }

    public void testInvalidMissing() throws IOException {
        final Settings settings = Settings.builder()
            .put("index.sort.field", "field1")
            .put("index.sort.missing", "default")
            .build();
        IllegalArgumentException exc =
            expectThrows(IllegalArgumentException.class, () -> indexSettings(settings));
        assertThat(exc.getMessage(), containsString("Illegal missing value:[default]," +
            " must be one of [_last, _first]"));
    }

    public void testInvalidVersion() throws IOException {
        final Settings settings = Settings.builder()
            .put("index.sort.field", "field1")
            .build();
        IllegalArgumentException exc =
            expectThrows(IllegalArgumentException.class, () -> indexSettings(settings, Version.V_5_4_0));
        assertThat(exc.getMessage(),
            containsString("unsupported index.version.created:5.4.0, " +
                "can't set index.sort on versions prior to 6.0.0-alpha1"));
    }
}

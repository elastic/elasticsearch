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

package org.elasticsearch.search;

import org.apache.lucene.search.Sort;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.search.internal.ScrollContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.rescore.RescoreContext;
import org.elasticsearch.search.sort.SortAndFormats;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;

public class SearchContextTests extends ESTestCase {
    public void testBuilder() {
        int maxResultWindow = randomIntBetween(50, 100);
        int maxRescoreWindow = randomIntBetween(50, 100);
        Settings settings = Settings.builder()
            .put("index.max_result_window", maxResultWindow)
            .put("index.max_rescore_window", maxRescoreWindow)
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 2)
            .build();
        SearchContext.Builder builder = createTestSearchContext(settings);
        builder.setFrom(300);
        // resultWindow greater than maxResultWindow and scrollContext is null
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> builder.build(() -> {}));
        assertThat(exception.getMessage(), equalTo("Result window is too large, from + size must be less than or equal to:"
            + " [" + maxResultWindow + "] but was [310]. See the scroll api for a more efficient way to request large data sets. "
            + "This limit can be set by changing the [" + IndexSettings.MAX_RESULT_WINDOW_SETTING.getKey()
            + "] index level setting."));

        // resultWindow greater than maxResultWindow and scrollContext isn't null
        builder.setScroll(new ScrollContext());
        exception = expectThrows(IllegalArgumentException.class, () -> builder.build(() -> {}));
        assertThat(exception.getMessage(), equalTo("Batch size is too large, size must be less than or equal to: ["
            + maxResultWindow + "] but was [310]. Scroll batch sizes cost as much memory as result windows so they are "
            + "controlled by the [" + IndexSettings.MAX_RESULT_WINDOW_SETTING.getKey() + "] index level setting."));

        // resultWindow not greater than maxResultWindow and both rescore and sort are not null
        builder.setFrom(0);
        SortAndFormats sortAndFormats = new SortAndFormats(Sort.INDEXORDER, new DocValueFormat[]{DocValueFormat.RAW});
        builder.setSort(sortAndFormats);

        RescoreContext rescoreContext = new RescoreContext(500, null);
        builder.setRescorers(Collections.singletonList(rescoreContext));
        exception = expectThrows(IllegalArgumentException.class, () -> builder.build(() -> { }));
        assertThat(exception.getMessage(), equalTo("Cannot use [sort] option in conjunction with [rescore]."));

        // rescore is null but sort is not null and rescoreContext.getWindowSize() exceeds maxResultWindow
        builder.setSort(null);
        exception = expectThrows(IllegalArgumentException.class, () -> builder.build(() -> { }));

        assertThat(exception.getMessage(), equalTo("Rescore window [" + rescoreContext.getWindowSize() + "] is too large. "
            + "It must be less than [" + maxRescoreWindow + "]. This prevents allocating massive heaps for storing the results "
            + "to be rescored. This limit can be set by changing the [" + IndexSettings.MAX_RESCORE_WINDOW_SETTING.getKey()
            + "] index level setting."));
    }
}

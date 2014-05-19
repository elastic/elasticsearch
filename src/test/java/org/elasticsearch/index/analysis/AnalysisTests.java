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

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.util.CharArraySet;
import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.Matchers.is;

public class AnalysisTests extends ElasticsearchTestCase {
    @Test
    public void testParseStemExclusion() {

        /* Comma separated list */
        Settings settings = settingsBuilder().put("stem_exclusion", "foo,bar").build();
        CharArraySet set = Analysis.parseStemExclusion(settings, CharArraySet.EMPTY_SET, Version.CURRENT.luceneVersion);
        assertThat(set.contains("foo"), is(true));
        assertThat(set.contains("bar"), is(true));
        assertThat(set.contains("baz"), is(false));

        /* Array */
        settings = settingsBuilder().putArray("stem_exclusion", "foo","bar").build();
        set = Analysis.parseStemExclusion(settings, CharArraySet.EMPTY_SET, Version.CURRENT.luceneVersion);
        assertThat(set.contains("foo"), is(true));
        assertThat(set.contains("bar"), is(true));
        assertThat(set.contains("baz"), is(false));
    }

}

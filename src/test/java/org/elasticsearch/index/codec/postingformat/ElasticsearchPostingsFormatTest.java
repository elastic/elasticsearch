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

package org.elasticsearch.index.codec.postingformat;

import com.carrotsearch.randomizedtesting.annotations.Listeners;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.BasePostingsFormatTestCase;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.TimeUnits;
import org.elasticsearch.index.codec.postingsformat.Elasticsearch090PostingsFormat;
import org.elasticsearch.test.ElasticsearchThreadFilter;
import org.elasticsearch.test.junit.listeners.ReproduceInfoPrinter;

/** Runs elasticsearch postings format against lucene's standard postings format tests */
@Listeners({
        ReproduceInfoPrinter.class
})
@ThreadLeakFilters(defaultFilters = true, filters = {ElasticsearchThreadFilter.class})
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
@TimeoutSuite(millis = TimeUnits.HOUR)
@LuceneTestCase.SuppressSysoutChecks(bugUrl = "we log a lot on purpose")
public class ElasticsearchPostingsFormatTest extends BasePostingsFormatTestCase {

    @Override
    protected Codec getCodec() {
        return TestUtil.alwaysPostingsFormat(new Elasticsearch090PostingsFormat());
    }
    
}

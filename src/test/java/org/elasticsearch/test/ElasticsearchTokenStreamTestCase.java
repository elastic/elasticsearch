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

package org.elasticsearch.test;

import com.carrotsearch.randomizedtesting.annotations.Listeners;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TimeUnits;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.test.junit.listeners.ReproduceInfoPrinter;

@Listeners({
        ReproduceInfoPrinter.class
})
@TimeoutSuite(millis = TimeUnits.HOUR)
@LuceneTestCase.SuppressReproduceLine
@LuceneTestCase.SuppressSysoutChecks(bugUrl = "we log a lot on purpose")
/**
 * Basic test case for token streams. the assertion methods in this class will
 * run basic checks to enforce correct behavior of the token streams.
 */
public abstract class ElasticsearchTokenStreamTestCase extends BaseTokenStreamTestCase {

    static {
        SecurityHack.ensureInitialized();
    }
    
    public static Version randomVersion() {
        return VersionUtils.randomVersion(random());
    }

    public ImmutableSettings.Builder newAnalysisSettingsBuilder() {
        return ImmutableSettings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT);
    }
}

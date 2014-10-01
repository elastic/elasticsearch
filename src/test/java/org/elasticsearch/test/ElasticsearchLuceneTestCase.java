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

import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.annotations.*;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope.Scope;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.lucene.util.TimeUnits;
import org.elasticsearch.test.junit.listeners.ReproduceInfoPrinter;


/**
 * Base testcase for lucene based testing. This class should be used if low level lucene features are tested.
 */
@Listeners({
        ReproduceInfoPrinter.class
})
@ThreadLeakFilters(defaultFilters = true, filters = {ElasticsearchThreadFilter.class})
@ThreadLeakScope(Scope.NONE)
@TimeoutSuite(millis = TimeUnits.HOUR)
@SuppressCodecs("Lucene3x")
@LuceneTestCase.SuppressSysoutChecks(bugUrl = "we log a lot on purpose")
public abstract class ElasticsearchLuceneTestCase extends LuceneTestCase {

    private static final Codec DEFAULT_CODEC = Codec.getDefault();

    /**
     * Returns the lucene default codec without any randomization
     */
    public static Codec actualDefaultCodec() {
        return DEFAULT_CODEC;
    }

    /**
     * Forcefully reset the default codec
     */
    public static void forceDefaultCodec() {
        Codec.setDefault(DEFAULT_CODEC);
    }

    public static int scaledRandomIntBetween(int min, int max) {
        return RandomizedTest.scaledRandomIntBetween(min, max);
    }
}

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
package org.elasticsearch.common.lucene;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

import org.apache.lucene.util.Version;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.junit.Test;

/**
 * 
 */
public class LuceneTest {


    /*
     * simple test that ensures that we bump the version on Upgrade
     */
    @Test
    public void testVersion() {
        ESLogger logger = ESLoggerFactory.getLogger(LuceneTest.class.getName());
        Version[] values = Version.values();
        assertThat(Version.LUCENE_CURRENT, equalTo(values[values.length-1]));
        assertThat("Latest Lucene Version is not set after upgrade", Lucene.VERSION, equalTo(values[values.length-2]));
        assertThat(Lucene.parseVersion(null, Lucene.VERSION, null), equalTo(Lucene.VERSION));
        for (int i = 0; i < values.length-1; i++) {
            // this should fail if the lucene version is not mapped as a string in Lucene.java
            assertThat(Lucene.parseVersion(values[i].name().replaceFirst("^LUCENE_(\\d)(\\d)$", "$1.$2"), Version.LUCENE_CURRENT, logger), equalTo(values[i]));
        }
    }
}

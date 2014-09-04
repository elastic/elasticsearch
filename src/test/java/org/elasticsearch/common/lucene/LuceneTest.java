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
import org.apache.lucene.util.Version;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

/**
 * 
 */
public class LuceneTest extends ElasticsearchTestCase   {


    /*
     * simple test that ensures that we bump the version on Upgrade
     */
    @Test
    public void testVersion() {
        // note this is just a silly sanity check, we test it in lucene, and we point to it this way
        assertEquals(Lucene.VERSION, Version.LATEST);
    }
}

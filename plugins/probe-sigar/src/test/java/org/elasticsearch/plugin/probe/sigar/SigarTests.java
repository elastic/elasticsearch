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

package org.elasticsearch.plugin.probe.sigar;

import org.elasticsearch.test.ElasticsearchTestCase;
import org.hyperic.sigar.Sigar;
import org.junit.Test;

public class SigarTests extends ElasticsearchTestCase {
    
    @Override
    public void setUp() throws Exception {
        super.setUp();
        assumeTrue("we can only ensure sigar is working when running from maven", 
                   Boolean.parseBoolean(System.getProperty("tests.maven")));
    }

    @Test
    public void testSigarLoads() throws Exception {
        Sigar.load();
    }

    @Test
    public void testSigarWorks() throws Exception {
        Sigar sigar = new Sigar();
        assertNotNull(sigar.getCpu());
    }
}

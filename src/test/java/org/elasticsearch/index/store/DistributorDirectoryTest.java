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
package org.elasticsearch.index.store;

import com.carrotsearch.randomizedtesting.annotations.Listeners;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;
import org.apache.lucene.store.BaseDirectoryTestCase;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.TimeUnits;
import org.elasticsearch.test.ElasticsearchThreadFilter;
import org.elasticsearch.test.junit.listeners.LoggingListener;

import java.io.File;
import java.io.IOException;

@ThreadLeakFilters(defaultFilters = true, filters = {ElasticsearchThreadFilter.class})
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
@TimeoutSuite(millis = 20 * TimeUnits.MINUTE) // timeout the suite after 20min and fail the test.
@Listeners(LoggingListener.class)
public class DistributorDirectoryTest extends BaseDirectoryTestCase {

    @Override
    protected Directory getDirectory(File path) throws IOException {
        Directory[] directories = new Directory[1 + random().nextInt(5)];
        for (int i = 0; i < directories.length; i++) {
            directories[i] = newDirectory();
        }
        return new DistributorDirectory(directories);
    }

}

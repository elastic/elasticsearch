/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.util.lucene.versioned;

import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (Shay Banon)
 */
public abstract class AbstractVersionedMapTests {

    protected abstract VersionedMap create();

    @Test public void testSimple() {
        VersionedMap versionedMap = create();

        assertThat(true, equalTo(versionedMap.beforeVersion(1, 1)));
        assertThat(true, equalTo(versionedMap.beforeVersion(2, 2)));

        versionedMap.putVersion(1, 2);
        assertThat(true, equalTo(versionedMap.beforeVersion(1, 1)));
        assertThat(false, equalTo(versionedMap.beforeVersion(1, 2)));
        assertThat(true, equalTo(versionedMap.beforeVersion(2, 2)));

        versionedMap.putVersionIfAbsent(1, 0);
        assertThat(true, equalTo(versionedMap.beforeVersion(1, 1)));
        assertThat(true, equalTo(versionedMap.beforeVersion(2, 2)));

        versionedMap.putVersion(2, 1);
        assertThat(true, equalTo(versionedMap.beforeVersion(2, 0)));
        assertThat(false, equalTo(versionedMap.beforeVersion(2, 1)));
        assertThat(false, equalTo(versionedMap.beforeVersion(2, 2)));
        assertThat(false, equalTo(versionedMap.beforeVersion(2, 3)));
    }
}

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

package org.elasticsearch.index.refresh;

import org.elasticsearch.Version;
import org.elasticsearch.test.AbstractStreamableTestCase;

import java.io.IOException;

public class RefreshStatsTests extends AbstractStreamableTestCase<RefreshStats> {
    @Override
    protected RefreshStats createTestInstance() {
        return new RefreshStats(randomNonNegativeLong(), randomNonNegativeLong(), between(0, Integer.MAX_VALUE));
    }

    @Override
    protected RefreshStats createBlankInstance() {
        return new RefreshStats();
    }

    public void testPre5Dot2() throws IOException {
        // We can drop the compatibility once the assertion just below this list fails
        assertTrue(Version.CURRENT.minimumCompatibilityVersion().before(Version.V_5_2_0_UNRELEASED));

        RefreshStats instance = createTestInstance();
        RefreshStats copied = copyInstance(instance, Version.V_5_1_1_UNRELEASED);
        assertEquals(instance.getTotal(), copied.getTotal());
        assertEquals(instance.getTotalTimeInMillis(), copied.getTotalTimeInMillis());
        assertEquals(0, copied.getListeners());
    }
}

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
package org.elasticsearch.client.ilm;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import static org.hamcrest.Matchers.is;

public class WaitForSnapshotActionTests extends AbstractXContentTestCase<WaitForSnapshotAction> {

    @Override
    protected WaitForSnapshotAction doParseInstance(XContentParser parser) {
        return WaitForSnapshotAction.parse(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    static WaitForSnapshotAction randomInstance() {
        return new WaitForSnapshotAction(randomAlphaOfLength(5));
    }

    @Override
    protected WaitForSnapshotAction createTestInstance() {
        return randomInstance();
    }

    public void testActionWithEmptyOrNullPolicy() {
        {
            IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> new WaitForSnapshotAction(""));
            assertThat(exception.getMessage(), is("policy name must be specified"));
        }

        {
            IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> new WaitForSnapshotAction(null));
            assertThat(exception.getMessage(), is("policy name must be specified"));
        }
    }
}

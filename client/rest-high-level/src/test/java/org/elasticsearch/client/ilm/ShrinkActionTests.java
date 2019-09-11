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

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class ShrinkActionTests extends AbstractXContentTestCase<ShrinkAction> {

    @Override
    protected ShrinkAction doParseInstance(XContentParser parser) throws IOException {
        return ShrinkAction.parse(parser);
    }

    @Override
    protected ShrinkAction createTestInstance() {
        return randomInstance();
    }

    static ShrinkAction randomInstance() {
        return new ShrinkAction(randomIntBetween(1, 100));
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    public void testNonPositiveShardNumber() {
        Exception e = expectThrows(Exception.class, () -> new ShrinkAction(randomIntBetween(-100, 0)));
        assertThat(e.getMessage(), equalTo("[number_of_shards] must be greater than 0"));
    }
}

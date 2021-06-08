/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.io.stream.AbstractWriteableEnumTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class InterpolationTypeTests extends AbstractWriteableEnumTestCase {

    public InterpolationTypeTests() {
        super(BucketHelpers.InterpolationType::readFromStream);
    }

    @Override public void testValidOrdinals() {
        assertThat(BucketHelpers.InterpolationType.NONE.ordinal(), equalTo(0));
        assertThat(BucketHelpers.InterpolationType.LINEAR.ordinal(), equalTo(1));
    }

    @Override public void testFromString() {
        assertThat(BucketHelpers.InterpolationType.parse("none"), equalTo(BucketHelpers.InterpolationType.NONE));
        assertThat(BucketHelpers.InterpolationType.parse("linear"), equalTo(BucketHelpers.InterpolationType.LINEAR));
        final ElasticsearchParseException ex = expectThrows(ElasticsearchParseException.class,
            () -> BucketHelpers.InterpolationType.parse("try_to_parse"));
        assertEquals("Invalid interpolation type: [try_to_parse]", ex.getMessage());
    }

    @Override public void testReadFrom() throws IOException {
        assertReadFromStream(0, BucketHelpers.InterpolationType.NONE);
        assertReadFromStream(1, BucketHelpers.InterpolationType.LINEAR);
    }

    @Override public void testWriteTo() throws IOException {
        assertWriteToStream(BucketHelpers.InterpolationType.NONE, 0);
        assertWriteToStream(BucketHelpers.InterpolationType.LINEAR, 1);
    }
}

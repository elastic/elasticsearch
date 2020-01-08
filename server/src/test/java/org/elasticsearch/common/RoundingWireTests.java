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

package org.elasticsearch.common;

import org.elasticsearch.common.Rounding.DateTimeUnit;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class RoundingWireTests extends AbstractWireSerializingTestCase<Rounding> {
    @Override
    protected Rounding createTestInstance() {
        Rounding.Builder builder;
        if (randomBoolean()) {
            builder = Rounding.builder(randomFrom(DateTimeUnit.values()));
        } else {
            // The time value's millisecond component must be > 0 so we're limited in the suffixes we can use.
            final var tv = randomTimeValue(1, 1000, "d", "h", "ms", "s", "m");
            builder = Rounding.builder(TimeValue.parseTimeValue(tv, "test"));
        }
        if (randomBoolean()) {
            builder.timeZone(randomZone());
        }
        if (randomBoolean()) {
            builder.offset(randomLong());
        }
        return builder.build();
    }

    @Override
    protected Reader<Rounding> instanceReader() {
        return Rounding::read;
    }
}

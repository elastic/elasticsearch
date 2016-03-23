/*
x * Licensed to Elasticsearch under one or more contributor
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

package org.elasticsearch.search.sort;

import java.io.IOException;

public class FieldSortBuilderTests extends AbstractSortTestCase<FieldSortBuilder> {

    @Override
    protected FieldSortBuilder createTestItem() {
        return randomFieldSortBuilder();
    }

    public static FieldSortBuilder randomFieldSortBuilder() {
        String fieldName = rarely() ? SortParseElement.DOC_FIELD_NAME : randomAsciiOfLengthBetween(1, 10);
        FieldSortBuilder builder = new FieldSortBuilder(fieldName);
        if (randomBoolean()) {
            builder.order(RandomSortDataGenerator.order(null));
        }

        if (randomBoolean()) {
            builder.missing(RandomSortDataGenerator.missing(builder.missing()));
        }

        if (randomBoolean()) {
            builder.unmappedType(RandomSortDataGenerator.randomAscii(builder.unmappedType()));
        }

        if (randomBoolean()) {
            builder.sortMode(RandomSortDataGenerator.mode(builder.sortMode()));
        }

        if (randomBoolean()) {
            builder.setNestedFilter(RandomSortDataGenerator.nestedFilter(builder.getNestedFilter()));
        }

        if (randomBoolean()) {
            builder.setNestedPath(RandomSortDataGenerator.randomAscii(builder.getNestedPath()));
        }

        return builder;
    }

    @Override
    protected FieldSortBuilder mutate(FieldSortBuilder original) throws IOException {
        FieldSortBuilder mutated = new FieldSortBuilder(original);
        int parameter = randomIntBetween(0, 5);
        switch (parameter) {
        case 0:
            mutated.setNestedPath(RandomSortDataGenerator.randomAscii(mutated.getNestedPath()));
            break;
        case 1:
            mutated.setNestedFilter(RandomSortDataGenerator.nestedFilter(mutated.getNestedFilter()));
            break;
        case 2:
            mutated.sortMode(RandomSortDataGenerator.mode(mutated.sortMode()));
            break;
        case 3:
            mutated.unmappedType(RandomSortDataGenerator.randomAscii(mutated.unmappedType()));
            break;
        case 4:
            mutated.missing(RandomSortDataGenerator.missing(mutated.missing()));
            break;
        case 5:
            mutated.order(RandomSortDataGenerator.order(mutated.order()));
            break;
        default:
            throw new IllegalStateException("Unsupported mutation.");
        }
        return mutated;
    }
}

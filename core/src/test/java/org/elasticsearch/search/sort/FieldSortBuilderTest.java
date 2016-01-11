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

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;

import java.io.IOException;

public class FieldSortBuilderTest extends AbstractSearchSourceItemTestCase<FieldSortBuilder> {

    @SuppressWarnings("unchecked")
    public Class<FieldSortBuilder> getPrototype() {
        return (Class<FieldSortBuilder>) FieldSortBuilder.PROTOTYPE.getClass();
    }

    @Override
    protected FieldSortBuilder createTestItem() {
        String fieldName = randomAsciiOfLengthBetween(1, 10);
        FieldSortBuilder builder = new FieldSortBuilder(fieldName);
        if (randomBoolean()) {
            order(builder);
        }

        if (randomBoolean()) {
            missing(builder);
        }

        if (randomBoolean()) {
            unmappedType(builder);
        }

        if (randomBoolean()) {
            modes(builder);
        }

        if (randomBoolean()) {
            nestedFilter(builder);
        }

        if (randomBoolean()) {
            nestedPath(builder);
        }
        
        return builder;
    }

    private void nestedFilter(FieldSortBuilder builder) {
        @SuppressWarnings("rawtypes")
        QueryBuilder nested = null;
        while (nested == null || nested.equals(builder.getNestedFilter())) {
            switch (randomInt(2)) {
            case 0:
                nested = new MatchAllQueryBuilder();
                break;
            case 1:
                nested = new IdsQueryBuilder();
                break;
            default:
            case 2:
                nested = new TermQueryBuilder(randomAsciiOfLengthBetween(1, 10), randomAsciiOfLengthBetween(1, 10));
                break;
            }
            nested.boost((float) randomDoubleBetween(0, 10, false));
        }
        builder.setNestedFilter(nested);
    }

    private void nestedPath(FieldSortBuilder builder) {
        String nestedPath = randomAsciiOfLengthBetween(1, 10);
        while (nestedPath.equals(builder.getNestedPath())) {
            nestedPath = randomAsciiOfLengthBetween(1, 10);
        }
        builder.setNestedPath(nestedPath);
    }

    private void modes(FieldSortBuilder builder) {
        String[] modes = {"min", "max", "avg", "sum"};
        String mode = randomFrom(modes);
        while (mode.equals(builder.sortMode())) {
            mode = randomFrom(modes);
        }
        builder.sortMode(mode);
    }
    
    private void missing(FieldSortBuilder builder) {
        Object missing = null;
        Object otherMissing = null;
        if (builder.missing() instanceof BytesRef) {
            otherMissing = ((BytesRef) builder.missing()).utf8ToString();
        } else {
            otherMissing = builder.missing();
        }

        while (missing == null || missing.equals(otherMissing)) {
          int missingId = randomIntBetween(0, 3);
          switch (missingId) {
          case 0:
              missing = ("_last");
              break;
          case 1:
              missing = ("_first");
              break;
          case 2:
              missing = randomAsciiOfLength(10);
              break;
          case 3:
              missing = randomInt();
              break;
          default:
              throw new IllegalStateException("Unknown missing type.");
              
          }
        }
        builder.missing(missing);
    }
    
    public void unmappedType(FieldSortBuilder builder) {
        String type = randomAsciiOfLengthBetween(1, 10);
        while (type.equals(builder.unmappedType())) {
            type = randomAsciiOfLengthBetween(1, 10);
        }
        builder.unmappedType(type);
    }
    
    public void order(FieldSortBuilder builder) {
        SortOrder order = SortOrder.ASC;
        if (order.equals(builder.order())) {
            builder.order(SortOrder.DESC);
        } else {
            builder.order(SortOrder.ASC);
        }
    }

    @Override
    protected FieldSortBuilder mutate(FieldSortBuilder original) throws IOException {
        FieldSortBuilder mutated = new FieldSortBuilder(original);
        int parameter = randomIntBetween(0, 5);
        switch (parameter) {
        case 0:
            nestedPath(mutated);
            break;
        case 1:
            nestedFilter(mutated);
            break;
        case 2:
            modes(mutated);
            break;
        case 3:
            unmappedType(mutated);
            break;
        case 4:
            missing(mutated);
            break;
        case 5:
            order(mutated);
            break;
        default:
            throw new IllegalStateException("Unsupported mutation.");
        }
        return mutated;
        
    }
}

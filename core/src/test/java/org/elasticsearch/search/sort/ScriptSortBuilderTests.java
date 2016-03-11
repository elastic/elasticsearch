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

package org.elasticsearch.search.sort;


import org.elasticsearch.script.Script;

import java.io.IOException;

public class ScriptSortBuilderTests extends AbstractSortTestCase<ScriptSortBuilder> {

    @Override
    protected ScriptSortBuilder createTestItem() {
        ScriptSortBuilder builder = new ScriptSortBuilder(new Script(randomAsciiOfLengthBetween(5, 10)),
                randomBoolean() ? ScriptSortParser.NUMBER_SORT_TYPE : ScriptSortParser.STRING_SORT_TYPE);
        if (randomBoolean()) {
            builder.order(RandomSortDataGenerator.order(builder.order()));
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
    protected ScriptSortBuilder mutate(ScriptSortBuilder original) throws IOException {
        ScriptSortBuilder result;
        if (randomBoolean()) {
            // change one of the constructor args, copy the rest over
            Script script = original.script();
            String type = original.type();
            if (randomBoolean()) {
                result = new ScriptSortBuilder(new Script(script.getScript() + "_suffix"), type);
            } else {
                result = new ScriptSortBuilder(script, type + "_suffix");
            }
            result.order(original.order());
            result.sortMode(original.sortMode());
            result.setNestedFilter(original.getNestedFilter());
            result.setNestedPath(original.getNestedPath());
            return result;
        }
        result = new ScriptSortBuilder(original);
        switch (randomIntBetween(0, 3)) {
            case 0:
                if (original.order() == SortOrder.ASC) {
                    result.order(SortOrder.DESC);
                } else {
                    result.order(SortOrder.ASC);
                }
                break;
            case 1:
                result.sortMode(RandomSortDataGenerator.mode(original.sortMode()));
                break;
            case 2:
                result.setNestedFilter(RandomSortDataGenerator.nestedFilter(original.getNestedFilter()));
                break;
            case 3:
                result.setNestedPath(original.getNestedPath() + "_some_suffix");
                break;
        }
        return result;
    }
}

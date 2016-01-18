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

package org.elasticsearch.search.suggest.phrase;

import org.elasticsearch.search.suggest.phrase.PhraseSuggestionBuilder.StupidBackoff;

public class StupidBackoffModelTests extends SmoothingModelTest<StupidBackoff> {

    @Override
    protected StupidBackoff createTestModel() {
        return new StupidBackoff(randomDoubleBetween(0.0, 10.0, false));
    }

    /**
     * mutate the given model so the returned smoothing model is different
     */
    @Override
    protected StupidBackoff createMutation(StupidBackoff original) {
        return new StupidBackoff(original.getDiscount() + 0.1);
    }
}

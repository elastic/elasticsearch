/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsearch.examples.nativescript.script.stockaggs;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.script.AbstractExecutableScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.NativeScriptFactory;

import java.util.ArrayList;
import java.util.Map;

/**
 * Combine script from https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-scripted-metric-aggregation.html
 * <p>
 * profit = 0; for (t in _agg.transactions) { profit += t }; return profit
 */
public class CombineScriptFactory implements NativeScriptFactory {

    @Override
    @SuppressWarnings("unchecked")
    public ExecutableScript newScript(final @Nullable Map<String, Object> params) {
        Map<String, Object> agg = (Map<String, Object>) params.get("_agg");
        final ArrayList<Long> transactions = (ArrayList<Long>) agg.get(InitScriptFactory.TRANSACTIONS_FIELD);
        return new CombineScript(transactions);
    }

    @Override
    public boolean needsScores() {
        return false;
    }

    private static class CombineScript extends AbstractExecutableScript {

        private final ArrayList<Long> transactions;

        public CombineScript(ArrayList<Long> transactions) {
            this.transactions = transactions;
        }

        @Override
        public Object run() {
            long profit = 0;
            for (long t : transactions) {
                profit += t;
            }
            return profit;
        }
    }
}

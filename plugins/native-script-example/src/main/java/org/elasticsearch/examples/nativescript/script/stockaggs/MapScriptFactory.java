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
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.script.AbstractSearchScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.NativeScriptFactory;

import java.util.ArrayList;
import java.util.Map;

/**
 * Map script from
 * https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-scripted-metric-aggregation.html
 * <p>
 * if (doc['type'].value == \"sale\") { _agg.transactions.add(doc['amount'].value) } else {_agg.transactions.add(-1 * doc['amount'].value)}
 */
public class MapScriptFactory implements NativeScriptFactory {

    @Override
    @SuppressWarnings("unchecked")
    public ExecutableScript newScript(@Nullable final Map<String, Object> params) {
        Map<String, Object> agg = (Map<String, Object>) params.get("_agg");
        ArrayList<Long> transactions = (ArrayList<Long>) agg.get(InitScriptFactory.TRANSACTIONS_FIELD);
        return new MapScript(transactions);
    }

    @Override
    public boolean needsScores() {
        return false;
    }

    @Override
    public String getName() {
        return "stockaggs_map";
    }

    private static class MapScript extends AbstractSearchScript {

        private final ArrayList<Long> transactions;

        public MapScript(ArrayList<Long> transactions) {
            this.transactions = transactions;

        }

        @Override
        public Object run() {
            ScriptDocValues.Longs amount = (ScriptDocValues.Longs) doc().get("amount");
            ScriptDocValues.Strings type = (ScriptDocValues.Strings) doc().get("type");
            if ("sale".equals(type.getValue())) {
                transactions.add(amount.getValue());
            } else {
                transactions.add(-amount.getValue());
            }
            return null;
        }
    }
}

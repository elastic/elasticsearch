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

package org.elasticsearch.examples.nativescript.script;

import org.apache.lucene.search.Scorer;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.script.AbstractDoubleSearchScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.NativeScriptFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Factory for the script that boosts score of a record based on a value of  the record's field.
 * <p>
 * This native script demonstrates how to write native custom scores scripts.
 */
public class PopularityScoreScriptFactory implements NativeScriptFactory {

    @Override
    public ExecutableScript newScript(@Nullable Map<String, Object> params) {
        String fieldName = params == null ? null : XContentMapValues.nodeStringValue(params.get("field"), null);
        if (fieldName == null) {
            throw new IllegalArgumentException("Missing the field parameter");
        }
        return new PopularityScoreScript(fieldName);
    }

    /**
     * Indicates if document scores may be needed by the produced scripts.
     *
     * @return {@code true} if scores are needed.
     */
    @Override
    public boolean needsScores() {
        return true;
    }

    @Override
    public String getName() {
        return "popularity";
    }

    /**
     * This script takes a numeric value from the field specified in the parameter field. And calculates boost
     * for the record using the following formula: 1 + log10(field_value + 1). So, records with value 0 in the field
     * get no boost. Records with value 9 gets boost of 2.0, records with value 99, gets boost of 3, 999 - 4 and so on.
     */
    private static class PopularityScoreScript extends AbstractDoubleSearchScript {

        private final String field;

        private Scorer scorer;

        public PopularityScoreScript(String field) {
            this.field = field;
        }

        @Override
        public void setScorer(Scorer scorer) {
            this.scorer = scorer;
        }

        @Override
        @SuppressWarnings("unchecked")
        public double runAsDouble() {
            try {
                ScriptDocValues<Long> docValue = (ScriptDocValues<Long>) doc().get(field);
                if (docValue != null && !docValue.isEmpty()) {
                    ScriptDocValues.Longs fieldData = (ScriptDocValues.Longs) docValue;
                    double boost = 1 + Math.log10(fieldData.getValue() + 1);
                    // Because this script is used in custom_score script the value of score() is populated.
                    // In all other cases doc().getScore() should be used instead.
                    return boost * scorer.score();

                }
                return scorer.score();
            } catch (IOException ex) {
                return 0.0;
            }
        }

        @Override
        public Object run() {
            return null;
        }
    }
}

package org.elasticsearch.examples.nativescript.script;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.script.AbstractFloatSearchScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.NativeScriptFactory;
import org.elasticsearch.script.ScriptException;

import java.util.Map;

/**
 * Factory for the script that boosts score of a record based on a value of  the record's field.
 * <p/>
 * This native script demonstrates how to write native custom scores scripts.
 */
public class PopularityScoreScriptFactory implements NativeScriptFactory {

    @Override
    public ExecutableScript newScript(@Nullable Map<String, Object> params) {
        String fieldName = params == null ? null : XContentMapValues.nodeStringValue(params.get("field"), null);
        if (fieldName == null) {
            throw new ScriptException("Missing the field parameter");
        }
        return new PopularityScoreScript(fieldName);
    }


    /**
     * This script takes a numeric value from the field specified in the parameter field. And calculates boost
     * for the record using the following formula: 1 + log10(field_value + 1). So, records with value 0 in the field
     * get no boost. Records with value 9 gets boost of 2.0, records with value 99, gets boost of 3, 999 - 4 and so on.
     */
    private static class PopularityScoreScript extends AbstractFloatSearchScript {

        private final String field;

        public PopularityScoreScript(String field) {
            this.field = field;
        }

        @Override
        public float runAsFloat() {
            ScriptDocValues docValue = (ScriptDocValues) doc().get(field);
            if (docValue != null && !docValue.isEmpty()) {
                ScriptDocValues.Longs fieldData = (ScriptDocValues.Longs) docValue;
                double boost = 1 + Math.log10(fieldData.getValue() + 1);
                // Because this script is used in custom_score script the value of score() is populated.
                // In all other cases doc().getScore() should be used instead.
                return (float) boost * score();
            }
            return score();
        }
    }
}

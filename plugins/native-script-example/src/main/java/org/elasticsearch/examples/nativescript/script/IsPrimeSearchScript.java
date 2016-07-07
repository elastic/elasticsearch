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

import java.math.BigInteger;
import java.util.Map;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.ScriptDocValues.Longs;
import org.elasticsearch.script.AbstractSearchScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.NativeScriptFactory;

/**
 * Implementation of the native script that checks that the field exists and contains a prime number.
 * <p>
 * The native script has to implement {@link org.elasticsearch.script.SearchScript} interface. But the
 * {@link org.elasticsearch.script.AbstractSearchScript} class can be used to simplify the implementation.
 */
public class IsPrimeSearchScript extends AbstractSearchScript {

    /**
     * Native scripts are build using factories that are registered in the
     * {@link org.elasticsearch.examples.nativescript.plugin.NativeScriptExamplesPlugin#onModule(org.elasticsearch.script.ScriptModule)}
     * method when plugin is loaded.
     */
    public static class Factory implements NativeScriptFactory {

        /**
         * This method is called for every search on every shard.
         *
         * @param params list of script parameters passed with the query
         * @return new native script
         */
        @Override
        public ExecutableScript newScript(@Nullable Map<String, Object> params) {
            // Example of a mandatory string parameter
            // The XContentMapValues helper class can be used to simplify parameter parsing
            String fieldName = params == null ? null : XContentMapValues.nodeStringValue(params.get("field"), null);
            if (fieldName == null) {
                throw new IllegalArgumentException("Missing the field parameter");
            }

            // Example of an optional integer  parameter
            int certainty = params == null ? 10 : XContentMapValues.nodeIntegerValue(params.get("certainty"), 10);
            return new IsPrimeSearchScript(fieldName, certainty);
        }

        /**
         * Indicates if document scores may be needed by the produced scripts.
         *
         * @return {@code true} if scores are needed.
         */
        @Override
        public boolean needsScores() {
            return false;
        }

        @Override
        public String getName() {
            return "is_prime";
        }

    }

    private final String fieldName;

    private final int certainty;

    /**
     * Factory creates this script on every
     *
     * @param fieldName the name of the field that should be checked
     * @param certainty the required certainty for the number to be prime
     */
    private IsPrimeSearchScript(String fieldName, int certainty) {
        this.fieldName = fieldName;
        this.certainty = certainty;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object run() {
        // First we get field using doc lookup
        ScriptDocValues<Long> docValue = (ScriptDocValues<Long>) doc().get(fieldName);
        // Check if field exists
        if (docValue != null && !docValue.isEmpty()) {
            try {
                // Try to parse it as an integer
                BigInteger bigInteger = BigInteger.valueOf(((Longs) docValue).getValue());
                // Check if it's prime
                return bigInteger.isProbablePrime(certainty);
            } catch (NumberFormatException ex) {
                return false;
            }
        }
        return false;
    }
}

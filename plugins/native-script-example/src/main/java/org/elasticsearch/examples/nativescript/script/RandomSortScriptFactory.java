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

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.script.AbstractLongSearchScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.NativeScriptFactory;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.Random;

/**
 * This script demonstrates how native scripts can be used to create custom sort order.
 * Since sort operation is expecting float parameter, the {@link AbstractLongSearchScript} can be used.
 * <p>
 * The script accepts one optional parameter salt. If parameter is specified, a pseudo random sort order is used.
 * Otherwise, a random sort order is used.
 */
public class RandomSortScriptFactory implements NativeScriptFactory {

    /**
     * This method is called for every search on every shard.
     *
     * @param params list of script parameters passed with the query
     * @return new native script
     */
    @Override
    public ExecutableScript newScript(@Nullable Map<String, Object> params) {
        String salt = params == null ? null : XContentMapValues.nodeStringValue(params.get("salt"), null);
        if (salt == null) {
            return new RandomSortScript();
        } else {
            return new PseudoRandomSortScript(salt);
        }
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
        return "random";
    }

    private static class RandomSortScript extends AbstractLongSearchScript {
        private final Random random;

        private RandomSortScript() {
            random = Randomness.get();
        }

        @Override
        public long runAsLong() {
            return random.nextLong();
        }
    }

    private static class PseudoRandomSortScript extends AbstractLongSearchScript {
        private final String salt;

        private PseudoRandomSortScript(String salt) {
            this.salt = salt;
        }

        @Override
        public long runAsLong() {
            ScriptDocValues.Strings fieldData = (ScriptDocValues.Strings) doc().get(UidFieldMapper.NAME);
            try {
                MessageDigest m = MessageDigest.getInstance("MD5");
                m.reset();
                m.update((fieldData.getValue() + salt).getBytes(StandardCharsets.UTF_8));
                byte[] sort = m.digest();
                return (sort[0] & 0xFFL) << 56
                        | (sort[1] & 0xFFL) << 48
                        | (sort[2] & 0xFFL) << 40
                        | (sort[3] & 0xFFL) << 32
                        | (sort[4] & 0xFFL) << 24
                        | (sort[5] & 0xFFL) << 16
                        | (sort[6] & 0xFFL) << 8
                        | (sort[7] & 0xFFL);
            } catch (NoSuchAlgorithmException ex) {
                return -1;
            }
        }
    }
}

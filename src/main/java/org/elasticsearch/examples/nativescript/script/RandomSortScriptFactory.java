package org.elasticsearch.examples.nativescript.script;

import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.script.AbstractFloatSearchScript;
import org.elasticsearch.script.AbstractLongSearchScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.NativeScriptFactory;
import sun.security.provider.MD5;

import java.util.Map;
import java.util.Random;

/**
 * This script demonstrates how native scripts can be used to create custom sort order.
 * Since sort operation is expecting float parameter, the {@link AbstractFloatSearchScript} can be used.
 *
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

    private static class RandomSortScript extends AbstractLongSearchScript {
        private final Random random;

        private RandomSortScript() {
            random = new Random();
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
            ScriptDocValues.Strings fieldData = (ScriptDocValues.Strings)doc().get(UidFieldMapper.NAME);
            byte[] sort = org.elasticsearch.common.Digest.md5(fieldData.getValue() + salt);
            return (sort[0] & 0xFFL) << 56
                    | (sort[1] & 0xFFL) << 48
                    | (sort[2] & 0xFFL) << 40
                    | (sort[3] & 0xFFL) << 32
                    | (sort[4] & 0xFFL) << 24
                    | (sort[5] & 0xFFL) << 16
                    | (sort[6] & 0xFFL) << 8
                    | (sort[7] & 0xFFL);
        }
    }
}

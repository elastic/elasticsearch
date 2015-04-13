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

package org.elasticsearch.index.query;

import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * A filter that restricts search results to values that have a matching regular expression in a given
 * field.
 *
 *
 */
public class RegexpFilterBuilder extends BaseFilterBuilder {

    private final String name;
    private final String regexp;
    private int flags = -1;
    private int maxDeterminizedStates = Operations.DEFAULT_MAX_DETERMINIZED_STATES;
    private boolean maxDetermizedStatesSet;

    private Boolean cache;
    private String cacheKey;
    private String filterName;

    /**
     * A filter that restricts search results to values that have a matching prefix in a given
     * field.
     *
     * @param name   The field name
     * @param regexp The regular expression
     */
    public RegexpFilterBuilder(String name, String regexp) {
        this.name = name;
        this.regexp = regexp;
    }

    /**
     * Sets the filter name for the filter that can be used when searching for matched_filters per hit.
     */
    public RegexpFilterBuilder filterName(String filterName) {
        this.filterName = filterName;
        return this;
    }

    /**
     * Sets the regexp flags (see {@link RegexpFlag}).
     */
    public RegexpFilterBuilder flags(RegexpFlag... flags) {
        int value = 0;
        if (flags.length == 0) {
            value = RegexpFlag.ALL.value;
        } else {
            for (RegexpFlag flag : flags) {
                value |= flag.value;
            }
        }
        this.flags = value;
        return this;
    }

    /**
     * Sets the regexp maxDeterminizedStates.
     */
    public RegexpFilterBuilder maxDeterminizedStates(int value) {
        this.maxDeterminizedStates = value;
        this.maxDetermizedStatesSet = true;
        return this;
    }

    /**
     * Should the filter be cached or not. Defaults to <tt>false</tt>.
     */
    public RegexpFilterBuilder cache(boolean cache) {
        this.cache = cache;
        return this;
    }

    public RegexpFilterBuilder cacheKey(String cacheKey) {
        this.cacheKey = cacheKey;
        return this;
    }

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(RegexpFilterParser.NAME);
        if (flags < 0) {
            builder.field(name, regexp);
        } else {
            builder.startObject(name)
                    .field("value", regexp)
                    .field("flags_value", flags);
            if (maxDetermizedStatesSet) {
                builder.field("max_determinized_states", maxDeterminizedStates);
            }
            builder.endObject();
        }

        if (filterName != null) {
            builder.field("_name", filterName);
        }
        if (cache != null) {
            builder.field("_cache", cache);
        }
        if (cacheKey != null) {
            builder.field("_cache_key", cacheKey);
        }
        builder.endObject();
    }
}

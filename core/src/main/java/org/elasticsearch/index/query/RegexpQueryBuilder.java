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
 * A Query that does fuzzy matching for a specific value.
 */
public class RegexpQueryBuilder extends AbstractQueryBuilder<RegexpQueryBuilder> implements MultiTermQueryBuilder<RegexpQueryBuilder> {

    public static final String NAME = "regexp";
    private final String name;
    private final String regexp;

    private int flags = RegexpQueryParser.DEFAULT_FLAGS_VALUE;
    
    private String rewrite;
    private int maxDeterminizedStates = Operations.DEFAULT_MAX_DETERMINIZED_STATES;
    private boolean maxDetermizedStatesSet;
    static final RegexpQueryBuilder PROTOTYPE = new RegexpQueryBuilder(null, null);

    /**
     * Constructs a new term query.
     *
     * @param name  The name of the field
     * @param regexp The regular expression
     */
    public RegexpQueryBuilder(String name, String regexp) {
        this.name = name;
        this.regexp = regexp;
    }

    public RegexpQueryBuilder flags(RegexpFlag... flags) {
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
    public RegexpQueryBuilder maxDeterminizedStates(int value) {
        this.maxDeterminizedStates = value;
        this.maxDetermizedStatesSet = true;
        return this;
    }

    public RegexpQueryBuilder rewrite(String rewrite) {
        this.rewrite = rewrite;
        return this;
    }

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.startObject(name);
        builder.field("value", regexp);
        if (flags != -1) {
            builder.field("flags_value", flags);
        }
        if (maxDetermizedStatesSet) {
            builder.field("max_determinized_states", maxDeterminizedStates);
        }
        if (rewrite != null) {
            builder.field("rewrite", rewrite);
        }
        printBoostAndQueryName(builder);
        builder.endObject();
        builder.endObject();
    }

    @Override
    public String getName() {
        return NAME;
    }
}

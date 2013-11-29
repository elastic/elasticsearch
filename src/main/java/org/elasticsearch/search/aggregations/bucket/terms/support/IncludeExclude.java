/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.bucket.terms.support;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.UnicodeUtil;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Defines the include/exclude regular expression filtering for string terms aggregation. In this filtering logic,
 * exclusion has precedence, where the {@code include} is evaluated first and then the {@code exclude}.
 */
public class IncludeExclude {

    private final Matcher include;
    private final Matcher exclude;
    private final CharsRef scratch = new CharsRef();

    /**
     * @param include   The regular expression pattern for the terms to be included
     *                  (may only be {@code null} if {@code exclude} is not {@code null}
     * @param exclude   The regular expression pattern for the terms to be excluded
     *                  (may only be {@code null} if {@code include} is not {@code null}
     */
    public IncludeExclude(Pattern include, Pattern exclude) {
        assert include != null || exclude != null : "include & exclude cannot both be null"; // otherwise IncludeExclude object should be null
        this.include = include != null ? include.matcher("") : null;
        this.exclude = exclude != null ? exclude.matcher("") : null;
    }

    /**
     * Returns whether the given value is accepted based on the {@code include} & {@code exclude} patterns.
     */
    public boolean accept(BytesRef value) {
        UnicodeUtil.UTF8toUTF16(value, scratch);
        if (include == null) {
            // exclude must not be null
            return !exclude.reset(scratch).matches();
        }
        if (!include.reset(scratch).matches()) {
            return false;
        }
        if (exclude == null) {
            return true;
        }
        return !exclude.reset(scratch).matches();
    }
}

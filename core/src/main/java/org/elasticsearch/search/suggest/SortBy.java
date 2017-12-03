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

package org.elasticsearch.search.suggest;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

/**
 * An enum representing the valid sorting options
 */
public enum SortBy implements Writeable {
    /** Sort should first be based on score, then document frequency and then the term itself. */
    SCORE,
    /** Sort should first be based on document frequency, then score and then the term itself. */
    FREQUENCY;

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeEnum(this);
    }

    public static SortBy readFromStream(final StreamInput in) throws IOException {
        return in.readEnum(SortBy.class);
    }

    public static SortBy resolve(final String str) {
        Objects.requireNonNull(str, "Input string is null");
        return valueOf(str.toUpperCase(Locale.ROOT));
    }
}

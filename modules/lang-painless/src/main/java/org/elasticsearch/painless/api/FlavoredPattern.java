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

package org.elasticsearch.painless.api;

import org.elasticsearch.dissect.DissectParser;
import org.elasticsearch.grok.Grok;

import java.util.Map;

/**
 * A simpler matcher that we can implement against grok and dissect.
 */
public interface FlavoredPattern {
    /**
     * Returns a {@link Map} containing all named capture groups if the
     * string matches or {@code null} if it doesn't.
     */
    Map<String, ?> map(String in);

    class ForGrok implements FlavoredPattern {
        private final Grok grok;

        public ForGrok(Grok grok) {
            this.grok = grok;
        }

        @Override
        public Map<String, ?> map(String in) {
            return grok.captures(in);
        }
    }

    class ForDissect implements FlavoredPattern {
        private final DissectParser dissect;

        public ForDissect(DissectParser dissect) {
            this.dissect = dissect;
        }

        @Override
        public Map<String, ?> map(String in) {
            return dissect.parse(in);
        }
    }
}

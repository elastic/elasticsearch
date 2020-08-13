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

package org.elasticsearch.indices;

import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.elasticsearch.common.regex.Regex;

import java.util.Objects;

/**
 * Describes a system index. Provides the information required to create and maintain the system index.
 */
public class SystemIndexDescriptor {
    private final String indexPattern;
    private final String description;
    private final CharacterRunAutomaton indexPatternAutomaton;

    /**
     *
     * @param indexPattern The pattern of index names that this descriptor will be used for. Must start with a '.' character.
     * @param description The name of the plugin responsible for this system index.
     */
    public SystemIndexDescriptor(String indexPattern, String description) {
        Objects.requireNonNull(indexPattern, "system index pattern must not be null");
        if (indexPattern.length() < 2) {
            throw new IllegalArgumentException("system index pattern provided as [" + indexPattern +
                "] but must at least 2 characters in length");
        }
        if (indexPattern.charAt(0) != '.') {
            throw new IllegalArgumentException("system index pattern provided as [" + indexPattern +
                "] but must start with the character [.]");
        }
        if (indexPattern.charAt(1) == '*') {
            throw new IllegalArgumentException("system index pattern provided as [" + indexPattern +
                "] but must not start with the character sequence [.*] to prevent conflicts");
        }
        this.indexPattern = indexPattern;
        this.indexPatternAutomaton = new CharacterRunAutomaton(Regex.simpleMatchToAutomaton(indexPattern));
        this.description = description;
    }

    /**
     * @return The pattern of index names that this descriptor will be used for.
     */
    public String getIndexPattern() {
        return indexPattern;
    }

    /**
     * Checks whether an index name matches the system index name pattern for this descriptor.
     * @param index The index name to be checked against the index pattern given at construction time.
     * @return True if the name matches the pattern, false otherwise.
     */
    public boolean matchesIndexPattern(String index) {
        return indexPatternAutomaton.run(index);
    }

    /**
     * @return A short description of the purpose of this system index.
     */
    public String getDescription() {
        return description;
    }

    @Override
    public String toString() {
        return "SystemIndexDescriptor[pattern=[" + indexPattern + "], description=[" + description + "]]";
    }

    // TODO: Index settings and mapping
    // TODO: getThreadpool()
    // TODO: Upgrade handling (reindex script?)
}

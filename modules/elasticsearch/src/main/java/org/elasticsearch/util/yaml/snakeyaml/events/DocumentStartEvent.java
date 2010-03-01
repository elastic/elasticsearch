/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
package org.elasticsearch.util.yaml.snakeyaml.events;

import org.elasticsearch.util.yaml.snakeyaml.error.Mark;

import java.util.Map;

/**
 * Marks the beginning of a document.
 * <p>
 * This event followed by the document's content and a {@link DocumentEndEvent}.
 * </p>
 */
public final class DocumentStartEvent extends Event {
    private final boolean explicit;
    private final Integer[] version;
    private final Map<String, String> tags;

    public DocumentStartEvent(Mark startMark, Mark endMark, boolean explicit, Integer[] version,
                              Map<String, String> tags) {
        super(startMark, endMark);
        this.explicit = explicit;
        this.version = version;
        this.tags = tags;
    }

    public boolean getExplicit() {
        return explicit;
    }

    /**
     * YAML version the document conforms to.
     *
     * @return <code>null</code>if the document has no explicit
     *         <code>%YAML</code> directive. Otherwise an array with two
     *         components, the major and minor part of the version (in this
     *         order).
     */
    public Integer[] getVersion() {
        return version;
    }

    /**
     * Tag shorthands as defined by the <code>%TAG</code> directive.
     *
     * @return Mapping of 'handles' to 'prefixes' (the handles include the '!'
     *         characters).
     */
    public Map<String, String> getTags() {
        return tags;
    }

    @Override
    public boolean is(ID id) {
        return ID.DocumentStart == id;
    }
}

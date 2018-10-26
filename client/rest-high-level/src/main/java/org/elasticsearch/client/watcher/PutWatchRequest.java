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
package org.elasticsearch.client.watcher;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * This request class contains the data needed to create a watch along with the name of the watch.
 * The name of the watch will become the ID of the indexed document.
 */
public final class PutWatchRequest implements Validatable {

    private static final Pattern NO_WS_PATTERN = Pattern.compile("\\S+");

    private final String id;
    private final BytesReference source;
    private final XContentType xContentType;
    private boolean active = true;
    private long version = Versions.MATCH_ANY;

    public PutWatchRequest(String id, BytesReference source, XContentType xContentType) {
        Objects.requireNonNull(id, "watch id is missing");
        if (isValidId(id) == false) {
            throw new IllegalArgumentException("watch id contains whitespace");
        }
        Objects.requireNonNull(source, "watch source is missing");
        Objects.requireNonNull(xContentType, "request body is missing");
        this.id = id;
        this.source = source;
        this.xContentType = xContentType;
    }

    /**
     * @return The name that will be the ID of the indexed document
     */
    public String getId() {
        return id;
    }

    /**
     * @return The source of the watch
     */
    public BytesReference getSource() {
        return source;
    }

    /**
     * @return The initial active state of the watch (defaults to {@code true}, e.g. "active")
     */
    public boolean isActive() {
        return active;
    }

    /**
     * Sets the initial active state of the watch
     */
    public void setActive(boolean active) {
        this.active = active;
    }

    /**
     * Get the content type for the source
     */
    public XContentType xContentType() {
        return xContentType;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public static boolean isValidId(String id) {
        return Strings.isEmpty(id) == false && NO_WS_PATTERN.matcher(id).matches();
    }
}

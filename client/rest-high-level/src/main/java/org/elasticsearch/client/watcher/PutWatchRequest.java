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
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.seqno.SequenceNumbers;

import java.util.Objects;
import java.util.regex.Pattern;

import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

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
    private long ifSeqNo = SequenceNumbers.UNASSIGNED_SEQ_NO;
    private long ifPrimaryTerm = UNASSIGNED_PRIMARY_TERM;

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

    /**
     * only performs this put request if the watch's last modification was assigned the given
     * sequence number. Must be used in combination with {@link #setIfPrimaryTerm(long)}
     *
     * If the watch's last modification was assigned a different sequence number a
     * {@link org.elasticsearch.index.engine.VersionConflictEngineException} will be thrown.
     */
    public PutWatchRequest setIfSeqNo(long seqNo) {
        if (seqNo < 0 && seqNo != UNASSIGNED_SEQ_NO) {
            throw new IllegalArgumentException("sequence numbers must be non negative. got [" +  seqNo + "].");
        }
        ifSeqNo = seqNo;
        return this;
    }

    /**
     * only performs this put request if the watch's last modification was assigned the given
     * primary term. Must be used in combination with {@link #setIfSeqNo(long)}
     *
     * If the watch last modification was assigned a different term a
     * {@link org.elasticsearch.index.engine.VersionConflictEngineException} will be thrown.
     */
    public PutWatchRequest setIfPrimaryTerm(long term) {
        if (term < 0) {
            throw new IllegalArgumentException("primary term must be non negative. got [" + term + "]");
        }
        ifPrimaryTerm = term;
        return this;
    }

    /**
     * If set, only perform this put watch request if the watch's last modification was assigned this sequence number.
     * If the watch last last modification was assigned a different sequence number a
     * {@link org.elasticsearch.index.engine.VersionConflictEngineException} will be thrown.
     */
    public long ifSeqNo() {
        return ifSeqNo;
    }

    /**
     * If set, only perform this put watch request if the watch's last modification was assigned this primary term.
     *
     * If the watch's last modification was assigned a different term a
     * {@link org.elasticsearch.index.engine.VersionConflictEngineException} will be thrown.
     */
    public long ifPrimaryTerm() {
        return ifPrimaryTerm;
    }


    public static boolean isValidId(String id) {
        return Strings.isEmpty(id) == false && NO_WS_PATTERN.matcher(id).matches();
    }
}

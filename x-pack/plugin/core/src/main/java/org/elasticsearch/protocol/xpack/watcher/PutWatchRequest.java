/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.protocol.xpack.watcher;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.seqno.SequenceNumbers;

import java.io.IOException;
import java.util.regex.Pattern;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

/**
 * This request class contains the data needed to create a watch along with the name of the watch.
 * The name of the watch will become the ID of the indexed document.
 */
public class PutWatchRequest extends MasterNodeRequest<PutWatchRequest> {

    private static final DeprecationLogger DEPRECATION_LOGGER = new DeprecationLogger(LogManager.getLogger(PutWatchRequest.class));

    private static final TimeValue DEFAULT_TIMEOUT = TimeValue.timeValueSeconds(10);
    private static final Pattern NO_WS_PATTERN = Pattern.compile("\\S+");

    private String id;
    private BytesReference source;
    private XContentType xContentType = XContentType.JSON;
    private boolean active = true;
    private long version = Versions.MATCH_ANY;

    private long ifSeqNo = SequenceNumbers.UNASSIGNED_SEQ_NO;
    private long ifPrimaryTerm = UNASSIGNED_PRIMARY_TERM;

    public PutWatchRequest() {}

    public PutWatchRequest(StreamInput in) throws IOException {
        readFrom(in);
    }

    public PutWatchRequest(String id, BytesReference source, XContentType xContentType) {
        this.id = id;
        this.source = source;
        this.xContentType = xContentType;
        masterNodeTimeout(DEFAULT_TIMEOUT);
    }

    /**
     * @return The name that will be the ID of the indexed document
     */
    public String getId() {
        return id;
    }

    /**
     * Set the watch name
     */
    public void setId(String id) {
        this.id = id;
    }

    /**
     * @return The source of the watch
     */
    public BytesReference getSource() {
        return source;
    }

    /**
     * Set the source of the watch
     */
    public void setSource(BytesReference source, XContentType xContentType) {
        this.source = source;
        this.xContentType = xContentType;
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
    public long getIfSeqNo() {
        return ifSeqNo;
    }

    /**
     * If set, only perform this put watch request if the watch's last modification was assigned this primary term.
     *
     * If the watch's last modification was assigned a different term a
     * {@link org.elasticsearch.index.engine.VersionConflictEngineException} will be thrown.
     */
    public long getIfPrimaryTerm() {
        return ifPrimaryTerm;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (id == null) {
            validationException = addValidationError("watch id is missing", validationException);
        } else if (isValidId(id) == false) {
            validationException = addValidationError("watch id contains whitespace", validationException);
        }
        if (source == null) {
            validationException = addValidationError("watch source is missing", validationException);
        }
        if (xContentType == null) {
            validationException = addValidationError("request body is missing", validationException);
        }
        if (ifSeqNo != UNASSIGNED_SEQ_NO && version != Versions.MATCH_ANY) {
            validationException = addValidationError("compare and write operations can not use versioning", validationException);
        }

        if (version != Versions.MATCH_ANY) {
            DEPRECATION_LOGGER.deprecated(
                "Usage of internal versioning for optimistic concurrency control is deprecated and will be removed. Please use" +
                    " the `if_seq_no` and `if_primary_term` parameters instead.");
        }
        if (ifPrimaryTerm == UNASSIGNED_PRIMARY_TERM && ifSeqNo != UNASSIGNED_SEQ_NO) {
            validationException = addValidationError("ifSeqNo is set, but primary term is [0]", validationException);
        }
        if (ifPrimaryTerm != UNASSIGNED_PRIMARY_TERM && ifSeqNo == UNASSIGNED_SEQ_NO) {
            validationException =
                addValidationError("ifSeqNo is unassigned, but primary term is [" + ifPrimaryTerm + "]", validationException);
        }

        return validationException;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        id = in.readString();
        source = in.readBytesReference();
        active = in.readBoolean();
        if (in.getVersion().onOrAfter(Version.V_5_3_0)) {
            xContentType = in.readEnum(XContentType.class);
        } else {
            xContentType = XContentHelper.xContentType(source);
        }
        if (in.getVersion().onOrAfter(Version.V_6_3_0)) {
            version = in.readZLong();
        } else {
            version = Versions.MATCH_ANY;
        }
        if (in.getVersion().onOrAfter(Version.V_6_7_0)) {
            ifSeqNo = in.readZLong();
            ifPrimaryTerm = in.readVLong();
        } else {
            ifSeqNo = SequenceNumbers.UNASSIGNED_SEQ_NO;
            ifPrimaryTerm = UNASSIGNED_PRIMARY_TERM;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(id);
        out.writeBytesReference(source);
        out.writeBoolean(active);
        if (out.getVersion().onOrAfter(Version.V_5_3_0)) {
            out.writeEnum(xContentType);
        }
        if (out.getVersion().onOrAfter(Version.V_6_3_0)) {
            out.writeZLong(version);
        }
        if (out.getVersion().onOrAfter(Version.V_6_7_0)) {
            out.writeZLong(ifSeqNo);
            out.writeVLong(ifPrimaryTerm);
        }
    }

    public static boolean isValidId(String id) {
        return Strings.isEmpty(id) == false && NO_WS_PATTERN.matcher(id).matches();
    }
}

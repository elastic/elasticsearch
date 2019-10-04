package org.elasticsearch.transport;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public class Header {

    private final Version version;
    private final long requestId;
    private final byte status;

    Header(long requestId, byte status, Version version) {
        this.version = version;
        this.requestId = requestId;
        this.status = status;
    }

    Version getVersion() {
        return version;
    }

    long getRequestId() {
        return requestId;
    }

    byte getStatus() {
        return status;
    }

    boolean isRequest() {
        return TransportStatus.isRequest(status);
    }

    boolean isResponse() {
        return TransportStatus.isRequest(status) == false;
    }

    boolean isError() {
        return TransportStatus.isError(status);
    }

    boolean isHandshake() {
        return TransportStatus.isHandshake(status);
    }

    boolean isCompress() {
        return TransportStatus.isCompress(status);
    }

    boolean isCompressed() {
        return TransportStatus.isCompress(status);
    }
}

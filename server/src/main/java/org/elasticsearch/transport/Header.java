package org.elasticsearch.transport;

import org.elasticsearch.Version;

public class Header {

    private final int networkMessageSize;
    private final Version version;
    private final long requestId;
    private final byte status;

    Header(int networkMessageSize, long requestId, byte status, Version version) {
        this.networkMessageSize = networkMessageSize;
        this.version = version;
        this.requestId = requestId;
        this.status = status;
    }

    public int getNetworkMessageSize() {
        return networkMessageSize;
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

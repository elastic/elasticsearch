package org.elasticsearch.transport;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public class Header {

    private final Version remoteVersion;
    private final long requestId;
    private final byte status;
    private Map<String, String> requestHeaders;
    private String action;
    private Map<String, Set<?>> responseHeaders;

    Header(long requestId, byte status, Version remoteVersion) {
        this.remoteVersion = remoteVersion;
        this.requestId = requestId;
        this.status = status;
    }

    Version getVersion() {
        return remoteVersion;
    }

    long getRequestId() {
        return requestId;
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

    String getAction() {
        return action;
    }

    boolean isCompressed() {
        return TransportStatus.isCompress(status);
    }

    boolean isFullyParsed() {
        return requestHeaders != null;
    }

    void finishParsing(StreamInput in) throws IOException {
        assert isFullyParsed() == false : "Can only call finishParsing if not header not parsed";
        requestHeaders = in.readMap(StreamInput::readString, StreamInput::readString);
        responseHeaders = in.readMap(StreamInput::readString, input -> {
            final int size = input.readVInt();
            if (size == 0) {
                return Collections.emptySet();
            } else if (size == 1) {
                return Collections.singleton(input.readString());
            } else {
                // use a linked hash set to preserve order
                final LinkedHashSet<String> values = new LinkedHashSet<>(size);
                for (int i = 0; i < size; i++) {
                    final String value = input.readString();
                    final boolean added = values.add(value);
                    assert added : value;
                }
                return values;
            }
        });
        InboundMessage message;
        if (TransportStatus.isRequest(status)) {
            if (remoteVersion.before(Version.V_8_0_0)) {
                // discard features
                in.readStringArray();
            }
            final String action = in.readString();
        }
    }
}

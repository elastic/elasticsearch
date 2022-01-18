/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.coordination;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportResponse;

import java.io.IOException;
import java.util.Optional;

/**
 * Response to a {@link PublishRequest}. Encapsulates both a {@link PublishResponse}
 * and an optional {@link Join}.
 */
public class PublishWithJoinResponse extends TransportResponse {
    private final PublishResponse publishResponse;
    private final Optional<Join> optionalJoin;

    public PublishWithJoinResponse(PublishResponse publishResponse, Optional<Join> optionalJoin) {
        this.publishResponse = publishResponse;
        this.optionalJoin = optionalJoin;
    }

    public PublishWithJoinResponse(StreamInput in) throws IOException {
        this.publishResponse = new PublishResponse(in);
        this.optionalJoin = Optional.ofNullable(in.readOptionalWriteable(Join::new));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        publishResponse.writeTo(out);
        out.writeOptionalWriteable(optionalJoin.orElse(null));
    }

    public PublishResponse getPublishResponse() {
        return publishResponse;
    }

    public Optional<Join> getJoin() {
        return optionalJoin;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if ((o instanceof PublishWithJoinResponse) == false) return false;

        PublishWithJoinResponse that = (PublishWithJoinResponse) o;

        if (publishResponse.equals(that.publishResponse) == false) return false;
        return optionalJoin.equals(that.optionalJoin);
    }

    @Override
    public int hashCode() {
        int result = publishResponse.hashCode();
        result = 31 * result + optionalJoin.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "PublishWithJoinResponse{" + "publishResponse=" + publishResponse + ", optionalJoin=" + optionalJoin + '}';
    }
}

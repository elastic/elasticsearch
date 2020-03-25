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
        if (!(o instanceof PublishWithJoinResponse)) return false;

        PublishWithJoinResponse that = (PublishWithJoinResponse) o;

        if (!publishResponse.equals(that.publishResponse)) return false;
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
        return "PublishWithJoinResponse{" +
            "publishResponse=" + publishResponse +
            ", optionalJoin=" + optionalJoin +
            '}';
    }
}

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

package org.elasticsearch.cluster.routing;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

/**
 * {@link ResyncFailedInfo} holds additional information why the shard was failed in resync.
 */
public final class ResyncFailedInfo implements Writeable, ToXContentFragment {
    private final String reason;
    private final Exception failure;

    public ResyncFailedInfo(String reason, Exception failure) {
        this.reason = reason;
        this.failure = failure;
    }

    ResyncFailedInfo(StreamInput in) throws IOException {
        this.reason = in.readString();
        this.failure = in.readException();
    }

    public String getReason() {
        return reason;
    }

    public Exception getFailure() {
        return failure;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(reason);
        out.writeException(failure);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("resync_failed_info");
        builder.field("reason", reason);
        builder.field("failure", ExceptionsHelper.detailedMessage(failure));
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ResyncFailedInfo that = (ResyncFailedInfo) o;
        return Objects.equals(this.reason, that.reason) && Objects.equals(this.failure, that.failure);
    }

    @Override
    public int hashCode() {
        int result = reason.hashCode();
        result = 31 * result + failure.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return String.format(Locale.ROOT, "resync_failed{reason [%s], failure [%s]}", reason, failure);
    }
}

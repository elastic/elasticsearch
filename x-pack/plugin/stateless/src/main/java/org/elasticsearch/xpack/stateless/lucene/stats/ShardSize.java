/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.lucene.stats;

import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

public record ShardSize(long interactiveSizeInBytes, long nonInteractiveSizeInBytes, PrimaryTermAndGeneration primaryTermGeneration)
    implements
        Writeable {

    public static final ShardSize EMPTY = new ShardSize(0, 0, PrimaryTermAndGeneration.ZERO);
    public static final TransportVersion PRIMARY_TERM_GENERATION_VERSION = TransportVersions.SHARD_SIZE_PRIMARY_TERM_GEN_ADDED;

    public ShardSize {
        assert interactiveSizeInBytes >= 0 : "interactiveSize must be non negative";
        assert nonInteractiveSizeInBytes >= 0 : "nonInteractiveSize must be non negative";
    }

    public static ShardSize from(StreamInput in) throws IOException {
        if (in.getTransportVersion().before(PRIMARY_TERM_GENERATION_VERSION)) {
            return new ShardSize(in.readLong(), in.readLong(), PrimaryTermAndGeneration.ZERO);
        }
        return new ShardSize(in.readLong(), in.readLong(), new PrimaryTermAndGeneration(in));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(interactiveSizeInBytes);
        out.writeLong(nonInteractiveSizeInBytes);
        if (out.getTransportVersion().onOrAfter(PRIMARY_TERM_GENERATION_VERSION)) {
            primaryTermGeneration.writeTo(out);
        }
    }

    public long totalSizeInBytes() {
        return interactiveSizeInBytes + nonInteractiveSizeInBytes;
    }

    @Override
    public String toString() {
        return "[interactive_in_bytes="
            + interactiveSizeInBytes
            + ", non-interactive_in_bytes="
            + nonInteractiveSizeInBytes
            + ']'
            + primaryTermGeneration;
    }
}

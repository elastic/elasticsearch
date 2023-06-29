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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

public record ShardSize(long interactiveSizeInBytes, long nonInteractiveSizeInBytes) implements Writeable {

    public static final ShardSize EMPTY = new ShardSize(0, 0);

    public ShardSize {
        assert interactiveSizeInBytes >= 0 : "interactiveSize must be non negative";
        assert nonInteractiveSizeInBytes >= 0 : "nonInteractiveSize must be non negative";
    }

    public ShardSize(StreamInput in) throws IOException {
        this(in.readLong(), in.readLong());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(interactiveSizeInBytes);
        out.writeLong(nonInteractiveSizeInBytes);
    }

    public long totalSizeInBytes() {
        return interactiveSizeInBytes + nonInteractiveSizeInBytes;
    }
}

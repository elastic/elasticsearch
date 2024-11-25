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

package co.elastic.elasticsearch.stateless.commits;

import org.apache.lucene.codecs.CodecUtil;
import org.elasticsearch.blobcache.common.BlobCacheBufferedIndexInput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Compound commit consists of 3 main blocks:
 * - header
 * - replicated content
 * - main content
 * Replicated content is composed of ranges of bytes of copied from main content that are located beyond the first region.
 * Such ranges are used to store the headers and footers of Lucene files which are always accessed when the Lucene index is opened.
 * By copying the bytes corresponding to the headers/footers in the first region,
 * we minimize the number of requests to the object store that are needed to open a shard in order to speedup recovery and relocation.
 *
 * This structure is the header describing replicated content. It references the positions of replicated bytes relative to the main content.
 */
public record InternalFilesReplicatedRanges(List<InternalFileReplicatedRange> replicatedRanges, long dataSizeInBytes)
    implements
        ToXContentFragment {

    public static final short REPLICATED_CONTENT_HEADER_SIZE = BlobCacheBufferedIndexInput.BUFFER_SIZE;
    public static final short REPLICATED_CONTENT_FOOTER_SIZE = (short) CodecUtil.footerLength();
    public static final short REPLICATED_CONTENT_MAX_SINGLE_FILE_SIZE = (short) (REPLICATED_CONTENT_HEADER_SIZE
        + REPLICATED_CONTENT_FOOTER_SIZE);

    public static InternalFilesReplicatedRanges EMPTY = new InternalFilesReplicatedRanges(List.of(), 0L);

    public InternalFilesReplicatedRanges {
        assert replicatedRanges != null;
        assert assertRangesSorted(replicatedRanges);
        assert dataSizeInBytes == dataSizeInBytes(replicatedRanges);
    }

    public static InternalFilesReplicatedRanges from(List<InternalFileReplicatedRange> replicatedRanges) {
        return replicatedRanges != null && replicatedRanges.isEmpty() == false
            ? new InternalFilesReplicatedRanges(replicatedRanges, dataSizeInBytes(replicatedRanges))
            : InternalFilesReplicatedRanges.EMPTY;
    }

    private static boolean assertRangesSorted(List<InternalFileReplicatedRange> replicatedRanges) {
        InternalFileReplicatedRange previous = null;
        for (InternalFileReplicatedRange range : replicatedRanges) {
            assert previous == null || previous.position + previous.length <= range.position : "Ranges are not sorted: " + replicatedRanges;
            previous = range;
        }
        return true;
    }

    private static long dataSizeInBytes(List<InternalFileReplicatedRange> replicatedRanges) {
        long size = 0;
        for (var range : replicatedRanges) {
            size += range.length;
        }
        return size;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startArray("internal_files_replicated_ranges");
        for (var r : replicatedRanges) {
            r.toXContent(builder, ToXContent.EMPTY_PARAMS);
        }
        builder.endArray();
        return builder;
    }

    public boolean isEmpty() {
        return replicatedRanges.isEmpty();
    }

    public record InternalFileReplicatedRange(long position, short length)
        implements
            Writeable,
            ToXContentObject,
            Comparable<InternalFileReplicatedRange> {

        public static final ConstructingObjectParser<InternalFileReplicatedRange, Void> PARSER = new ConstructingObjectParser<>(
            "internal_file_replicated_range",
            true,
            args -> new InternalFileReplicatedRange((long) args[0], ((Integer) args[1]).shortValue())
        );

        static {
            PARSER.declareLong(constructorArg(), new ParseField("position"));
            PARSER.declareInt(constructorArg(), new ParseField("length"));
        }

        public InternalFileReplicatedRange {
            assert position >= 0 : "Position must be non negative: " + position;
            assert length > 0 : "Must replicate non empty content: " + length;
        }

        public static InternalFileReplicatedRange fromStream(StreamInput in) throws IOException {
            return new InternalFileReplicatedRange(in.readVLong(), in.readShort());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(position);
            out.writeShort(length);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject().field("position", position).field("length", length).endObject();
        }

        @Override
        public int compareTo(InternalFileReplicatedRange o) {
            return Long.compare(position, o.position);
        }
    }
}

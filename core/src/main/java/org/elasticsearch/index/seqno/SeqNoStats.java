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
package org.elasticsearch.index.seqno;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;

public class SeqNoStats implements ToXContent, Writeable<SeqNoStats> {

    public static final SeqNoStats PROTO  = new SeqNoStats(0,0);

    final long maxSeqNo;
    final long localCheckpoint;

    public SeqNoStats(long maxSeqNo, long localCheckpoint) {
        this.maxSeqNo = maxSeqNo;
        this.localCheckpoint = localCheckpoint;
    }

    public SeqNoStats(StreamInput in) throws IOException {
        this(in.readZLong(), in.readZLong());
    }

    /** the maximum sequence number seen so far */
    public long getMaxSeqNo() {
        return maxSeqNo;
    }

    /** the maximum sequence number for which all previous operations (including) have been completed */
    public long getLocalCheckpoint() {
        return localCheckpoint;
    }

    @Override
    public SeqNoStats readFrom(StreamInput in) throws IOException {
        return new SeqNoStats(in.readLong(), in.readLong());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeZLong(maxSeqNo);
        out.writeZLong(localCheckpoint);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.SEQ_NO);
        builder.field(Fields.MAX_SEQ_NO, maxSeqNo);
        builder.field(Fields.LOCAL_CHECKPOINT, localCheckpoint);
        builder.endObject();
        return builder;
    }


    static final class Fields {
        static final XContentBuilderString SEQ_NO = new XContentBuilderString("seq_no");
        static final XContentBuilderString MAX_SEQ_NO = new XContentBuilderString("max");
        static final XContentBuilderString LOCAL_CHECKPOINT = new XContentBuilderString("local_checkpoint");
    }
}

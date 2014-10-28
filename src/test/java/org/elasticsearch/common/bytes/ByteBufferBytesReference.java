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

package org.elasticsearch.common.bytes;

import com.google.common.base.Charsets;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.Channels;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.util.CharsetUtil;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.GatheringByteChannel;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;

/**
 * Note: this is only used by one lone test method.
 */
public class ByteBufferBytesReference implements BytesReference {

    private final ByteBuffer buffer;

    public ByteBufferBytesReference(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    @Override
    public byte get(int index) {
        return buffer.get(buffer.position() + index);
    }

    @Override
    public int length() {
        return buffer.remaining();
    }

    @Override
    public BytesReference slice(int from, int length) {
        ByteBuffer dup = buffer.duplicate();
        dup.position(buffer.position() + from);
        dup.limit(buffer.position() + from + length);
        return new ByteBufferBytesReference(dup);
    }

    @Override
    public StreamInput streamInput() {
        return new ByteBufferStreamInput(buffer);
    }

    @Override
    public void writeTo(OutputStream os) throws IOException {
        if (buffer.hasArray()) {
            os.write(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
        } else {
            byte[] tmp = new byte[8192];
            ByteBuffer buf = buffer.duplicate();
            while (buf.hasRemaining()) {
                buf.get(tmp, 0, Math.min(tmp.length, buf.remaining()));
                os.write(tmp);
            }
        }
    }

    @Override
    public void writeTo(GatheringByteChannel channel) throws IOException {
        Channels.writeToChannel(buffer, channel);
    }

    @Override
    public byte[] toBytes() {
        if (!buffer.hasRemaining()) {
            return BytesRef.EMPTY_BYTES;
        }
        byte[] tmp = new byte[buffer.remaining()];
        buffer.duplicate().get(tmp);
        return tmp;
    }

    @Override
    public BytesArray toBytesArray() {
        if (buffer.hasArray()) {
            return new BytesArray(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
        }
        return new BytesArray(toBytes());
    }

    @Override
    public BytesArray copyBytesArray() {
        return new BytesArray(toBytes());
    }

    @Override
    public ChannelBuffer toChannelBuffer() {
        return ChannelBuffers.wrappedBuffer(buffer);
    }

    @Override
    public boolean hasArray() {
        return buffer.hasArray();
    }

    @Override
    public byte[] array() {
        return buffer.array();
    }

    @Override
    public int arrayOffset() {
        return buffer.arrayOffset() + buffer.position();
    }

    @Override
    public int hashCode() {
        return Helper.bytesHashCode(this);
    }

    @Override
    public boolean equals(Object obj) {
        return Helper.bytesEqual(this, (BytesReference) obj);
    }

    @Override
    public String toUtf8() {
        if (!buffer.hasRemaining()) {
            return "";
        }
        final CharsetDecoder decoder = CharsetUtil.getDecoder(Charsets.UTF_8);
        final CharBuffer dst = CharBuffer.allocate(
                (int) ((double) buffer.remaining() * decoder.maxCharsPerByte()));
        try {
            CoderResult cr = decoder.decode(buffer, dst, true);
            if (!cr.isUnderflow()) {
                cr.throwException();
            }
            cr = decoder.flush(dst);
            if (!cr.isUnderflow()) {
                cr.throwException();
            }
        } catch (CharacterCodingException x) {
            throw new IllegalStateException(x);
        }
        return dst.flip().toString();
    }

    @Override
    public BytesRef toBytesRef() {
        if (buffer.hasArray()) {
            return new BytesRef(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
        }
        return new BytesRef(toBytes());
    }

    @Override
    public BytesRef copyBytesRef() {
        return new BytesRef(toBytes());
    }
}

/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.transport.netty;

import org.elasticsearch.Version;
import org.jboss.netty.buffer.ChannelBuffer;

/**
 */
public class NettyHeader {

    public static final int HEADER_SIZE = 2 + 4 + 8 + 1 + 4;

    public static void writeHeader(ChannelBuffer buffer, long requestId, byte status, Version version) {
        int index = buffer.readerIndex();
        buffer.setByte(index, 'E');
        index += 1;
        buffer.setByte(index, 'S');
        index += 1;
        // write the size, the size indicates the remaining message size, not including the size int
        buffer.setInt(index, buffer.readableBytes() - 6);
        index += 4;
        buffer.setLong(index, requestId);
        index += 8;
        buffer.setByte(index, status);
        index += 1;
        buffer.setInt(index, version.id);
    }
}

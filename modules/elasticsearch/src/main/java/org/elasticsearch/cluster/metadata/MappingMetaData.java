/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.compress.CompressedString;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.mapper.DocumentMapper;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class MappingMetaData {

    public static class Routing {

        public static final Routing EMPTY = new Routing(false);

        private final boolean required;

        public Routing(boolean required) {
            this.required = required;
        }

        public boolean required() {
            return required;
        }
    }

    private final String type;

    private final CompressedString source;

    private final Routing routing;

    public MappingMetaData(DocumentMapper docMapper) {
        this.type = docMapper.type();
        this.source = docMapper.mappingSource();
        this.routing = new Routing(docMapper.routingFieldMapper().required());
    }

    public MappingMetaData(String type, CompressedString source) {
        this.type = type;
        this.source = source;
        this.routing = Routing.EMPTY;
    }

    MappingMetaData(String type, CompressedString source, Routing routing) {
        this.type = type;
        this.source = source;
        this.routing = routing;
    }

    public String type() {
        return this.type;
    }

    public CompressedString source() {
        return this.source;
    }

    public Routing routing() {
        return this.routing;
    }

    public static void writeTo(MappingMetaData mappingMd, StreamOutput out) throws IOException {
        out.writeUTF(mappingMd.type());
        mappingMd.source().writeTo(out);
        // routing
        out.writeBoolean(mappingMd.routing().required());
    }

    public static MappingMetaData readFrom(StreamInput in) throws IOException {
        String type = in.readUTF();
        CompressedString source = CompressedString.readCompressedString(in);
        // routing
        Routing routing = new Routing(in.readBoolean());
        return new MappingMetaData(type, source, routing);
    }
}

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

package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.search.DocValueFormat;

import java.io.IOException;
import java.util.Map;

public class BucketStreamContext implements Streamable {

    private DocValueFormat format;
    private boolean keyed;
    private Map<String, Object> attributes;

    public BucketStreamContext() {
    }

    public void format(DocValueFormat format) {
        this.format = format;
    }

    public DocValueFormat format() {
        return format;
    }

    public void keyed(boolean keyed) {
        this.keyed = keyed;
    }

    public boolean keyed() {
        return keyed;
    }

    public void attributes(Map<String, Object> attributes) {
        this.attributes = attributes;
    }

    public Map<String, Object> attributes() {
        return attributes;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        format = in.readNamedWriteable(DocValueFormat.class);
        keyed = in.readBoolean();
        attributes = in.readMap();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(format);
        out.writeBoolean(keyed);
        out.writeMap(attributes);
    }

}

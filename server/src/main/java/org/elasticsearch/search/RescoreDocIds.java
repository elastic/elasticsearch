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

package org.elasticsearch.search;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

public final class RescoreDocIds implements Writeable {
    public static final RescoreDocIds EMPTY = new RescoreDocIds(Map.of());

    private final Map<Integer, Set<Integer>> docIds;

    public RescoreDocIds(Map<Integer, Set<Integer>> docIds) {
        this.docIds = docIds;
    }

    public RescoreDocIds(StreamInput in) throws IOException {
        docIds = in.readMap(StreamInput::readVInt, i -> i.readSet(StreamInput::readVInt));
    }

    public Set<Integer> getId(int index) {
        return docIds.get(index);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(docIds, StreamOutput::writeVInt, (o, v) -> o.writeCollection(v, StreamOutput::writeVInt));
    }
}

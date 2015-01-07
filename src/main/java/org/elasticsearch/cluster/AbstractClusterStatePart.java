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

package org.elasticsearch.cluster;

import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.*;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;

/**
 * Basic implementation of cluster state part that sends entire part as a difference if the part got changed.
 */
public abstract class AbstractClusterStatePart implements ClusterStatePart {

    protected static abstract class AbstractFactory<T extends AbstractClusterStatePart> implements ClusterStatePart.Factory<T> {

        @Override
        public Version addedIn() {
            return Version.V_2_0_0;
        }

        @Override
        public EnumSet<XContentContext> context() {
            return API;
        }

        @Override
        public T fromXContent(XContentParser parser, LocalContext context) throws IOException {
            throw new UnsupportedOperationException("Not implemented yet");
        }

        @Override
        public T fromMap(Map<String, Object> map, LocalContext context) throws IOException {
            // if it starts with the type, remove it
            if (map.size() == 1 && map.containsKey(partType())) {
                map = (Map<String, Object>) map.values().iterator().next();
            }
            XContentBuilder builder = XContentFactory.smileBuilder().map(map);
            try (XContentParser parser = XContentFactory.xContent(XContentType.SMILE).createParser(builder.bytes())) {
                // move to START_OBJECT
                parser.nextToken();
                return fromXContent(parser, context);
            }
        }

        @Override
        public Diff<T> diff(@Nullable T before, T after) {
            if (after.equals(before)) {
                return new FullClusterStatePartDiff<T>(null);
            } else {
                return new FullClusterStatePartDiff<T>(after);
            }

        }

        @Override
        public Diff<T> readDiffFrom(StreamInput in, LocalContext context) throws IOException {
            if (in.readBoolean()) {
                return new FullClusterStatePartDiff<T>(readFrom(in, context));
            } else {
                return new FullClusterStatePartDiff<T>(null);
            }
        }

        @Override
        public void writeDiffsTo(Diff<T> diff, StreamOutput out) throws IOException {
            FullClusterStatePartDiff<T> fullDiff = (FullClusterStatePartDiff<T>) diff;
            if (fullDiff.part != null) {
                out.writeBoolean(true);
                writeTo(fullDiff.part, out);
            } else {
                out.writeBoolean(false);
            }
        }
    }


    private static class FullClusterStatePartDiff<T extends AbstractClusterStatePart> implements Diff<T> {

        private T part;

        FullClusterStatePartDiff(@Nullable T part) {
            this.part = part;
        }

        @Override
        public T apply(T part) {
            if (this.part != null) {
                return this.part;
            } else {
                return part;
            }
        }
    }

}

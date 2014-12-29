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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.EnumSet;

/**
 */
public abstract class AbstractClusterStatePart implements ClusterStatePart {

    @Override
    public EnumSet<XContentContext> context() {
        return API_ONLY;
    }

    public static class CompleteDiff<T extends ClusterStatePart> implements Diff<T> {
        private T part;

        public CompleteDiff(T part) {
            this.part = part;
        }

        @Override
        public T apply(T state) {
            return part;
        }

        public void writeTo(StreamOutput out) throws IOException{
            out.writeBoolean(true);
            if (part != null) {
                out.writeBoolean(true);
                part.writeTo(out);
            } else {
                out.writeBoolean(false);
            }
        }
    }

    protected static class NoDiff<T extends ClusterStatePart> implements Diff<T> {

        public NoDiff() {
        }

        @Override
        public T apply(T part) {
            return part;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(false);
        }
    }

    protected static abstract class AbstractFactory<T extends ClusterStatePart> implements ClusterStatePart.Factory<T> {

        @Override
        public Diff<T> diff(T before, T after) {
            if (before.equals(after)) {
                return new NoDiff<T>();
            } else {
                return new CompleteDiff<T>(after);
            }
        }

        @Override
        public Diff<T> readDiffFrom(StreamInput in, LocalContext context) throws IOException {
            if(in.readBoolean()) {
                if (in.readBoolean()) {
                    T part = readFrom(in, context);
                    return new CompleteDiff<T>(part);
                } else {
                    return new CompleteDiff<T>(null);
                }
            } else {
                return new NoDiff<T>();
            }
        }

        @Override
        public Version addedIn() {
            return null;
        }

        @Override
        public T fromXContent(XContentParser parser, LocalContext context) throws IOException {
            throw new UnsupportedOperationException("Not implemented yet");
        }
    }

}

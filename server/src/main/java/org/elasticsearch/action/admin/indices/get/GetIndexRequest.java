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

package org.elasticsearch.action.admin.indices.get;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.info.ClusterInfoRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.ArrayUtils;

import java.io.IOException;

/**
 * A request to retrieve information about an index.
 */
public class GetIndexRequest extends ClusterInfoRequest<GetIndexRequest> {
    public enum Feature {
        ALIASES((byte) 0),
        MAPPINGS((byte) 1),
        SETTINGS((byte) 2);

        private static final Feature[] FEATURES = new Feature[Feature.values().length];

        static {
            for (Feature feature : Feature.values()) {
                assert feature.id() < FEATURES.length && feature.id() >= 0;
                FEATURES[feature.id] = feature;
            }
        }

        private final byte id;

        Feature(byte id) {
            this.id = id;
        }

        public byte id() {
            return id;
        }

        public static Feature fromId(byte id) {
            if (id < 0 || id >= FEATURES.length) {
                throw new IllegalArgumentException("No mapping for id [" + id + "]");
            }
            return FEATURES[id];
        }
    }

    private static final Feature[] DEFAULT_FEATURES = new Feature[] { Feature.ALIASES, Feature.MAPPINGS, Feature.SETTINGS };
    private Feature[] features = DEFAULT_FEATURES;
    private boolean humanReadable = false;
    private transient boolean includeDefaults = false;

    public GetIndexRequest() {

    }

    public GetIndexRequest(StreamInput in) throws IOException {
        super(in);
        features = in.readArray(i -> Feature.fromId(i.readByte()), Feature[]::new);
        humanReadable = in.readBoolean();
        includeDefaults = in.readBoolean();
    }

    public GetIndexRequest features(Feature... features) {
        if (features == null) {
            throw new IllegalArgumentException("features cannot be null");
        } else {
            this.features = features;
        }
        return this;
    }

    public GetIndexRequest addFeatures(Feature... features) {
        if (this.features == DEFAULT_FEATURES) {
            return features(features);
        } else {
            return features(ArrayUtils.concat(features(), features, Feature.class));
        }
    }

    public Feature[] features() {
        return features;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public GetIndexRequest humanReadable(boolean humanReadable) {
        this.humanReadable = humanReadable;
        return this;
    }

    public boolean humanReadable() {
        return humanReadable;
    }

    /**
     * Sets the value of "include_defaults".
     *
     * @param includeDefaults value of "include_defaults" to be set.
     * @return this request
     */
    public GetIndexRequest includeDefaults(boolean includeDefaults) {
        this.includeDefaults = includeDefaults;
        return this;
    }

    /**
     * Whether to return all default settings for each of the indices.
     *
     * @return <code>true</code> if defaults settings for each of the indices need to returned;
     * <code>false</code> otherwise.
     */
    public boolean includeDefaults() {
        return includeDefaults;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeArray((o, f) -> o.writeByte(f.id), features);
        out.writeBoolean(humanReadable);
        out.writeBoolean(includeDefaults);
    }
}

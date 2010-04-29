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

package org.elasticsearch.util.xcontent;

import org.elasticsearch.util.xcontent.builder.XContentBuilder;

import java.io.IOException;
import java.util.Map;

/**
 * An interface allowing to transfer an object to "XContent" using an {@link org.elasticsearch.util.xcontent.builder.XContentBuilder}.
 *
 * @author kimchy (shay.banon)
 */
public interface ToXContent {

    public static interface Params {
        String param(String key);
    }

    public static final Params EMPTY_PARAMS = new Params() {
        @Override public String param(String key) {
            return null;
        }
    };

    public static class MapParams implements Params {

        private final Map<String, String> params;

        public MapParams(Map<String, String> params) {
            this.params = params;
        }

        @Override public String param(String key) {
            return params.get(key);
        }
    }

    void toXContent(XContentBuilder builder, Params params) throws IOException;
}

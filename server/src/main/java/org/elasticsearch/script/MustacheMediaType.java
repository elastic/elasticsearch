/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.script;

import org.elasticsearch.common.xcontent.MediaType;
import org.elasticsearch.common.xcontent.MediaTypeRegistry;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.List;

public enum MustacheMediaType implements MediaType {

    //    JSON {
//        @Override
//        public String queryParameter() {
//            return null;
//        }
//
//        @Override
//        public List<HeaderValue> headerValues() {
//            return List.of(
//                new HeaderValue("application/json",
//                    Map.of("charset", "utf-8")));
//        }
//    },
    PLAIN_TEXT {
        @Override
        public String queryParameter() {
            return null;
        }

        @Override
        public List<HeaderValue> headerValues() {
            return List.of(new HeaderValue("text/plain"));
        }
    },
    X_WWW_FORM_URLENCODED {
        @Override
        public String queryParameter() {
            return null;
        }

        @Override
        public List<HeaderValue> headerValues() {
            return List.of(new HeaderValue("application/x-www-form-urlencoded"));
        }
    };

    public static final MediaTypeRegistry<MediaType> REGISTRY = new MediaTypeRegistry<>()
        .register(MustacheMediaType.values())
        .register(XContentType.JSON); // TODO PG make JSON and ND-JSON separate? then make the headerValues return single instance?
}

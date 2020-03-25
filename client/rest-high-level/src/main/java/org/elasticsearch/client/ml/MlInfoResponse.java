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

package org.elasticsearch.client.ml;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class MlInfoResponse implements Validatable {
    private final Map<String, Object> info;

    private MlInfoResponse(Map<String, Object> info) {
        this.info = info;
    }

    public Map<String, Object> getInfo() {
        return info;
    }

    public static MlInfoResponse fromXContent(XContentParser parser) throws IOException {
        Map<String, Object> info = parser.map();
        return new MlInfoResponse(info);
    }

    @Override
    public int hashCode() {
        return Objects.hash(info);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        MlInfoResponse other = (MlInfoResponse) obj;
        return Objects.equals(info, other.info);
    }
}

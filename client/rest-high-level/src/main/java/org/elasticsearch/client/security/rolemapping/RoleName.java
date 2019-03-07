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

package org.elasticsearch.client.security.rolemapping;

import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

/**
 * The Role Mapping API can define target role names using either {@link StaticRoleName a static string} or a
 * {@link TemplateRoleName a mustache template}.
 * This interface is a base for those implementations.
 */
public interface RoleName extends ToXContentObject {

    static RoleName fromXContent(XContentParser parser) throws IOException {
        if (parser.currentToken() == XContentParser.Token.VALUE_STRING) {
            return StaticRoleName.fromXContent(parser);
        } else {
            return TemplateRoleName.fromXContent(parser);
        }
    }
}

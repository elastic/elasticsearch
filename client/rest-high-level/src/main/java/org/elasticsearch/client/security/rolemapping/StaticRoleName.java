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

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A {@link RoleName} that is a static string.
 */
public class StaticRoleName implements RoleName {
    private final String name;

    public StaticRoleName(String name) {
        this.name = Objects.requireNonNull(name);
    }

    public String getName() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()){
            return false;
        }
        final StaticRoleName that = (StaticRoleName) o;
        return Objects.equals(this.name, that.name);
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.value(name);
    }

    public static StaticRoleName fromXContent(XContentParser parser) throws IOException {
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.VALUE_STRING, parser.currentToken(), parser::getTokenLocation);
        return new StaticRoleName(parser.text());
    }

    public static List<StaticRoleName> list(List<String> names) {
        return names.stream().map(StaticRoleName::new).collect(Collectors.toList());
    }

}

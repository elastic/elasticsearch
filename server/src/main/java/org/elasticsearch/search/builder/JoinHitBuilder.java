/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

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

package org.elasticsearch.search.builder;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.util.Objects;

public class JoinHitBuilder {
    private final String name;
    private final String index;
    private final @Nullable
    String matchField;
    private final SearchSourceBuilder source;

    JoinHitBuilder(String name, String index, String matchField, SearchSourceBuilder source) {
        this.name = name;
        this.index = index;
        this.matchField = matchField;
        this.source = source;
    }

    public static JoinHitBuilder fromXContent(XContentParser parser) throws IOException {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        SetOnce<String> name = new SetOnce<>();
        SetOnce<String> index = new SetOnce<>();
        SetOnce<String> matchField = new SetOnce<>();

        sourceBuilder.parseXContent(parser, false, (fieldName) -> {
            switch (fieldName) {
                case "name":
                    name.set(parser.text());
                    return true;
                case "index":
                    index.set(parser.text());
                    return true;
                case "match_field":
                    matchField.set(parser.text());
                    return true;
                default:
                    return false;
            }
        });
        if (Strings.isEmpty(name.get())) {
            throw new IllegalArgumentException("[name] parameter of a join hit must be specified");
        }
        if (Strings.isEmpty(index.get())) {
            throw new IllegalArgumentException("[index] parameter of a join hit must be specified");
        }
        return new JoinHitBuilder(name.get(), index.get(), matchField.get(), sourceBuilder);
    }

    public String getName() {
        return name;
    }

    public String getIndex() {
        return index;
    }

    public String getMatchField() {
        return matchField;
    }

    public SearchSourceBuilder getSource() {
        return source;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JoinHitBuilder that = (JoinHitBuilder) o;
        return name.equals(that.name) && index.equals(that.index)
            && Objects.equals(matchField, that.matchField)
            && source.equals(that.source);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, index, matchField, source);
    }
}

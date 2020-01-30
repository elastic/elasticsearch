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

package org.elasticsearch.search.fetch.subphase.matches;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class MatchesContextBuilder implements Writeable {

    private final Map<String, Settings> processors;

    public MatchesContextBuilder(StreamInput in) throws IOException {
        this.processors = in.readMap(StreamInput::readString, Settings::readSettingsFromStream);
    }

    public MatchesContextBuilder(Map<String, Settings> processors) {
        this.processors = processors;
    }

    public MatchesContext build(boolean addNamedQueries) {
        if (addNamedQueries) {
            processors.put(NamedQueriesProcessor.NAME, Settings.EMPTY);
        }
        return new MatchesContext(processors);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(processors, StreamOutput::writeString, (o, s) -> Settings.writeSettingsToStream(s, o));
    }

    public static MatchesContextBuilder fromXContent(XContentParser parser) throws IOException {
        return new MatchesContextBuilder(parser.map(HashMap::new, Settings::fromXContent));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MatchesContextBuilder that = (MatchesContextBuilder) o;
        return Objects.equals(processors, that.processors);
    }

    @Override
    public int hashCode() {
        return Objects.hash(processors);
    }
}

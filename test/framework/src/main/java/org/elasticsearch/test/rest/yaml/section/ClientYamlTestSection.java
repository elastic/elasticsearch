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
package org.elasticsearch.test.rest.yaml.section;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.xcontent.XContentLocation;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Represents a test section, which is composed of a skip section and multiple executable sections.
 */
public class ClientYamlTestSection implements Comparable<ClientYamlTestSection> {
    public static ClientYamlTestSection parse(XContentParser parser) throws IOException {
        ParserUtils.advanceToFieldName(parser);
        XContentLocation sectionLocation = parser.getTokenLocation();
        String sectionName = parser.currentName();
        List<ExecutableSection> executableSections = new ArrayList<>();
        try {
            parser.nextToken();
            SkipSection skipSection = SkipSection.parseIfNext(parser);
            while (parser.currentToken() != XContentParser.Token.END_ARRAY) {
                ParserUtils.advanceToFieldName(parser);
                executableSections.add(ExecutableSection.parse(parser));
            }
            if (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                throw new IllegalArgumentException("malformed section [" + sectionName + "] expected ["
                        + XContentParser.Token.END_OBJECT + "] but was [" + parser.currentToken() + "]");
            }
            parser.nextToken();
            return new ClientYamlTestSection(sectionLocation, sectionName, skipSection, executableSections);
        } catch (Exception e) {
            throw new ParsingException(parser.getTokenLocation(), "Error parsing test named [" + sectionName + "]", e);
        }
    }

    private final XContentLocation location;
    private final String name;
    private final SkipSection skipSection;
    private final List<ExecutableSection> executableSections;

    ClientYamlTestSection(XContentLocation location, String name, SkipSection skipSection, List<ExecutableSection> executableSections) {
        this.location = location;
        this.name = name;
        this.skipSection = Objects.requireNonNull(skipSection, "skip section cannot be null");
        this.executableSections = Collections.unmodifiableList(executableSections);
    }

    public XContentLocation getLocation() {
        return location;
    }

    public String getName() {
        return name;
    }

    public SkipSection getSkipSection() {
        return skipSection;
    }

    public List<ExecutableSection> getExecutableSections() {
        return executableSections;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ClientYamlTestSection that = (ClientYamlTestSection) o;

        if (name != null ? !name.equals(that.name) : that.name != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return name != null ? name.hashCode() : 0;
    }

    @Override
    public int compareTo(ClientYamlTestSection o) {
        return name.compareTo(o.getName());
    }
}

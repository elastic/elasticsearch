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

package org.elasticsearch.common.xcontent;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

/**
 * Thrown when {@link NamedXContentRegistry#parseNamedObject(Class, String, XContentParser, Object)} is called with an unregistered
 * name. When this bubbles up to the rest layer it is converted into a response with {@code 400 BAD REQUEST} status.
 */
public class UnknownNamedObjectException extends ParsingException {
    private final String categoryClass;
    private final String name;

    public UnknownNamedObjectException(XContentLocation contentLocation, Class<?> categoryClass, String name) {
        super(contentLocation, "Unknown " + categoryClass.getSimpleName() + " [" + name + "]");
        this.categoryClass = requireNonNull(categoryClass, "categoryClass is required").getName();
        this.name = requireNonNull(name, "name is required");
    }

    /**
     * Read from a stream.
     */
    public UnknownNamedObjectException(StreamInput in) throws IOException {
        super(in);
        categoryClass = in.readString();
        name = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(categoryClass);
        out.writeString(name);
    }

    /**
     * Category class that was missing a parser. This is a String instead of a class because the class might not be on the classpath
     * of all nodes or it might be exclusive to a plugin or something.
     */
    public String getCategoryClass() {
        return categoryClass;
    }

    /**
     * Name of the missing parser.
     */
    public String getName() {
        return name;
    }
}

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

package org.elasticsearch.test;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 * Utility methods for assisting with testing {@link Writeable} object.
 */
public class SerializationTestUtils {

    public static <T extends Writeable> T assertRoundTrip(T object, Writeable.Reader<T> reader) throws IOException {
        return assertRoundTrip(object, reader, Version.CURRENT);
    }

    public static <T extends Writeable> T assertRoundTrip(T object, Writeable.Reader<T> reader, Version version) throws IOException {
        final byte[] bytes;
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             OutputStreamStreamOutput out = new OutputStreamStreamOutput(bos)) {
            out.setVersion(version);
            object.writeTo(out);
            bytes = bos.toByteArray();
        }
        try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
             InputStreamStreamInput in = new InputStreamStreamInput(bis)) {
            in.setVersion(version);
            final T readObject = reader.read(in);
            assertThat("Serialized object with version [" + version + "] does not match original object", readObject, equalTo(object));
            return readObject;
        }
    }

}

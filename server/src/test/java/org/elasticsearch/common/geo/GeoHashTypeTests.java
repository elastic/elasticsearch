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

package org.elasticsearch.common.geo;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class GeoHashTypeTests extends ESTestCase {

    public void testValidOrdinals() {
        assertThat(GeoHashType.DEFAULT.ordinal(), equalTo(0));
        assertThat(GeoHashType.GEOHASH.ordinal(), equalTo(0));

        // FIXME: actual ordinal may be different, may need to be updated before merging

        assertThat(GeoHashType.PLUSCODE.ordinal(), equalTo(1));
    }

    public void testWriteTo() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {

            // FIXME: update version after backport

            final Version version = Version.V_7_0_0_alpha1;
            out.setVersion(version);
            GeoHashType.GEOHASH.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(in.readVInt(), equalTo(0));
            }
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {

            // FIXME: update version after backport to the last unsupported

            out.setVersion(Version.V_6_3_0);
            GeoHashType.GEOHASH.writeTo(out);
            // Should not have written anything
            assertThat(out.size(), equalTo(0));
        }

        // ATTENTION: new hashing types should also assert that writeTo() throws
        // an error when values other than GEOHASH are written to older version streams
    }

    public void testReadFrom() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(0);
            try (StreamInput in = out.bytes().streamInput()) {

                // FIXME: update version after backport

                in.setVersion(Version.V_7_0_0_alpha1);
                assertThat(in.available(), equalTo(1));
                assertThat(GeoHashType.readFromStream(in), equalTo(GeoHashType.GEOHASH));
                assertThat(in.available(), equalTo(0));
            }
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            // keep the stream empty to cause an exception if a read is attempted
            try (StreamInput in = out.bytes().streamInput()) {

                // FIXME: update version after backport to the last unsupported

                in.setVersion(Version.V_6_3_0);
                assertThat(in.available(), equalTo(0));
                assertThat(in.available(), equalTo(0));
                assertThat(GeoHashType.readFromStream(in), equalTo(GeoHashType.GEOHASH));
            }
        }
    }

    public void testInvalidReadFrom() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(randomIntBetween(1, Integer.MAX_VALUE));
            try (StreamInput in = out.bytes().streamInput()) {

                // FIXME: update version after backport

                in.setVersion(Version.V_7_0_0_alpha1);
                GeoHashType.readFromStream(in);
                fail("Expected IOException");
            } catch(IOException e) {
                assertThat(e.getMessage(), containsString("Unknown GeoHashType ordinal ["));
            }

        }
    }
}

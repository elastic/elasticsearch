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

package org.elasticsearch.ingest.geoip;

import com.maxmind.geoip2.DatabaseReader;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.ingest.core.IngestDocument;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class GeoIpProcessorTests extends ESTestCase {

    public void testCity() throws Exception {
        InputStream database = getDatabaseFileInputStream("/GeoLite2-City.mmdb.gz");
        GeoIpProcessor processor = new GeoIpProcessor(randomAsciiOfLength(10), "source_field", new DatabaseReader.Builder(database).build(), "target_field", EnumSet.allOf(GeoIpProcessor.Property.class));

        Map<String, Object> document = new HashMap<>();
        document.put("source_field", "8.8.8.8");
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        processor.execute(ingestDocument);

        assertThat(ingestDocument.getSourceAndMetadata().get("source_field"), equalTo("8.8.8.8"));
        @SuppressWarnings("unchecked")
        Map<String, Object> geoData = (Map<String, Object>) ingestDocument.getSourceAndMetadata().get("target_field");
        assertThat(geoData.size(), equalTo(8));
        assertThat(geoData.get("ip"), equalTo("8.8.8.8"));
        assertThat(geoData.get("country_iso_code"), equalTo("US"));
        assertThat(geoData.get("country_name"), equalTo("United States"));
        assertThat(geoData.get("continent_name"), equalTo("North America"));
        assertThat(geoData.get("region_name"), equalTo("California"));
        assertThat(geoData.get("city_name"), equalTo("Mountain View"));
        assertThat(geoData.get("timezone"), equalTo("America/Los_Angeles"));
        Map<String, Object> location = new HashMap<>();
        location.put("lat", 37.386d);
        location.put("lon", -122.0838d);
        assertThat(geoData.get("location"), equalTo(location));
    }

    public void testCountry() throws Exception {
        InputStream database = getDatabaseFileInputStream("/GeoLite2-Country.mmdb.gz");
        GeoIpProcessor processor = new GeoIpProcessor(randomAsciiOfLength(10), "source_field", new DatabaseReader.Builder(database).build(), "target_field", EnumSet.allOf(GeoIpProcessor.Property.class));

        Map<String, Object> document = new HashMap<>();
        document.put("source_field", "82.170.213.79");
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        processor.execute(ingestDocument);

        assertThat(ingestDocument.getSourceAndMetadata().get("source_field"), equalTo("82.170.213.79"));
        @SuppressWarnings("unchecked")
        Map<String, Object> geoData = (Map<String, Object>) ingestDocument.getSourceAndMetadata().get("target_field");
        assertThat(geoData.size(), equalTo(4));
        assertThat(geoData.get("ip"), equalTo("82.170.213.79"));
        assertThat(geoData.get("country_iso_code"), equalTo("NL"));
        assertThat(geoData.get("country_name"), equalTo("Netherlands"));
        assertThat(geoData.get("continent_name"), equalTo("Europe"));
    }

    public void testAddressIsNotInTheDatabase() throws Exception {
        InputStream database = getDatabaseFileInputStream("/GeoLite2-City.mmdb.gz");
        GeoIpProcessor processor = new GeoIpProcessor(randomAsciiOfLength(10), "source_field", new DatabaseReader.Builder(database).build(), "target_field", EnumSet.allOf(GeoIpProcessor.Property.class));

        Map<String, Object> document = new HashMap<>();
        document.put("source_field", "127.0.0.1");
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        processor.execute(ingestDocument);
        @SuppressWarnings("unchecked")
        Map<String, Object> geoData = (Map<String, Object>) ingestDocument.getSourceAndMetadata().get("target_field");
        assertThat(geoData.size(), equalTo(0));
    }

    /** Don't silently do DNS lookups or anything trappy on bogus data */
    public void testInvalid() throws Exception {
        InputStream database = getDatabaseFileInputStream("/GeoLite2-City.mmdb.gz");
        GeoIpProcessor processor = new GeoIpProcessor(randomAsciiOfLength(10), "source_field", new DatabaseReader.Builder(database).build(), "target_field", EnumSet.allOf(GeoIpProcessor.Property.class));

        Map<String, Object> document = new HashMap<>();
        document.put("source_field", "www.google.com");
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        try {
            processor.execute(ingestDocument);
            fail("did not get expected exception");
        } catch (IllegalArgumentException expected) {
            assertNotNull(expected.getMessage());
            assertThat(expected.getMessage(), containsString("not an IP string literal"));
        }
    }

    static InputStream getDatabaseFileInputStream(String path) throws IOException {
        return new GZIPInputStream(GeoIpProcessor.class.getResourceAsStream(path));
    }

}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.license;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TestMatchers;
import org.hamcrest.Matchers;

import java.nio.BufferUnderflowException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.DeprecationHandler.THROW_UNSUPPORTED_OPERATION;
import static org.elasticsearch.test.TestMatchers.throwableWithMessage;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

public class LicenseTests extends ESTestCase {

    public void testFromXContentForGoldLicenseWithVersion2Signature() throws Exception {
        String licenseString = "{\"license\":" +
            "{\"uid\":\"4056779d-b823-4c12-a9cb-efa4a8d8c422\"," +
            "\"type\":\"gold\"," +
            "\"issue_date_in_millis\":1546589020459," +
            "\"expiry_date_in_millis\":1546596340459," +
            "\"max_nodes\":5," +
            "\"issued_to\":\"customer\"," +
            "\"issuer\":\"elasticsearch\"," +
            "\"signature\":\"AAAAAgAAAA34V2kfTJVtvdL2LttwAAABmFJ6NGRnbEM3WVQrZVQwNkdKQmR1VytlMTMyM1J0dTZ1WGwyY2ZCVFhqMGtJU2gzZ3pnNTVpOW" +
            "F5Y1NaUkwyN2VsTEtCYnlZR2c5WWtjQ0phaDlhRjlDUXViUmUwMWhjSkE2TFcwSGdneTJHbUV4N2RHUWJxV20ybjRsZHRzV2xkN0ZmdDlYblJmNVcxMlBWeU81" +
            "V1hLUm1EK0V1dmF3cFdlSGZzTU5SZE1qUmFra3JkS1hCanBWVmVTaFFwV3BVZERzeG9Sci9rYnlJK2toODZXY09tNmFHUVNUL3IyUHExV3VSTlBneWNJcFQ0bX" +
            "l0cmhNNnRwbE1CWE4zWjJ5eGFuWFo0NGhsb3B5WFd1eTdYbFFWQkxFVFFPSlBERlB0eVVJYXVSZ0lsR2JpRS9rN1h4MSsvNUpOcGN6cU1NOHN1cHNtSTFIUGN1" +
            "bWNGNEcxekhrblhNOXZ2VEQvYmRzQUFwbytUZEpRR3l6QU5oS2ZFSFdSbGxxNDZyZ0xvUHIwRjdBL2JqcnJnNGFlK09Cek9pYlJ5Umc9PQAAAQAth77fQLF7CC" +
            "EL7wA6Z0/UuRm/weECcsjW/50kBnPLO8yEs+9/bPa5LSU0bF6byEXOVeO0ebUQfztpjulbXh8TrBDSG+6VdxGtohPo2IYPBaXzGs3LOOor6An/lhptxBWdwYmf" +
            "bcp0m8mnXZh1vN9rmbTsZXnhBIoPTaRDwUBi3vJ3Ms3iLaEm4S8Slrfmtht2jUjgGZ2vAeZ9OHU2YsGtrSpz6f\"}";
        License license = License.fromSource(new BytesArray(licenseString.getBytes(StandardCharsets.UTF_8)),
            XContentType.JSON);
        assertThat(license.type(), equalTo("gold"));
        assertThat(license.uid(), equalTo("4056779d-b823-4c12-a9cb-efa4a8d8c422"));
        assertThat(license.issuer(), equalTo("elasticsearch"));
        assertThat(license.issuedTo(), equalTo("customer"));
        assertThat(license.expiryDate(), equalTo(1546596340459L));
        assertThat(license.issueDate(), equalTo(1546589020459L));
        assertThat(license.maxNodes(), equalTo(5));
        assertThat(license.maxResourceUnits(), equalTo(-1));
        assertThat(license.version(), equalTo(2));
    }

    public void testFromXContentForGoldLicenseWithVersion4Signature() throws Exception {
        String licenseString = "{\"license\":{" +
            "\"uid\":\"4056779d-b823-4c12-a9cb-efa4a8d8c422\"," +
            "\"type\":\"gold\"," +
            "\"issue_date_in_millis\":1546589020459," +
            "\"expiry_date_in_millis\":1546596340459," +
            "\"max_nodes\":5," +
            "\"issued_to\":\"customer\"," +
            "\"issuer\":\"elasticsearch\"," +
            "\"signature\":\"AAAABAAAAA22vXffI41oM4jLCwZ6AAAAIAo5/x6hrsGh1GqqrJmy4qgmEC7gK0U4zQ6q5ZEMhm4jAAABAH3oL4weubwYGjLGNZsz90" +
            "EerX6yOX3Dh6wswG9EfqCiyv6lcjuC7aeKKuOkqhMRTHZ9vHnfMuakHWVlpuGC14WyGqaMwSmgTZ9jVAzt/W3sIotRxM/3rtlCXUc1rOUXNFcii1i3Kkrc" +
            "kTzhENTKjdkOmUN3qZlTEmHkp93eYpx8++iIukHYU9K9Vm2VKgydFfxvYaN/Qr+iPfJSbHJB8+DmS2ywdrmdqW+ScE+1ZNouPNhnP3RKTleNvixXPG9l5B" +
            "qZ2So1IlCrxVDByA1E6JH5AvjbOucpcGiWCm7IzvfpkzphKHMyxhUaIByoHl9UAf4AdPLhowWAQk0eHMRDDlo=\"," +
            "\"start_date_in_millis\":-1}}\n";
        License license = License.fromSource(new BytesArray(licenseString.getBytes(StandardCharsets.UTF_8)),
            XContentType.JSON);
        assertThat(license.type(), equalTo("gold"));
        assertThat(license.uid(), equalTo("4056779d-b823-4c12-a9cb-efa4a8d8c422"));
        assertThat(license.issuer(), equalTo("elasticsearch"));
        assertThat(license.issuedTo(), equalTo("customer"));
        assertThat(license.expiryDate(), equalTo(1546596340459L));
        assertThat(license.issueDate(), equalTo(1546589020459L));
        assertThat(license.maxNodes(), equalTo(5));
        assertThat(license.maxResourceUnits(), equalTo(-1));
        assertThat(license.version(), equalTo(4));
    }

    public void testFromXContentForEnterpriseLicenseWithV5Signature() throws Exception {
        String licenseString = "{\"license\":{" +
            "\"uid\":\"4056779d-b823-4c12-a9cb-efa4a8d8c422\"," +
            "\"type\":\"enterprise\"," +
            "\"issue_date_in_millis\":1546589020459," +
            "\"expiry_date_in_millis\":1546596340459," +
            "\"max_nodes\":null," +
            "\"max_resource_units\":15," +
            "\"issued_to\":\"customer\"," +
            "\"issuer\":\"elasticsearch\"," +
            "\"signature\":\"AAAABQAAAA2MUoEqXb9K9Ie5d6JJAAAAIAo5/x6hrsGh1GqqrJmy4qgmEC7gK0U4zQ6q5ZEMhm4jAAABAAAwVZKGAmDELUlS5PScBkhQsZa" +
            "DaQTtJ4ZP5EnZ/nLpmCt9Dj7d/FRsgMtHmSJLrr2CdrIo4Vx5VuhmbwzZvXMttLz2lrJzG7770PX3TnC9e7F9GdnE9ec0FP2U0ZlLOBOtPuirX0q+j6GfB+DLyE" +
            "5D+Lo1NQ3eLJGvbd3DBYPWJxkb+EBVHczCH2OrIEVWnN/TafmkdZCPX5PcultkNOs3j7d3s7b51EXHKoye8UTcB/RGmzZwMah+E6I/VJkqu7UHL8bB01wJeqo6W" +
            "xI4LC/9+f5kpmHrUu3CHe5pHbmMGDk7O6/cwt1pw/hnJXKIFCi36IGaKcHLgORxQdN0uzE=\"," +
            "\"start_date_in_millis\":-1}}";
        License license = License.fromSource(new BytesArray(licenseString.getBytes(StandardCharsets.UTF_8)),
            XContentType.JSON);
        assertThat(license.type(), equalTo("enterprise"));
        assertThat(license.uid(), equalTo("4056779d-b823-4c12-a9cb-efa4a8d8c422"));
        assertThat(license.issuer(), equalTo("elasticsearch"));
        assertThat(license.issuedTo(), equalTo("customer"));
        assertThat(license.expiryDate(), equalTo(1546596340459L));
        assertThat(license.issueDate(), equalTo(1546589020459L));
        assertThat(license.maxNodes(), equalTo(-1));
        assertThat(license.maxResourceUnits(), equalTo(15));
        assertThat(license.version(), equalTo(5));
    }

    public void testThatEnterpriseLicenseMayNotHaveMaxNodes() throws Exception {
        License.Builder builder = randomLicense(License.LicenseType.ENTERPRISE)
            .maxNodes(randomIntBetween(1, 50))
            .maxResourceUnits(randomIntBetween(10, 500));
        final IllegalStateException ex = expectThrows(IllegalStateException.class, builder::build);
        assertThat(ex, TestMatchers.throwableWithMessage("maxNodes may not be set for enterprise licenses (type=[enterprise])"));
    }

    public void testThatEnterpriseLicenseMustHaveMaxResourceUnits() throws Exception {
        License.Builder builder = randomLicense(License.LicenseType.ENTERPRISE)
            .maxResourceUnits(-1);
        final IllegalStateException ex = expectThrows(IllegalStateException.class, builder::build);
        assertThat(ex, TestMatchers.throwableWithMessage("maxResourceUnits must be set for enterprise licenses (type=[enterprise])"));
    }

    public void testThatRegularLicensesMustHaveMaxNodes() throws Exception {
        License.LicenseType type = randomValueOtherThan(License.LicenseType.ENTERPRISE, () -> randomFrom(License.LicenseType.values()));
        License.Builder builder = randomLicense(type)
            .maxNodes(-1);
        final IllegalStateException ex = expectThrows(IllegalStateException.class, builder::build);
        assertThat(ex, TestMatchers.throwableWithMessage("maxNodes has to be set"));
    }

    public void testThatRegularLicensesMayNotHaveMaxResourceUnits() throws Exception {
        License.LicenseType type = randomValueOtherThan(License.LicenseType.ENTERPRISE, () -> randomFrom(License.LicenseType.values()));
        License.Builder builder = randomLicense(type)
            .maxResourceUnits(randomIntBetween(10, 500))
            .maxNodes(randomIntBetween(1, 50));
        final IllegalStateException ex = expectThrows(IllegalStateException.class, builder::build);
        assertThat(ex, TestMatchers.throwableWithMessage("maxResourceUnits may only be set for enterprise licenses (not permitted " +
            "for type=[" + type.getTypeName() + "])"));
    }

    public void testLicenseToAndFromXContentForEveryLicenseType() throws Exception {
        for (License.LicenseType type : License.LicenseType.values()) {
            final License license1 = randomLicense(type)
                // We need a signature that parses correctly, but it doesn't need to verify
                .signature(
                    "AAAABQAAAA2MUoEqXb9K9Ie5d6JJAAAAIAo5/x6hrsGh1GqqrJmy4qgmEC7gK0U4zQ6q5ZEMhm4jAAABAAAwVZKGAmDELUlS5PScBkhQsZa"
                        + "DaQTtJ4ZP5EnZ/nLpmCt9Dj7d/FRsgMtHmSJLrr2CdrIo4Vx5VuhmbwzZvXMttLz2lrJzG7770PX3TnC9e7F9GdnE9ec0FP2U0ZlL"
                        + "OBOtPuirX0q+j6GfB+DLyE5D+Lo1NQ3eLJGvbd3DBYPWJxkb+EBVHczCH2OrIEVWnN/TafmkdZCPX5PcultkNOs3j7d3s7b51EXHK"
                        + "oye8UTcB/RGmzZwMah+E6I/VJkqu7UHL8bB01wJeqo6WxI4LC/9+f5kpmHrUu3CHe5pHbmMGDk7O6/cwt1pw/hnJXKIFCi36IGaKc"
                        + "HLgORxQdN0uzE="
                )
                .build();
            XContentParser parser = XContentType.JSON.xContent().createParser(NamedXContentRegistry.EMPTY, THROW_UNSUPPORTED_OPERATION,
                Strings.toString(license1));
            License license2 = License.fromXContent(parser);
            assertThat(license2, notNullValue());
            assertThat(license2.type(), equalTo(type.getTypeName()));
            assertThat(license2.uid(), equalTo(license1.uid()));
            assertThat(license2.issuer(), equalTo(license1.issuer()));
            assertThat(license2.issuedTo(), equalTo(license1.issuedTo()));
            assertThat(license2.expiryDate(), equalTo(license1.expiryDate()));
            assertThat(license2.issueDate(), equalTo(license1.issueDate()));
            assertThat(license2.maxNodes(), equalTo(license1.maxNodes()));
            assertThat(license2.maxResourceUnits(), equalTo(license1.maxResourceUnits()));
        }
    }

    public void testSerializationOfLicenseForEveryLicenseType() throws Exception {
        for (License.LicenseType type : License.LicenseType.values()) {
            final String signature = randomBoolean()
                ? null
                : "AAAABQAAAA2MUoEqXb9K9Ie5d6JJAAAAIAo5/x6hrsGh1GqqrJmy4qgmEC7gK0U4zQ6q5ZEM"
                    + "hm4jAAABAAAwVZKGAmDELUlS5PScBkhQsZaDaQTtJ4ZP5EnZ/nLpmCt9Dj7d/FRsgMtH"
                    + "mSJLrr2CdrIo4Vx5VuhmbwzZvXMttLz2lrJzG7770PX3TnC9e7F9GdnE9ec0FP2U0ZlL"
                    + "OBOtPuirX0q+j6GfB+DLyE5D+Lo1NQ3eLJGvbd3DBYPWJxkb+EBVHczCH2OrIEVWnN/T"
                    + "afmkdZCPX5PcultkNOs3j7d3s7b51EXHKoye8UTcB/RGmzZwMah+E6I/VJkqu7UHL8bB"
                    + "01wJeqo6WxI4LC/9+f5kpmHrUu3CHe5pHbmMGDk7O6/cwt1pw/hnJXKIFCi36IGaKcHL"
                    + "gORxQdN0uzE=";
            final int version;
            if (type == License.LicenseType.ENTERPRISE) {
                version = randomIntBetween(License.VERSION_ENTERPRISE, License.VERSION_CURRENT);
            } else {
                version = randomIntBetween(License.VERSION_NO_FEATURE_TYPE, License.VERSION_CURRENT);
            }

            final License license1 = randomLicense(type).signature(signature).version(version).build();

            final BytesStreamOutput out = new BytesStreamOutput();
            out.setVersion(Version.CURRENT);
            license1.writeTo(out);

            final StreamInput in = out.bytes().streamInput();
            in.setVersion(Version.CURRENT);
            final License license2 = License.readLicense(in);
            assertThat(in.read(), Matchers.equalTo(-1));

            assertThat(license2, notNullValue());
            assertThat(license2.type(), equalTo(type.getTypeName()));
            assertThat(license2.version(), equalTo(version));
            assertThat(license2.signature(), equalTo(signature));
            assertThat(license2.uid(), equalTo(license1.uid()));
            assertThat(license2.issuer(), equalTo(license1.issuer()));
            assertThat(license2.issuedTo(), equalTo(license1.issuedTo()));
            assertThat(license2.expiryDate(), equalTo(license1.expiryDate()));
            assertThat(license2.issueDate(), equalTo(license1.issueDate()));
            assertThat(license2.maxNodes(), equalTo(license1.maxNodes()));
            assertThat(license2.maxResourceUnits(), equalTo(license1.maxResourceUnits()));
        }
    }

    public void testNotEnoughBytesFromXContent() throws Exception {

        String licenseString = "{\"license\": " +
            "{\"uid\":\"4056779d-b823-4c12-a9cb-efa4a8d8c422\"," +
            "\"type\":\"gold\"," +
            "\"issue_date_in_millis\":1546589020459," +
            "\"expiry_date_in_millis\":1546596340459," +
            "\"max_nodes\":5," +
            "\"issued_to\":\"customer\"," +
            "\"issuer\":\"elasticsearch\"," +
            "\"signature\":\"AA\"}" +
            "}";
        ElasticsearchException exception =
            expectThrows(ElasticsearchException.class,
                () -> {
                    License.fromSource(new BytesArray(licenseString.getBytes(StandardCharsets.UTF_8)),
                        XContentType.JSON);
                });
        assertThat(exception.getMessage(), containsString("malformed signature for license [4056779d-b823-4c12-a9cb-efa4a8d8c422]"));
        assertThat(exception.getCause(), instanceOf(BufferUnderflowException.class));
    }

    public void testMalformedSignatureFromXContent() throws Exception {

        String licenseString = "{\"license\": " +
            "{\"uid\":\"4056779d-b823-4c12-a9cb-efa4a8d8c422\"," +
            "\"type\":\"gold\"," +
            "\"issue_date_in_millis\":1546589020459," +
            "\"expiry_date_in_millis\":1546596340459," +
            "\"max_nodes\":5," +
            "\"issued_to\":\"customer\"," +
            "\"issuer\":\"elasticsearch\"," +
            "\"signature\":\"" + randomAlphaOfLength(10) + "\"}" +
            "}";
        ElasticsearchException exception =
            expectThrows(ElasticsearchException.class,
                () -> {
                    License.fromSource(new BytesArray(licenseString.getBytes(StandardCharsets.UTF_8)),
                        XContentType.JSON);
                });
        // When parsing a license, we read the signature bytes to verify the _version_.
        // Random alphabetic sig bytes will generate a bad version
        assertThat(exception, throwableWithMessage(containsString("Unknown license version found")));
    }

    public void testUnableToBase64DecodeFromXContent() throws Exception {

        String licenseString = "{\"license\":" +
            "{\"uid\":\"4056779d-b823-4c12-a9cb-efa4a8d8c422\"," +
            "\"type\":\"gold\"," +
            "\"issue_date_in_millis\":1546589020459," +
            "\"expiry_date_in_millis\":1546596340459," +
            "\"max_nodes\":5," +
            "\"issued_to\":\"customer\"," +
            "\"issuer\":\"elasticsearch\"," +
            "\"signature\":\"AAAAAgAAAA34V2kfTJVtvdL2LttwAAABmFJ6NGRnbEM3WVQrZVQwNkdKQmR1VytlMTMyM1J0dTZ1WGwyY2ZCVFhqMGtJU2gzZ3pnNTVpOW" +
            "F5Y1NaUkwyN2VsTEtCYnlZR2c5WWtjQ0phaDlhRjlDUXViUmUwMWhjSkE2TFcwSGdneTJHbUV4N2RHUWJxV20ybjRsZHRzV2xkN0ZmdDlYblJmNVcxMlBWeU81" +
            "V1hLUm1EK0V1dmF3cFdlSGZzTU5SZE1qUmFra3JkS1hCanBWVmVTaFFwV3BVZERzeG9Sci9rYnlJK2toODZXY09tNmFHUVNUL3IyUHExV3VSTlBneWNJcFQ0bX" +
            "l0cmhNNnRwbE1CWE4zWjJ5eGFuWFo0NGhsb3B5WFd1eTdYbFFWQkxFVFFPSlBERlB0eVVJYXVSZ0lsR2JpRS9rN1h4MSsvNUpOcGN6cU1NOHN1cHNtSTFIUGN1" +
            "bWNGNEcxekhrblhNOXZ2VEQvYmRzQUFwbytUZEpRR3l6QU5oS2ZFSFdSbGxxNDZyZ0xvUHIwRjdBL2JqcnJnNGFlK09Cek9pYlJ5Umc9PQAAAQAth77fQLF7CC" +
            "EL7wA6Z0/UuRm/weECcsjW/50kBnPLO8yEs+9/bPa5LSU0bF6byEXOVeO0ebUQfztpjulbXh8TrBDSG+6VdxGtohPo2IYPBaXzGs3LOOor6An/lhptxBWdwYmf" +
            "+xHAQ8tyvRqP5G+PRU7tiluEwR/eyHGZV2exdJNzmoGzdPSWwueBM5HK2GexORICH+UFI4cuGz444/hL2MMM1RdpVWQkT0SJ6D9x/VuSmHuYPdtX59Pp41LXvl" +
            "bcp0m8mnXZh1vN9rmbTsZXnhBIoPTaRDwUBi3vJ3Ms3iLaEm4S8Slrfmtht2jUjgGZ2vAeZ9OHU2YsGtrSpz6fd\"}";
        ElasticsearchException exception =
            expectThrows(ElasticsearchException.class,
                () -> {
                    License.fromSource(new BytesArray(licenseString.getBytes(StandardCharsets.UTF_8)),
                        XContentType.JSON);
                });
        assertThat(exception.getMessage(), containsString("malformed signature for license [4056779d-b823-4c12-a9cb-efa4a8d8c422]"));
        assertThat(exception.getCause(), instanceOf(IllegalArgumentException.class));
    }

    private License.Builder randomLicense(License.LicenseType type) {
        return License.builder()
            .uid(UUIDs.randomBase64UUID(random()))
            .type(type)
            .issueDate(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(randomIntBetween(1, 10)))
            .expiryDate(System.currentTimeMillis() + TimeUnit.DAYS.toMillis(randomIntBetween(1, 1000)))
            .maxNodes(type == License.LicenseType.ENTERPRISE ? -1 : randomIntBetween(1, 100))
            .maxResourceUnits(type == License.LicenseType.ENTERPRISE ? randomIntBetween(1, 100) : -1)
            .issuedTo(randomAlphaOfLengthBetween(5, 50))
            .issuer(randomAlphaOfLengthBetween(5, 50));
    }

}
